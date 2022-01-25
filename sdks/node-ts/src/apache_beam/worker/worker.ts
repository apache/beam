// From sdks/node-ts
//     npx tsc && npm run worker
// From sdks/python
//     python trivial_pipeline.py --environment_type=EXTERNAL --environment_config='localhost:5555' --runner=PortableRunner --job_endpoint=embed

import * as grpc from "@grpc/grpc-js";

import { PTransform, PCollection } from "../proto/beam_runner_api";

import { InstructionRequest, InstructionResponse } from "../proto/beam_fn_api";
import {
  ProcessBundleDescriptor,
  ProcessBundleResponse,
} from "../proto/beam_fn_api";
import {
  BeamFnControlClient,
  IBeamFnControlClient,
} from "../proto/beam_fn_api.grpc-client";

import {
  StartWorkerRequest,
  StartWorkerResponse,
  StopWorkerRequest,
  StopWorkerResponse,
} from "../proto/beam_fn_api";
import {
  beamFnExternalWorkerPoolDefinition,
  IBeamFnExternalWorkerPool,
} from "../proto/beam_fn_api.grpc-server";

import { MultiplexingDataChannel, IDataChannel } from "./data";
import {
  MultiplexingStateChannel,
  CachingStateProvider,
  GrpcStateProvider,
  StateProvider,
} from "./state";
import {
  IOperator,
  Receiver,
  createOperator,
  OperatorContext,
} from "./operators";

export class Worker {
  controlClient: BeamFnControlClient;
  controlChannel: grpc.ClientDuplexStream<
    InstructionResponse,
    InstructionRequest
  >;

  processBundleDescriptors: Map<string, ProcessBundleDescriptor> = new Map();
  bundleProcessors: Map<string, BundleProcessor[]> = new Map();
  dataChannels: Map<string, MultiplexingDataChannel> = new Map();
  stateChannels: Map<string, MultiplexingStateChannel> = new Map();

  constructor(private id: string, private endpoints: StartWorkerRequest) {
    if (endpoints?.controlEndpoint?.url == undefined) {
      throw "Missing control endpoint.";
    }
    const metadata = new grpc.Metadata();
    metadata.add("worker_id", this.id);
    this.controlClient = new BeamFnControlClient(
      endpoints.controlEndpoint.url,
      grpc.ChannelCredentials.createInsecure(),
      {},
      {}
    );
    this.controlChannel = this.controlClient.control(metadata);
    this.controlChannel.on("data", async (request) => {
      console.log(request);
      if (request.request.oneofKind == "processBundle") {
        await this.process(request);
      } else {
        console.log("Unknown instruction type: ", request);
      }
    });
    this.controlChannel.on("end", () => {
      console.log("Control channel closed.");
      for (const dataChannel of this.dataChannels.values()) {
        dataChannel.close();
      }
      for (const stateChannel of this.stateChannels.values()) {
        stateChannel.close();
      }
    });
  }

  respond(response: InstructionResponse) {
    this.controlChannel.write(response);
  }

  async process(request) {
    const descriptorId =
      request.request.processBundle.processBundleDescriptorId;
    if (!this.processBundleDescriptors.has(descriptorId)) {
      const call = this.controlClient.getProcessBundleDescriptor(
        {
          processBundleDescriptorId: descriptorId,
        },
        (err, value: ProcessBundleDescriptor) => {
          if (err) {
            this.respond({
              instructionId: request.instructionId,
              error: "" + err,
              response: {
                oneofKind: "processBundle",
                processBundle: {
                  residualRoots: [],
                  monitoringInfos: [],
                  requiresFinalization: false,
                  monitoringData: {},
                },
              },
            });
          } else {
            this.processBundleDescriptors.set(descriptorId, value);
            this.process(request);
          }
        }
      );
      return;
    }

    const processor = this.aquireBundleProcessor(descriptorId);
    try {
      await processor.process(request.instructionId);
      await this.respond({
        instructionId: request.instructionId,
        error: "",
        response: {
          oneofKind: "processBundle",
          processBundle: {
            residualRoots: [],
            monitoringInfos: [],
            requiresFinalization: false,
            monitoringData: {},
          },
        },
      });
    } catch (error) {
      console.error("PROCESS ERROR", error);
      await this.respond({
        instructionId: request.instructionId,
        error: "" + error,
        response: undefined!,
      });
    }
    this.returnBundleProcessor(processor);
  }

  aquireBundleProcessor(descriptorId: string) {
    if (!this.bundleProcessors.has(descriptorId)) {
      this.bundleProcessors.set(descriptorId, []);
    }
    const processor = this.bundleProcessors.get(descriptorId)?.pop();
    if (processor != undefined) {
      return processor;
    } else {
      return new BundleProcessor(
        this.processBundleDescriptors.get(descriptorId)!,
        this.getDataChannel.bind(this),
        this.getStateChannel.bind(this)
      );
    }
  }

  returnBundleProcessor(processor: BundleProcessor) {
    this.bundleProcessors.get(processor.descriptor.id)?.push(processor);
  }

  getDataChannel(endpoint: string): MultiplexingDataChannel {
    // TODO: Is there a javascript equivalent of Python's defaultdict?
    if (!this.dataChannels.has(endpoint)) {
      this.dataChannels.set(
        endpoint,
        new MultiplexingDataChannel(endpoint, this.id)
      );
    }
    return this.dataChannels.get(endpoint)!;
  }

  getStateChannel(endpoint: string): MultiplexingStateChannel {
    if (!this.stateChannels.has(endpoint)) {
      this.stateChannels.set(
        endpoint,
        new MultiplexingStateChannel(endpoint, this.id)
      );
    }
    return this.stateChannels.get(endpoint)!;
  }

  stop() {
    this.controlChannel.end();
  }
}

export class BundleProcessor {
  descriptor: ProcessBundleDescriptor;

  topologicallyOrderedOperators: IOperator[] = [];
  operators: Map<string, IOperator> = new Map();
  receivers: Map<string, Receiver> = new Map();

  getDataChannel: (string) => MultiplexingDataChannel;
  getStateChannel: ((string) => MultiplexingStateChannel) | StateProvider;
  currentBundleId?: string;
  stateProvider?: StateProvider;

  constructor(
    descriptor: ProcessBundleDescriptor,
    getDataChannel: (string) => MultiplexingDataChannel,
    getStateChannel: ((string) => MultiplexingStateChannel) | StateProvider,
    root_urns = ["beam:runner:source:v1"]
  ) {
    this.descriptor = descriptor;
    this.getDataChannel = getDataChannel;
    this.getStateChannel = getStateChannel;

    // TODO: Consider defering this possibly expensive deserialization lazily to the worker thread.
    const this_ = this;
    const creationOrderedOperators: IOperator[] = [];

    const consumers = new Map<string, string[]>();
    Object.entries(descriptor.transforms).forEach(
      ([transformId, transform]) => {
        if (isPrimitive(transform)) {
          Object.values(transform.inputs).forEach((pcollectionId: string) => {
            // TODO: is there a javascript defaultdict?
            if (!consumers.has(pcollectionId)) {
              consumers.set(pcollectionId, []);
            }
            consumers.get(pcollectionId)!.push(transformId);
          });
        }
      }
    );

    function getReceiver(pcollectionId: string): Receiver {
      if (!this_.receivers.has(pcollectionId)) {
        this_.receivers.set(
          pcollectionId,
          new Receiver((consumers.get(pcollectionId) || []).map(getOperator))
        );
      }
      return this_.receivers.get(pcollectionId)!;
    }

    function getOperator(transformId: string): IOperator {
      if (!this_.operators.has(transformId)) {
        this_.operators.set(
          transformId,
          createOperator(
            transformId,
            new OperatorContext(
              descriptor,
              getReceiver,
              this_.getDataChannel,
              this_.getStateProvider.bind(this_),
              this_.getBundleId.bind(this_)
            )
          )
        );
        creationOrderedOperators.push(this_.operators.get(transformId)!);
      }
      return this_.operators.get(transformId)!;
    }

    Object.entries(descriptor.transforms).forEach(
      ([transformId, transform]) => {
        if (root_urns.includes(transform?.spec?.urn!)) {
          getOperator(transformId);
        }
      }
    );
    this.topologicallyOrderedOperators = creationOrderedOperators.reverse();
  }

  getStateProvider() {
    if (this.stateProvider == undefined) {
      if (typeof this.getStateChannel == "function") {
        this.stateProvider = new CachingStateProvider(
          new GrpcStateProvider(
            this.getStateChannel(
              this.descriptor.stateApiServiceDescriptor!.url
            ),
            this.getBundleId()
          )
        );
      } else {
        this.stateProvider = this.getStateChannel;
      }
    }
    return this.stateProvider;
  }

  getBundleId() {
    if (this.currentBundleId == undefined) {
      throw new Error("Not currently processing a bundle.");
    }
    return this.currentBundleId!;
  }

  // Put this on a worker thread...
  async process(instructionId: string, delay_ms = 600) {
    console.log("Processing ", this.descriptor.id, "for", instructionId);
    this.currentBundleId = instructionId;
    // We must await these in reverse topological order.
    for (const o of this.topologicallyOrderedOperators.slice().reverse()) {
      await o.startBundle();
    }
    // Now finish bundles all the bundles.
    // Note that process is not directly called on any operator here.
    // Instead, process is triggered by elements coming over the
    // data stream and/or operator start/finishBundle methods.
    for (const o of this.topologicallyOrderedOperators) {
      await o.finishBundle();
    }
    this.currentBundleId = undefined;
    this.stateProvider = undefined;
  }
}

export const primitiveSinks = ["beam:runner:sink:v1"];

function isPrimitive(transform: PTransform): boolean {
  const inputs = Object.values(transform.inputs);
  if (primitiveSinks.includes(transform?.spec?.urn!)) {
    return true;
  } else {
    return (
      transform.subtransforms.length == 0 &&
      Object.values(transform.outputs).some((pcoll) => !inputs.includes(pcoll))
    );
  }
}
