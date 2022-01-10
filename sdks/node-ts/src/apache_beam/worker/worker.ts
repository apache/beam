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
        this.getDataChannel.bind(this)
      );
    }
  }

  returnBundleProcessor(processor: BundleProcessor) {
    this.bundleProcessors.get(processor.descriptor.id)?.push(processor);
  }

  getDataChannel(endpoint: string): MultiplexingDataChannel {
    if (!this.dataChannels.has(endpoint)) {
      this.dataChannels.set(
        endpoint,
        new MultiplexingDataChannel(endpoint, this.id)
      );
    }
    return this.dataChannels.get(endpoint)!;
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
  currentBundleId?: string;

  constructor(
    descriptor: ProcessBundleDescriptor,
    getDataChannel: (string) => MultiplexingDataChannel,
    root_urns = ["beam:runner:source:v1"]
  ) {
    this.descriptor = descriptor;
    this.getDataChannel = getDataChannel;

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

  getBundleId() {
    return this.currentBundleId!;
  }

  // Put this on a worker thread...
  async process(instructionId: string, delay_ms = 600) {
    console.log("Processing ", this.descriptor.id, "for", instructionId);
    this.currentBundleId = instructionId;
    this.topologicallyOrderedOperators
      .slice()
      .reverse()
      .forEach((o) => o.startBundle());

    if (delay_ms > 0) {
      console.log("Waiting...");
      await new Promise((resolve) => {
        setTimeout(resolve, delay_ms);
      });
      console.log("Done waiting.");
    }

    this.topologicallyOrderedOperators.forEach((o) => o.finishBundle());
    this.currentBundleId = undefined;
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
