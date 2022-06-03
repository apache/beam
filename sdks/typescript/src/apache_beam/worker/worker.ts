/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

export interface WorkerEndpoints {
  controlUrl: string;
}

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

  constructor(
    private id: string,
    private endpoints: WorkerEndpoints,
    options: Object = {}
  ) {
    const metadata = new grpc.Metadata();
    metadata.add("worker_id", this.id);
    this.controlClient = new BeamFnControlClient(
      endpoints.controlUrl,
      grpc.ChannelCredentials.createInsecure(),
      {},
      {}
    );
    this.controlChannel = this.controlClient.control(metadata);
    this.controlChannel.on("data", this.handleRequest.bind(this));
    this.controlChannel.on("end", () => {
      console.log("Control channel closed.");
      for (const dataChannel of this.dataChannels.values()) {
        try {
          // Best effort.
          dataChannel.close();
        } finally {
        }
      }
      for (const stateChannel of this.stateChannels.values()) {
        try {
          // Best effort.
          stateChannel.close();
        } finally {
        }
      }
    });
  }

  async wait() {
    // TODO: Await closing of control log.
    await new Promise((r) => setTimeout(r, 1e9));
  }

  async handleRequest(request) {
    console.log(request);
    if (request.request.oneofKind === "processBundle") {
      await this.process(request);
    } else {
      console.log("Unknown instruction type: ", request);
      this.controlChannel.write({
        instructionId: request.instructionId,
        error: "Unknown instruction type: " + request.request.oneofKind,
        response: {
          oneofKind: undefined,
        },
      });
    }
  }

  respond(response: InstructionResponse) {
    this.controlChannel.write(response);
  }

  async process(request) {
    const descriptorId =
      request.request.processBundle.processBundleDescriptorId;
    try {
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
      this.returnBundleProcessor(processor);
    } catch (error) {
      console.error("PROCESS ERROR", error);
      await this.respond({
        instructionId: request.instructionId,
        error: "" + error,
        response: { oneofKind: undefined },
      });
    }
  }

  aquireBundleProcessor(descriptorId: string) {
    if (!this.bundleProcessors.has(descriptorId)) {
      this.bundleProcessors.set(descriptorId, []);
    }
    const processor = this.bundleProcessors.get(descriptorId)?.pop();
    if (processor) {
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
    if (!this.bundleProcessors.has(processor.descriptor.id)) {
      this.bundleProcessors.set(processor.descriptor.id, []);
    }
    this.bundleProcessors.get(processor.descriptor.id)!.push(processor);
  }

  getDataChannel(endpoint: string): MultiplexingDataChannel {
    // TODO: (Typescript) Defining something like Python's defaultdict could
    // simplify this code.
    // This would be handy in several other places as well.
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

    // TODO: (Perf) Consider defering this possibly expensive deserialization lazily to the worker thread.
    const this_ = this;
    const creationOrderedOperators: IOperator[] = [];

    const consumers = new Map<string, string[]>();
    Object.entries(descriptor.transforms).forEach(
      ([transformId, transform]) => {
        if (isPrimitive(transform)) {
          Object.values(transform.inputs).forEach((pcollectionId: string) => {
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
    if (!this.stateProvider) {
      if (typeof this.getStateChannel === "function") {
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
    if (this.currentBundleId === null || this.currentBundleId === undefined) {
      throw new Error("Not currently processing a bundle.");
    }
    return this.currentBundleId!;
  }

  // Put this on a worker thread...
  async process(instructionId: string) {
    console.debug("Processing ", this.descriptor.id, "for", instructionId);
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
      transform.subtransforms.length === 0 &&
      Object.values(transform.outputs).some((pcoll) => !inputs.includes(pcoll))
    );
  }
}
