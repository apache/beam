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
  ProcessBundleSplitRequest,
  ProcessBundleSplitResponse,
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
import { loggingLocalStorage, LoggingStageInfo } from "./logging";
import { MetricsContainer, MetricsShortIdCache } from "./metrics";
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
  DataSourceOperator,
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
  activeBundleProcessors: Map<string, BundleProcessor> = new Map();
  dataChannels: Map<string, MultiplexingDataChannel> = new Map();
  stateChannels: Map<string, MultiplexingStateChannel> = new Map();
  metricsShortIdCache = new MetricsShortIdCache();

  constructor(
    private id: string,
    private endpoints: WorkerEndpoints,
    options: Object = {},
  ) {
    const metadata = new grpc.Metadata();
    metadata.add("worker_id", this.id);
    this.controlClient = new BeamFnControlClient(
      endpoints.controlUrl,
      grpc.ChannelCredentials.createInsecure(),
      {},
      {},
    );
    this.controlChannel = this.controlClient.control(metadata);
    this.controlChannel.on("data", this.handleRequest.bind(this));
    this.controlChannel.on("end", () => {
      console.warn("Control channel closed.");
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
    try {
      console.log(request);
      if (request.request.oneofKind === "processBundle") {
        return this.process(request);
      } else if (request.request.oneofKind === "processBundleProgress") {
        return this.controlChannel.write(await this.progress(request));
      } else if (request.request.oneofKind === "processBundleSplit") {
        return this.controlChannel.write(await this.split(request));
      } else {
        console.error("Unknown instruction type: ", request);
        return this.controlChannel.write({
          instructionId: request.instructionId,
          error: "Unknown instruction type: " + request.request.oneofKind,
          response: {
            oneofKind: undefined,
          },
        });
      }
    } catch (e) {
      return this.controlChannel.write({
        instructionId: request.instructionId,
        error: "Error handling instruction: " + e + "\n" + e.stack,
        response: {
          oneofKind: undefined,
        },
      });
    }
  }

  async respond(response: InstructionResponse) {
    return this.controlChannel.write(response);
  }

  async progress(request): Promise<InstructionResponse> {
    const processor = this.activeBundleProcessors.get(
      request.request.processBundleProgress.instructionId,
    );
    if (processor) {
      const monitoringData = processor.monitoringData(this.metricsShortIdCache);
      return {
        instructionId: request.instructionId,
        error: "",
        response: {
          oneofKind: "processBundleProgress",
          processBundleProgress: {
            monitoringInfos: Array.from(monitoringData.entries()).map(
              ([id, payload]) =>
                this.metricsShortIdCache.asMonitoringInfo(id, payload),
            ),
            monitoringData: Object.fromEntries(monitoringData.entries()),
          },
        },
      };
    } else {
      return {
        instructionId: request.instructionId,
        error: "Unknown bundle: " + request.processBundleProgress.instructionId,
        response: {
          oneofKind: undefined,
        },
      };
    }
  }

  async split(request): Promise<InstructionResponse> {
    const processor = this.activeBundleProcessors.get(
      request.request.processBundleSplit.instructionId,
    );
    console.log(request.request.processBundleSplit, processor === undefined);
    if (processor) {
      return {
        instructionId: request.instructionId,
        error: "",
        response: {
          oneofKind: "processBundleSplit",
          processBundleSplit: processor.split(
            request.request.processBundleSplit,
          ),
        },
      };
    } else {
      return {
        instructionId: request.instructionId,
        error:
          "Unknown process bundle: " +
          request.request.processBundleSplit.instructionId,
        response: {
          oneofKind: undefined,
        },
      };
    }
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
              this.processBundleDescriptors.set(
                descriptorId,
                maybeStripDataflowWindowedWrappings(value),
              );
              this.process(request);
            }
          },
        );
        return;
      }

      const processor = this.aquireBundleProcessor(descriptorId);
      this.activeBundleProcessors.set(request.instructionId, processor);
      await processor.process(request.instructionId);
      const monitoringData = processor.monitoringData(this.metricsShortIdCache);
      await this.respond({
        instructionId: request.instructionId,
        error: "",
        response: {
          oneofKind: "processBundle",
          processBundle: {
            residualRoots: [],
            requiresFinalization: false,
            monitoringInfos: Array.from(monitoringData.entries()).map(
              ([id, payload]) =>
                this.metricsShortIdCache.asMonitoringInfo(id, payload),
            ),
            monitoringData: Object.fromEntries(monitoringData.entries()),
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
    } finally {
      this.activeBundleProcessors.delete(request.instructionId);
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
        this.getStateChannel.bind(this),
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
        new MultiplexingDataChannel(endpoint, this.id),
      );
    }
    return this.dataChannels.get(endpoint)!;
  }

  getStateChannel(endpoint: string): MultiplexingStateChannel {
    if (!this.stateChannels.has(endpoint)) {
      this.stateChannels.set(
        endpoint,
        new MultiplexingStateChannel(endpoint, this.id),
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
  loggingStageInfo: LoggingStageInfo = {};
  metricsContainer: MetricsContainer;

  constructor(
    descriptor: ProcessBundleDescriptor,
    getDataChannel: (string) => MultiplexingDataChannel,
    getStateChannel: ((string) => MultiplexingStateChannel) | StateProvider,
    root_urns = ["beam:runner:source:v1"],
  ) {
    this.descriptor = descriptor;
    this.getDataChannel = getDataChannel;
    this.getStateChannel = getStateChannel;
    this.metricsContainer = new MetricsContainer();

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
      },
    );

    function getReceiver(pcollectionId: string): Receiver {
      if (!this_.receivers.has(pcollectionId)) {
        this_.receivers.set(
          pcollectionId,
          new Receiver(
            (consumers.get(pcollectionId) || []).map(getOperator),
            this_.loggingStageInfo,
            this_.metricsContainer.elementCountMetric(pcollectionId),
          ),
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
              this_.getBundleId.bind(this_),
              this_.loggingStageInfo,
              this_.metricsContainer,
            ),
          ),
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
      },
    );
    this.topologicallyOrderedOperators = creationOrderedOperators.reverse();
  }

  getStateProvider() {
    if (!this.stateProvider) {
      if (typeof this.getStateChannel === "function") {
        this.stateProvider = new CachingStateProvider(
          new GrpcStateProvider(
            this.getStateChannel(
              this.descriptor.stateApiServiceDescriptor!.url,
            ),
            this.getBundleId(),
          ),
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
    this.metricsContainer.reset();
    this.currentBundleId = instructionId;
    this.loggingStageInfo.instructionId = instructionId;
    loggingLocalStorage.enterWith(this.loggingStageInfo);
    // We must await these in reverse topological order.
    for (const o of this.topologicallyOrderedOperators.slice().reverse()) {
      this.loggingStageInfo.transformId = o.transformId;
      await o.startBundle();
    }
    this.loggingStageInfo.transformId = undefined;
    // Now finish bundles all the bundles.
    // Note that process is not directly called on any operator here.
    // Instead, process is triggered by elements coming over the
    // data stream and/or operator start/finishBundle methods.
    for (const o of this.topologicallyOrderedOperators) {
      this.loggingStageInfo.transformId = o.transformId;
      await o.finishBundle();
    }
    this.loggingStageInfo.transformId = undefined;
    this.loggingStageInfo.instructionId = undefined;
    this.currentBundleId = undefined;
    this.stateProvider = undefined;
  }

  split(splitRequest: ProcessBundleSplitRequest): ProcessBundleSplitResponse {
    const root = this.topologicallyOrderedOperators[0] as DataSourceOperator;
    for (const [target, desiredSplit] of Object.entries(
      splitRequest.desiredSplits,
    )) {
      if (target == root.transformId) {
        const channelSplit = root.split(desiredSplit);
        if (channelSplit) {
          return {
            primaryRoots: [],
            residualRoots: [],
            channelSplits: [channelSplit],
          };
        }
      }
    }
    return ProcessBundleSplitResponse.create({});
  }

  monitoringData(shortIdCache: MetricsShortIdCache): Map<string, Uint8Array> {
    return this.metricsContainer.monitoringData(shortIdCache);
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

function maybeStripDataflowWindowedWrappings(
  descriptor: ProcessBundleDescriptor,
): ProcessBundleDescriptor {
  for (const pcoll of Object.values(descriptor.pcollections)) {
    const coder = descriptor.coders[pcoll.coderId];
    if (
      coder.spec?.urn == "beam:coder:windowed_value:v1" ||
      coder.spec?.urn == "beam:coder:param_windowed_value:v1"
    ) {
      // Dataflow sets PCollection coder_id to a coder of WindowedValues rather
      // than the coder of the element type alone.
      // Until Dataflow is fixed, we must assume that the element type is not
      // actually a windowed value, but that this wrapping was extraneously
      // added.
      pcoll.coderId = coder.componentCoderIds[0];
    }
  }
  return descriptor;
}
