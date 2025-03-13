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

import * as protobufjs from "protobufjs";

import { PTransform, PCollection } from "../proto/beam_runner_api";
import * as runnerApi from "../proto/beam_runner_api";
import * as fnApi from "../proto/beam_fn_api";
import { ProcessBundleDescriptor, RemoteGrpcPort } from "../proto/beam_fn_api";
import { MultiplexingDataChannel, IDataChannel } from "./data";
import { MetricsContainer } from "./metrics";
import { loggingLocalStorage, LoggingStageInfo } from "./logging";
import { StateProvider } from "./state";

import * as urns from "../internal/urns";
import { PipelineContext } from "../internal/pipeline";
import { deserializeFn } from "../internal/serialize";
import { Coder, Context as CoderContext } from "../coders/coders";
import { PaneInfo, Window, Instant, WindowedValue } from "../values";
import { PaneInfoCoder } from "../coders/standard_coders";
import { parDo, DoFn, SplitOptions } from "../transforms/pardo";
import { CombineFn } from "../transforms/group_and_combine";
import { WindowFn } from "../transforms/window";

import {
  ParamProviderImpl,
  SideInputInfo,
  createSideInputInfo,
} from "./pardo_context";

// Trying to get some of https://github.com/microsoft/TypeScript/issues/8240
export const NonPromise = null;

export type ProcessResult = null | Promise<void>;

export class ProcessResultBuilder {
  promises: Promise<void>[] = [];
  add(result: ProcessResult) {
    if (result !== NonPromise) {
      this.promises.push(result as Promise<void>);
    }
  }
  build(): ProcessResult {
    if (this.promises.length === 0) {
      return NonPromise;
    } else if (this.promises.length === 1) {
      return this.promises[0];
    } else {
      return Promise.all(this.promises).then(() => void null);
    }
  }
}

export interface IOperator {
  transformId: string;
  startBundle: () => Promise<void>;
  // As this is called at every operator at every element, and the vast majority
  // of the time Promises are not needed, we wish to avoid the overhead of
  // creating promises and await as much as possible.
  process: (wv: WindowedValue<unknown>) => ProcessResult;
  finishBundle: () => Promise<void>;
}

export class Receiver {
  constructor(
    private operators: IOperator[],
    private loggingStageInfo: LoggingStageInfo,
    private elementCounter: { update: (number) => void },
  ) {}

  receive(wvalue: WindowedValue<unknown>): ProcessResult {
    this.elementCounter.update(1);
    try {
      if (this.operators.length === 1) {
        const operator = this.operators[0];
        this.loggingStageInfo.transformId = operator.transformId;
        return operator.process(wvalue);
      } else {
        const result = new ProcessResultBuilder();
        for (const operator of this.operators) {
          this.loggingStageInfo.transformId = operator.transformId;
          result.add(operator.process(wvalue));
        }
        return result.build();
      }
    } finally {
      this.loggingStageInfo.transformId = undefined;
    }
  }
}

export class OperatorContext {
  pipelineContext: PipelineContext;
  constructor(
    public descriptor: ProcessBundleDescriptor,
    public getReceiver: (string) => Receiver,
    public getDataChannel: (string) => MultiplexingDataChannel,
    public getStateProvider: () => StateProvider,
    public getBundleId: () => string,
    public loggingStageInfo: LoggingStageInfo,
    public metricsContainer: MetricsContainer,
  ) {
    this.pipelineContext = new PipelineContext(descriptor, "");
  }
}

export function createOperator(
  transformId: string,
  context: OperatorContext,
): IOperator {
  const transform = context.descriptor.transforms[transformId];
  // Ensure receivers are eagerly created.
  Object.values(transform.outputs).map(context.getReceiver);
  let operatorConstructor = operatorsByUrn.get(transform.spec!.urn!);
  if (operatorConstructor === null || operatorConstructor === undefined) {
    throw new Error("Unknown transform type:" + transform.spec!.urn);
  }
  return operatorConstructor(transformId, transform, context);
}

type OperatorConstructor = (
  transformId: string,
  transformProto: PTransform,
  context: OperatorContext,
) => IOperator;
interface OperatorClass {
  new (
    transformId: string,
    transformProto: PTransform,
    context: OperatorContext,
  ): IOperator;
}

const operatorsByUrn: Map<string, OperatorConstructor> = new Map();

export function registerOperator(urn: string, cls: OperatorClass) {
  registerOperatorConstructor(urn, (transformId, transformProto, context) => {
    return new cls(transformId, transformProto, context);
  });
}

export function registerOperatorConstructor(
  urn: string,
  constructor: OperatorConstructor,
) {
  operatorsByUrn.set(urn, constructor);
}

////////// Actual operator implementation. //////////

// NOTE: It may have been more idiomatic to use objects in closures satisfying
// the IOperator interface here, but classes are used to make a clearer pattern
// potential SDK authors that are less familiar with javascript.

export class DataSourceOperator implements IOperator {
  transformId: string;
  getBundleId: () => string;
  multiplexingDataChannel: MultiplexingDataChannel;
  receiver: Receiver;
  coder: Coder<WindowedValue<unknown>>;
  endOfData: Promise<void>;
  loggingStageInfo: LoggingStageInfo;
  started: boolean;
  lastProcessedElement: number;
  lastToProcessElement: number;

  constructor(
    transformId: string,
    transform: PTransform,
    context: OperatorContext,
  ) {
    const readPort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
    this.multiplexingDataChannel = context.getDataChannel(
      readPort.apiServiceDescriptor!.url,
    );
    this.transformId = transformId;
    this.getBundleId = context.getBundleId;
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs)),
    );
    this.coder = context.pipelineContext.getCoder(readPort.coderId);
    this.loggingStageInfo = context.loggingStageInfo;
    this.started = false;
  }

  async startBundle() {
    const this_ = this;
    var endOfDataResolve, endOfDataReject;
    this.endOfData = new Promise(async (resolve, reject) => {
      endOfDataResolve = resolve;
      endOfDataReject = reject;
    });

    this.lastProcessedElement = -1;
    this.lastToProcessElement = Infinity;
    this.started = true;

    await this_.multiplexingDataChannel.registerConsumer(
      this_.getBundleId(),
      this_.transformId,
      {
        sendData: async function (data: Uint8Array) {
          this_.loggingStageInfo.transformId = this_.transformId;
          loggingLocalStorage.enterWith(this_.loggingStageInfo);
          const reader = new protobufjs.Reader(data);
          var lastYield = Date.now();
          while (reader.pos < reader.len) {
            if (this_.lastProcessedElement >= this_.lastToProcessElement) {
              break;
            }
            this_.lastProcessedElement += 1;
            const maybePromise = this_.receiver.receive(
              this_.coder.decode(reader, CoderContext.needsDelimiters),
            );
            if (maybePromise !== NonPromise) {
              await maybePromise;
            }
            // Periodically yield control explicitly to allow other tasks
            // (including splits requests) to get scheduled.
            // Note that waiting on a resolved promise is not sufficient, so
            // we do this in addition to the above loop.
            if (Date.now() - lastYield > 100 /* milliseconds */) {
              await new Promise((r) => setTimeout(r, 0));
              lastYield = new Date().getTime();
            }
          }
        },
        sendTimers: async function (timerFamilyId: string, timers: Uint8Array) {
          throw Error("Not expecting timers.");
        },
        close: function () {
          endOfDataResolve();
        },
        onError: function (error: Error) {
          endOfDataReject(error);
        },
      },
    );
  }

  process(wvalue: WindowedValue<unknown>): ProcessResult {
    throw Error("Data should not come in via process.");
  }

  split(
    desiredSplit: fnApi.ProcessBundleSplitRequest_DesiredSplit,
  ): fnApi.ProcessBundleSplitResponse_ChannelSplit | undefined {
    if (!this.started) {
      return undefined;
    }
    // If we've already split, we know where the end of this bundle is.
    // Otherwise, use the estimate the runner sent us (which is how much
    // it expects to send us) as the end.
    const end =
      this.lastToProcessElement < Infinity
        ? this.lastToProcessElement
        : Number(desiredSplit.estimatedInputElements) - 1;
    if (this.lastProcessedElement >= end) {
      return undefined;
    }
    // Split fractionOfRemainder of the way between our current position and
    // the end.
    var targetLastToProcessElement = Math.floor(
      this.lastProcessedElement +
        (end - this.lastProcessedElement) * desiredSplit.fractionOfRemainder,
    );
    // If desiredSplit.allowedSplitPoints is populated, try to find the closest
    // split point that's in this list.
    if (desiredSplit.allowedSplitPoints.length) {
      targetLastToProcessElement =
        Math.min(
          ...Array.from(desiredSplit.allowedSplitPoints)
            .filter(
              (allowedSplitPoint) =>
                allowedSplitPoint >= targetLastToProcessElement + 1,
            )
            .map(Number),
        ) - 1;
    }
    // If we were able to find a valid, meaningful split point, record it
    // as the last element that this bundle will process and return the
    // remainder to the runner.
    if (
      this.lastProcessedElement <= targetLastToProcessElement &&
      targetLastToProcessElement < this.lastToProcessElement
    ) {
      this.lastToProcessElement = targetLastToProcessElement;
      return {
        transformId: this.transformId,
        lastPrimaryElement: BigInt(this.lastToProcessElement),
        firstResidualElement: BigInt(this.lastToProcessElement + 1),
      };
    }
    return undefined;
  }

  async finishBundle() {
    try {
      await this.endOfData;
    } finally {
      this.multiplexingDataChannel.unregisterConsumer(
        this.getBundleId(),
        this.transformId,
      );
      this.started = false;
    }
  }
}

registerOperator("beam:runner:source:v1", DataSourceOperator);

class DataSinkOperator implements IOperator {
  transformId: string;
  getBundleId: () => string;
  multiplexingDataChannel: MultiplexingDataChannel;
  channel: IDataChannel;
  coder: Coder<WindowedValue<unknown>>;
  buffer: protobufjs.Writer;

  constructor(
    transformId: string,
    transform: PTransform,
    context: OperatorContext,
  ) {
    const writePort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
    this.multiplexingDataChannel = context.getDataChannel(
      writePort.apiServiceDescriptor!.url,
    );
    this.transformId = transformId;
    this.getBundleId = context.getBundleId;
    this.coder = context.pipelineContext.getCoder(writePort.coderId);
  }

  async startBundle() {
    this.channel = this.multiplexingDataChannel.getSendChannel(
      this.getBundleId(),
      this.transformId,
    );
    this.buffer = new protobufjs.Writer();
  }

  process(wvalue: WindowedValue<unknown>) {
    this.coder.encode(wvalue, this.buffer, CoderContext.needsDelimiters);
    if (this.buffer.len > 1e6) {
      return this.flush();
    }
    return NonPromise;
  }

  async finishBundle() {
    await this.flush();
    this.channel.close();
  }

  async flush() {
    if (this.buffer.len > 0) {
      await this.channel.sendData(this.buffer.finish());
      this.buffer = new protobufjs.Writer();
    }
  }
}

registerOperator("beam:runner:sink:v1", DataSinkOperator);

class FlattenOperator implements IOperator {
  receiver: Receiver;

  constructor(
    public transformId: string,
    transform: PTransform,
    context: OperatorContext,
  ) {
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs)),
    );
  }

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    return this.receiver.receive(wvalue);
  }

  async finishBundle() {}
}

registerOperator("beam:transform:flatten:v1", FlattenOperator);

// CombinePerKey operators.

abstract class CombineOperator<I, A, O> {
  receiver: Receiver;
  combineFn: CombineFn<I, A, O>;

  constructor(
    public transformId: string,
    transform: PTransform,
    context: OperatorContext,
  ) {
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs)),
    );
    const spec = runnerApi.CombinePayload.fromBinary(transform.spec!.payload);
    this.combineFn = deserializeFn(spec.combineFn!.payload).combineFn;
  }
}

export class CombinePerKeyPrecombineOperator<I, A, O>
  extends CombineOperator<I, A, O>
  implements IOperator
{
  keyCoder: Coder<unknown>;
  windowCoder: Coder<Window>;

  groups: Map<string, A>;
  maxKeys: number = 10000;

  static checkSupportsWindowing(
    windowingStrategy: runnerApi.WindowingStrategy,
  ) {
    if (
      windowingStrategy.mergeStatus !== runnerApi.MergeStatus_Enum.NON_MERGING
    ) {
      throw new Error("Unsupported non-merging WindowFn: " + windowingStrategy);
    }
    if (
      windowingStrategy.outputTime !== runnerApi.OutputTime_Enum.END_OF_WINDOW
    ) {
      throw new Error(
        "Unsupported windowing output time: " + windowingStrategy,
      );
    }
  }

  constructor(
    transformId: string,
    transform: PTransform,
    context: OperatorContext,
  ) {
    super(transformId, transform, context);
    const inputPc =
      context.descriptor.pcollections[
        onlyElement(Object.values(transform.inputs))
      ];
    this.keyCoder = context.pipelineContext.getCoder(
      context.descriptor.coders[inputPc.coderId].componentCoderIds[0],
    );
    const windowingStrategy =
      context.descriptor.windowingStrategies[inputPc.windowingStrategyId];
    CombinePerKeyPrecombineOperator.checkSupportsWindowing(windowingStrategy);
    this.windowCoder = context.pipelineContext.getCoder(
      windowingStrategy.windowCoderId,
    );
  }

  process(wvalue: WindowedValue<any>) {
    for (const window of wvalue.windows) {
      const wkey =
        encodeToBase64(window, this.windowCoder) +
        " " +
        encodeToBase64(wvalue.value.key, this.keyCoder);
      if (!this.groups.has(wkey)) {
        this.groups.set(wkey, this.combineFn.createAccumulator());
      }
      this.groups.set(
        wkey,
        this.combineFn.addInput(this.groups.get(wkey), wvalue.value.value),
      );
    }
    if (this.groups.size > this.maxKeys) {
      // Flush a random 10% of the map to make more room.
      // TODO: Tune this, or better use LRU or ARC for this cache.
      return this.flush(this.maxKeys * 0.9);
    } else {
      return NonPromise;
    }
  }

  async startBundle() {
    this.groups = new Map();
  }

  flush(target: number): ProcessResult {
    const result = new ProcessResultBuilder();
    const toDelete: string[] = [];
    for (const [wkey, values] of this.groups) {
      const parts = wkey.split(" ");
      const encodedWindow = parts[0];
      const encodedKey = parts[1];
      const window = decodeFromBase64(encodedWindow, this.windowCoder);
      result.add(
        this.receiver.receive({
          value: {
            key: decodeFromBase64(encodedKey, this.keyCoder),
            value: values,
          },
          windows: [window],
          timestamp: window.maxTimestamp(),
          pane: PaneInfoCoder.ONE_AND_ONLY_FIRING,
        }),
      );
      toDelete.push(wkey);
      if (this.groups.size - toDelete.length <= target) {
        break;
      }
    }
    for (const wkey of toDelete) {
      this.groups.delete(wkey);
    }
    return result.build();
  }

  async finishBundle() {
    const maybePromise = this.flush(0);
    if (maybePromise !== NonPromise) {
      await maybePromise;
    }
    this.groups = null!;
  }
}

registerOperator(
  "beam:transform:combine_per_key_precombine:v1",
  CombinePerKeyPrecombineOperator,
);

class CombinePerKeyMergeAccumulatorsOperator<I, A, O>
  extends CombineOperator<I, A, O>
  implements IOperator
{
  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const { key, value } = wvalue.value as { key: any; value: Iterable<A> };
    return this.receiver.receive({
      value: { key, value: this.combineFn.mergeAccumulators(value) },
      windows: wvalue.windows,
      timestamp: wvalue.timestamp,
      pane: wvalue.pane,
    });
  }

  async finishBundle() {}
}

registerOperator(
  "beam:transform:combine_per_key_merge_accumulators:v1",
  CombinePerKeyMergeAccumulatorsOperator,
);

class CombinePerKeyExtractOutputsOperator<I, A, O>
  extends CombineOperator<I, A, O>
  implements IOperator
{
  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const { key, value } = wvalue.value as { key: any; value: A };
    return this.receiver.receive({
      value: { key, value: this.combineFn.extractOutput(value) },
      windows: wvalue.windows,
      timestamp: wvalue.timestamp,
      pane: wvalue.pane,
    });
  }

  async finishBundle() {}
}

registerOperator(
  "beam:transform:combine_per_key_extract_outputs:v1",
  CombinePerKeyExtractOutputsOperator,
);

class CombinePerKeyConvertToAccumulatorsOperator<I, A, O>
  extends CombineOperator<I, A, O>
  implements IOperator
{
  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const { key, value } = wvalue.value as { key: any; value: I };
    return this.receiver.receive({
      value: {
        key,
        value: this.combineFn.addInput(
          this.combineFn.createAccumulator(),
          value,
        ),
      },
      windows: wvalue.windows,
      timestamp: wvalue.timestamp,
      pane: wvalue.pane,
    });
  }

  async finishBundle() {}
}

registerOperator(
  "beam:transform:combine_per_key_convert_to_accumulators:v1",
  CombinePerKeyConvertToAccumulatorsOperator,
);

class CombinePerKeyCombineGroupedValuesOperator<I, A, O>
  extends CombineOperator<I, A, O>
  implements IOperator
{
  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const { key, value } = wvalue.value as { key: any; value: Iterable<I> };
    let accumulator = this.combineFn.createAccumulator();
    for (const input of value) {
      accumulator = this.combineFn.addInput(accumulator, input);
    }
    return this.receiver.receive({
      value: {
        key,
        value: this.combineFn.extractOutput(accumulator),
      },
      windows: wvalue.windows,
      timestamp: wvalue.timestamp,
      pane: wvalue.pane,
    });
  }

  async finishBundle() {}
}

registerOperator(
  "beam:transform:combine_grouped_values:v1",
  CombinePerKeyCombineGroupedValuesOperator,
);

// ParDo operators.

class GenericParDoOperator implements IOperator {
  private doFn: DoFn<unknown, unknown, unknown>;
  private getStateProvider: () => StateProvider;
  private sideInputInfo: Map<string, SideInputInfo> = new Map();
  private originalContext: object | undefined;
  private augmentedContext: object | undefined;
  private paramProvider: ParamProviderImpl;
  private metricsContainer: MetricsContainer;

  constructor(
    public transformId: string,
    private receiver: Receiver,
    private spec: runnerApi.ParDoPayload,
    private payload: {
      doFn: DoFn<unknown, unknown, unknown>;
      context: any;
    },
    transformProto: runnerApi.PTransform,
    operatorContext: OperatorContext,
  ) {
    this.doFn = payload.doFn;
    this.originalContext = payload.context;
    this.getStateProvider = operatorContext.getStateProvider;
    this.sideInputInfo = createSideInputInfo(
      transformProto,
      spec,
      operatorContext,
    );
    this.metricsContainer = operatorContext.metricsContainer;
  }

  async startBundle() {
    this.paramProvider = new ParamProviderImpl(
      this.transformId,
      this.sideInputInfo,
      this.getStateProvider,
      this.metricsContainer,
    );
    this.augmentedContext = this.paramProvider.augmentContext(
      this.originalContext,
    );
    if (this.doFn.startBundle) {
      this.doFn.startBundle(this.augmentedContext);
    }
  }

  process(wvalue: WindowedValue<unknown>) {
    if (this.augmentedContext && wvalue.windows.length !== 1) {
      // We need to process each window separately.
      // TODO: (Perf) We could inspect the context more deeply and allow some
      // cases to go through.
      const result = new ProcessResultBuilder();
      for (const window of wvalue.windows) {
        result.add(
          this.process({
            value: wvalue.value,
            windows: [window],
            pane: wvalue.pane,
            timestamp: wvalue.timestamp,
          }),
        );
      }
      return result.build();
    }

    const this_ = this;
    function reallyProcess(): ProcessResult {
      const doFnOutput = this_.doFn.process(
        wvalue.value,
        this_.augmentedContext,
      );
      if (!doFnOutput) {
        return NonPromise;
      }
      const result = new ProcessResultBuilder();
      for (const element of doFnOutput) {
        result.add(
          this_.receiver.receive({
            value: element,
            windows: wvalue.windows,
            pane: wvalue.pane,
            timestamp: wvalue.timestamp,
          }),
        );
      }
      this_.paramProvider.setCurrentValue(undefined);
      return result.build();
    }

    // Update the context with any information specific to this window.
    const updateContextResult = this.paramProvider.setCurrentValue(wvalue);

    // If we were able to do so without any deferred actions, process the
    // element immediately.
    if (updateContextResult === NonPromise) {
      return reallyProcess();
    } else {
      // Otherwise return a promise that first waits for all the deferred
      // actions to complete and then process the element.
      return (async () => {
        await updateContextResult;
        const update2 = this.paramProvider.setCurrentValue(wvalue);
        if (update2 !== NonPromise) {
          throw new Error("Expected all promises to be resolved: " + update2);
        }
        await reallyProcess();
      })();
    }
  }

  async finishBundle() {
    if (this.doFn.finishBundle) {
      const finishBundleOutput = this.doFn.finishBundle(this.augmentedContext);
      if (!finishBundleOutput) {
        return;
      }
      // The finishBundle method must return `void` or a Generator<WindowedValue<OutputT>>. It may not
      // return Generator<OutputT> without windowing information because a single bundle may contain
      // elements from different windows, so each element must specify its window.
      for (const element of finishBundleOutput) {
        const maybePromise = this.receiver.receive(element);
        if (maybePromise !== NonPromise) {
          await maybePromise;
        }
      }
    }
  }
}

class IdentityParDoOperator implements IOperator {
  constructor(
    public transformId: string,
    private receiver: Receiver,
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    return this.receiver.receive(wvalue);
  }

  async finishBundle() {}
}

class SplittingDoFnOperator implements IOperator {
  constructor(
    public transformId: string,
    private receivers: { [key: string]: Receiver },
    private options: SplitOptions,
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    const result = new ProcessResultBuilder();
    const keys = Object.keys(wvalue.value as object);
    if (this.options.exclusive && keys.length !== 1) {
      throw new Error(
        "Multiple keys for exclusively split element: " + wvalue.value,
      );
    }
    for (let tag of keys) {
      if (!this.options.knownTags!.includes(tag)) {
        if (this.options.unknownTagBehavior === "rename") {
          tag = this.options.unknownTagName!;
        } else if (this.options.unknownTagBehavior === "ignore") {
          continue;
        } else {
          throw new Error(
            "Unexpected tag '" +
              tag +
              "' for " +
              wvalue.value +
              " not in " +
              this.options.knownTags,
          );
        }
      }
      const receiver = this.receivers[tag];
      if (receiver) {
        result.add(
          receiver.receive({
            value: (wvalue.value as object)[tag],
            windows: wvalue.windows,
            timestamp: wvalue.timestamp,
            pane: wvalue.pane,
          }),
        );
      }
    }
    return result.build();
  }

  async finishBundle() {}
}

class AssignWindowsParDoOperator implements IOperator {
  constructor(
    public transformId: string,
    private receiver: Receiver,
    private windowFn: WindowFn<Window>,
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    const newWindowsOnce = this.windowFn.assignWindows(wvalue.timestamp);
    if (newWindowsOnce.length > 0) {
      // Each element from each original window is assigned to the new windows.
      const newWindows: Window[] = [];
      for (var i = 0; i < wvalue.windows.length; i++) {
        newWindows.push(...newWindowsOnce);
      }
      return this.receiver.receive({
        value: wvalue.value,
        windows: newWindows,
        timestamp: wvalue.timestamp,
        pane: wvalue.pane,
      });
    } else {
      return NonPromise;
    }
  }

  async finishBundle() {}
}

class AssignTimestampsParDoOperator implements IOperator {
  constructor(
    public transformId: string,
    private receiver: Receiver,
    private func: (any, Instant) => typeof Instant,
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    return this.receiver.receive({
      value: wvalue.value,
      windows: wvalue.windows,
      // TODO: Verify it falls in window and doesn't cause late data.
      timestamp: this.func(wvalue.value, wvalue.timestamp),
      pane: wvalue.pane,
    });
  }

  async finishBundle() {}
}

registerOperatorConstructor(
  parDo.urn,
  (transformId: string, transform: PTransform, context: OperatorContext) => {
    const receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs)),
    );
    const spec = runnerApi.ParDoPayload.fromBinary(transform.spec!.payload);
    // TODO: (Cleanup) Ideally we could branch on the urn itself, but some runners have a closed set of known URNs.
    if (spec.doFn?.urn === urns.SERIALIZED_JS_DOFN_INFO) {
      return new GenericParDoOperator(
        transformId,
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        spec,
        deserializeFn(spec.doFn.payload!),
        transform,
        context,
      );
    } else if (spec.doFn?.urn === urns.IDENTITY_DOFN_URN) {
      return new IdentityParDoOperator(
        transformId,
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
      );
    } else if (spec.doFn?.urn === urns.JS_WINDOW_INTO_DOFN_URN) {
      return new AssignWindowsParDoOperator(
        transformId,
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        deserializeFn(spec.doFn.payload!).windowFn,
      );
    } else if (spec.doFn?.urn === urns.JS_ASSIGN_TIMESTAMPS_DOFN_URN) {
      return new AssignTimestampsParDoOperator(
        transformId,
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        deserializeFn(spec.doFn.payload!).func,
      );
    } else if (spec.doFn?.urn === urns.SPLITTING_JS_DOFN_URN) {
      return new SplittingDoFnOperator(
        transformId,
        Object.fromEntries(
          Object.entries(transform.outputs).map(([tag, pcId]) => [
            tag,
            context.getReceiver(pcId),
          ]),
        ),
        deserializeFn(spec.doFn.payload!),
      );
    } else {
      throw new Error("Unknown DoFn type: " + spec);
    }
  },
);

///

export function encodeToBase64<T>(element: T, coder: Coder<T>): string {
  const writer = new protobufjs.Writer();
  coder.encode(element, writer, CoderContext.wholeStream);
  return Buffer.from(writer.finish()).toString("base64");
}

export function decodeFromBase64<T>(s: string, coder: Coder<T>): T {
  return coder.decode(
    new protobufjs.Reader(Buffer.from(s, "base64")),
    CoderContext.wholeStream,
  );
}

function onlyElement<Type>(arg: Type[]): Type {
  if (arg.length > 1) {
    Error("Expecting exactly one element.");
  }
  return arg[0];
}
