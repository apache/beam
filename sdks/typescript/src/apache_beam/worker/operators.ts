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
import { StateProvider } from "./state";

import * as urns from "../internal/urns";
import { PipelineContext } from "../internal/pipeline";
import { deserializeFn } from "../internal/serialize";
import { Coder, Context as CoderContext } from "../coders/coders";
import { Window, Instant, WindowedValue } from "../values";
import { parDo, DoFn, SplitOptions } from "../transforms/pardo";
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
  startBundle: () => Promise<void>;
  // As this is called at every operator at every element, and the vast majority
  // of the time Promises are not needed, we wish to avoid the overhead of
  // creating promisses and await as much as possible.
  process: (wv: WindowedValue<unknown>) => ProcessResult;
  finishBundle: () => Promise<void>;
}

export class Receiver {
  constructor(private operators: IOperator[]) {}

  receive(wvalue: WindowedValue<unknown>): ProcessResult {
    if (this.operators.length === 1) {
      return this.operators[0].process(wvalue);
    } else {
      const result = new ProcessResultBuilder();
      for (const operator of this.operators) {
        result.add(operator.process(wvalue));
      }
      return result.build();
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
    public getBundleId: () => string
  ) {
    this.pipelineContext = new PipelineContext(descriptor);
  }
}

export function createOperator(
  transformId: string,
  context: OperatorContext
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
  context: OperatorContext
) => IOperator;
interface OperatorClass {
  new (
    transformId: string,
    transformProto: PTransform,
    context: OperatorContext
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
  constructor: OperatorConstructor
) {
  operatorsByUrn.set(urn, constructor);
}

////////// Actual operator implementation. //////////

// NOTE: It may have been more idiomatic to use objects in closures satisfying
// the IOperator interface here, but classes are used to make a clearer pattern
// potential SDK authors that are less familiar with javascript.

class DataSourceOperator implements IOperator {
  transformId: string;
  getBundleId: () => string;
  multiplexingDataChannel: MultiplexingDataChannel;
  receiver: Receiver;
  coder: Coder<WindowedValue<unknown>>;
  endOfData: Promise<void>;

  constructor(
    transformId: string,
    transform: PTransform,
    context: OperatorContext
  ) {
    const readPort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
    this.multiplexingDataChannel = context.getDataChannel(
      readPort.apiServiceDescriptor!.url
    );
    this.transformId = transformId;
    this.getBundleId = context.getBundleId;
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs))
    );
    this.coder = context.pipelineContext.getCoder(readPort.coderId);
  }

  async startBundle() {
    const this_ = this;
    var endOfDataResolve, endOfDataReject;
    this.endOfData = new Promise(async (resolve, reject) => {
      endOfDataResolve = resolve;
      endOfDataReject = reject;
    });

    await this_.multiplexingDataChannel.registerConsumer(
      this_.getBundleId(),
      this_.transformId,
      {
        sendData: async function (data: Uint8Array) {
          console.log("Got", data);
          const reader = new protobufjs.Reader(data);
          while (reader.pos < reader.len) {
            const maybePromise = this_.receiver.receive(
              this_.coder.decode(reader, CoderContext.needsDelimiters)
            );
            if (maybePromise !== NonPromise) {
              await maybePromise;
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
      }
    );
  }

  process(wvalue: WindowedValue<unknown>): ProcessResult {
    throw Error("Data should not come in via process.");
  }

  async finishBundle() {
    try {
      await this.endOfData;
    } finally {
      this.multiplexingDataChannel.unregisterConsumer(
        this.getBundleId(),
        this.transformId
      );
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
    context: OperatorContext
  ) {
    const writePort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
    this.multiplexingDataChannel = context.getDataChannel(
      writePort.apiServiceDescriptor!.url
    );
    this.transformId = transformId;
    this.getBundleId = context.getBundleId;
    this.coder = context.pipelineContext.getCoder(writePort.coderId);
  }

  async startBundle() {
    this.channel = this.multiplexingDataChannel.getSendChannel(
      this.getBundleId(),
      this.transformId
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
    transformId: string,
    transform: PTransform,
    context: OperatorContext
  ) {
    this.receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs))
    );
  }

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    return this.receiver.receive(wvalue);
  }

  async finishBundle() {}
}

registerOperator("beam:transform:flatten:v1", FlattenOperator);

class GenericParDoOperator implements IOperator {
  private doFn: DoFn<unknown, unknown, unknown>;
  private getStateProvider: () => StateProvider;
  private sideInputInfo: Map<string, SideInputInfo> = new Map();
  private originalContext: object | undefined;
  private augmentedContext: object | undefined;
  private paramProvider: ParamProviderImpl;

  constructor(
    private transformId: string,
    private receiver: Receiver,
    private spec: runnerApi.ParDoPayload,
    private payload: {
      doFn: DoFn<unknown, unknown, unknown>;
      context: any;
    },
    transformProto: runnerApi.PTransform,
    operatorContext: OperatorContext
  ) {
    this.doFn = payload.doFn;
    this.originalContext = payload.context;
    this.getStateProvider = operatorContext.getStateProvider;
    this.sideInputInfo = createSideInputInfo(
      transformProto,
      spec,
      operatorContext
    );
  }

  async startBundle() {
    this.paramProvider = new ParamProviderImpl(
      this.transformId,
      this.sideInputInfo,
      this.getStateProvider
    );
    this.augmentedContext = this.paramProvider.augmentContext(
      this.originalContext
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
          })
        );
      }
      return result.build();
    }

    const this_ = this;
    function reallyProcess(): ProcessResult {
      const doFnOutput = this_.doFn.process(
        wvalue.value,
        this_.augmentedContext
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
          })
        );
      }
      this_.paramProvider.update(undefined);
      return result.build();
    }

    // Update the context with any information specific to this window.
    const updateContextResult = this.paramProvider.update(wvalue);

    // If we were able to do so without any deferred actions, process the
    // element immediately.
    if (updateContextResult === NonPromise) {
      return reallyProcess();
    } else {
      // Otherwise return a promise that first waits for all the deferred
      // actions to complete and then process the element.
      return (async () => {
        await updateContextResult;
        const update2 = this.paramProvider.update(wvalue);
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
  constructor(private receiver: Receiver) {}

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    return this.receiver.receive(wvalue);
  }

  async finishBundle() {}
}

class SplittingDoFnOperator implements IOperator {
  constructor(
    private receivers: { [key: string]: Receiver },
    private options: SplitOptions
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<unknown>) {
    const result = new ProcessResultBuilder();
    const keys = Object.keys(wvalue.value as object);
    if (this.options.exclusive && keys.length !== 1) {
      throw new Error(
        "Multiple keys for exclusively split element: " + wvalue.value
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
              this.options.knownTags
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
          })
        );
      }
    }
    return result.build();
  }

  async finishBundle() {}
}

class AssignWindowsParDoOperator implements IOperator {
  constructor(private receiver: Receiver, private windowFn: WindowFn<Window>) {}

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
    private receiver: Receiver,
    private func: (any, Instant) => typeof Instant
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
      onlyElement(Object.values(transform.outputs))
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
        context
      );
    } else if (spec.doFn?.urn === urns.IDENTITY_DOFN_URN) {
      return new IdentityParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs)))
      );
    } else if (spec.doFn?.urn === urns.JS_WINDOW_INTO_DOFN_URN) {
      return new AssignWindowsParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        deserializeFn(spec.doFn.payload!).windowFn
      );
    } else if (spec.doFn?.urn === urns.JS_ASSIGN_TIMESTAMPS_DOFN_URN) {
      return new AssignTimestampsParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        deserializeFn(spec.doFn.payload!).func
      );
    } else if (spec.doFn?.urn === urns.SPLITTING_JS_DOFN_URN) {
      return new SplittingDoFnOperator(
        Object.fromEntries(
          Object.entries(transform.outputs).map(([tag, pcId]) => [
            tag,
            context.getReceiver(pcId),
          ])
        ),
        deserializeFn(spec.doFn.payload!)
      );
    } else {
      throw new Error("Unknown DoFn type: " + spec);
    }
  }
);

function onlyElement<Type>(arg: Type[]): Type {
  if (arg.length > 1) {
    Error("Expecting exactly one element.");
  }
  return arg[0];
}
