import * as protobufjs from "protobufjs";

import { PTransform, PCollection } from "../proto/beam_runner_api";
import * as runnerApi from "../proto/beam_runner_api";
import * as fnApi from "../proto/beam_fn_api";
import { ProcessBundleDescriptor, RemoteGrpcPort } from "../proto/beam_fn_api";
import { MultiplexingDataChannel, IDataChannel } from "./data";
import { StateProvider } from "./state";

import * as base from "../base";
import * as urns from "../internal/urns";
import { Coder, Context as CoderContext } from "../coders/coders";
import { GlobalWindowCoder } from "../coders/required_coders";
import { BoundedWindow, Instant, PaneInfo, WindowedValue } from "../values";
import {
  DoFn,
  ParDoParam,
  ParamProvider,
  SideInputParam,
} from "../transforms/pardo";

// Trying to get some of https://github.com/microsoft/TypeScript/issues/8240
export class NonPromiseClass {
  static INSTANCE = new NonPromiseClass();
  private constructor() {}
}
export const NonPromise = NonPromiseClass.INSTANCE;

export type ProcessResult = NonPromiseClass | Promise<void>;

export interface IOperator {
  startBundle: () => Promise<void>;
  // As this is called at every operator at every element, and the vast majority
  // of the time Promises are not needed, we wish to avoid the overhead of
  // creating promisses and await as much as possible.
  process: (wv: WindowedValue<any>) => ProcessResult;
  finishBundle: () => Promise<void>;
}

export class Receiver {
  constructor(private operators: IOperator[]) {}

  receive(wvalue: WindowedValue<any>): ProcessResult {
    if (this.operators.length == 1) {
      return this.operators[0].process(wvalue);
    } else {
      const promises: Promise<void>[] = [];
      for (const operator of this.operators) {
        const maybePromise = operator.process(wvalue);
        if (maybePromise != NonPromise) {
          promises.push(maybePromise as Promise<void>);
        }
      }
      if (promises.length == 0) {
        return NonPromise;
      } else if (promises.length == 1) {
        return promises[0];
      } else {
        return Promise.all(promises).then(() => null);
      }
    }
  }
}

export class OperatorContext {
  pipelineContext: base.PipelineContext;
  constructor(
    public descriptor: ProcessBundleDescriptor,
    public getReceiver: (string) => Receiver,
    public getDataChannel: (string) => MultiplexingDataChannel,
    public getStateProvider: () => StateProvider,
    public getBundleId: () => string
  ) {
    this.pipelineContext = new base.PipelineContext(descriptor);
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
  if (operatorConstructor == undefined) {
    console.log("Unknown transform type:", transform.spec?.urn);
    // TODO: For testing only...
    operatorConstructor = (transformId, transformProto, context) => {
      return new PassThroughOperator(transformId, transformProto, context);
    };
  }
  return operatorConstructor(transformId, transform, context);
}

// TODO: Is there a good way to get the construtor as a function to avoid this new operator hacking?
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

class DataSourceOperator implements IOperator {
  transformId: string;
  getBundleId: () => string;
  multiplexingDataChannel: MultiplexingDataChannel;
  receiver: Receiver;
  coder: Coder<any>;
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
            if (maybePromise != NonPromise) {
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

  process(wvalue: WindowedValue<any>): ProcessResult {
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
  coder: Coder<any>;
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

  process(wvalue: WindowedValue<any>) {
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

  process(wvalue: WindowedValue<any>) {
    return this.receiver.receive(wvalue);
  }

  async finishBundle() {}
}

registerOperator("beam:transform:flatten:v1", FlattenOperator);

interface SideInputInfo {
  elementCoder: Coder<any>;
  windowCoder: Coder<BoundedWindow>;
  windowMappingFn: (window: BoundedWindow) => BoundedWindow;
}

class GenericParDoOperator implements IOperator {
  private doFn: base.DoFn<any, any, any>;
  private originalContext: object | undefined;
  private augmentedContext: object | undefined;
  private paramProvider: ParamProviderImpl;
  private getStateProvider: () => StateProvider;
  private sideInputInfo: Map<string, SideInputInfo> = new Map();

  constructor(
    private transformId: string,
    private receiver: Receiver,
    private spec: runnerApi.ParDoPayload,
    private payload: { doFn: base.DoFn<any, any, any>; context: any },
    transformProto: runnerApi.PTransform,
    operatorContext: OperatorContext
  ) {
    this.doFn = payload.doFn;
    this.originalContext = payload.context;
    this.getStateProvider = operatorContext.getStateProvider;
    for (const [sideInputId, sideInput] of Object.entries(spec.sideInputs)) {
      const sidePColl =
        operatorContext.descriptor.pcollections[
          transformProto.inputs[sideInputId]
        ];
      const windowingStrategy =
        operatorContext.pipelineContext.getWindowingStrategy(
          sidePColl.windowingStrategyId
        );
      this.sideInputInfo.set(sideInputId, {
        elementCoder: operatorContext.pipelineContext.getCoder(
          sidePColl.coderId
        ),
        windowCoder: operatorContext.pipelineContext.getCoder(
          windowingStrategy.windowCoderId
        ),
        windowMappingFn: (window) => window,
      });
    }
  }

  async startBundle() {
    this.doFn.startBundle();
    this.paramProvider = new ParamProviderImpl(
      this.transformId,
      this.sideInputInfo,
      this.getStateProvider
    );
    this.augmentedContext = this.paramProvider.augmentContext(
      this.originalContext
    );
  }

  process(wvalue: WindowedValue<any>) {
    if (this.augmentedContext && wvalue.windows.length != 1) {
      // We need to process each window separately.
      const promises: Promise<void>[] = [];
      for (const window of wvalue.windows) {
        const maybePromise = this.process({
          value: wvalue.value,
          windows: [window],
          pane: wvalue.pane,
          timestamp: wvalue.timestamp,
        });
        if (maybePromise != NonPromise) {
          promises.push(maybePromise as Promise<void>);
        }
      }
      if (promises.length == 0) {
        return NonPromise;
      } else if (promises.length == 1) {
        return promises[0];
      } else {
        return Promise.all(promises).then(() => null);
      }
    }

    const updateResult = this.paramProvider.update(wvalue);

    const this_ = this;
    function reallyProcess(): ProcessResult {
      const doFnOutput = this_.doFn.process(
        wvalue.value,
        this_.augmentedContext
      );
      if (!doFnOutput) {
        return NonPromise;
      }
      // promises logic inlined as it's performance critical
      // TODO: Actually benchmark.
      const promises: Promise<void>[] = [];
      for (const element of doFnOutput) {
        const maybePromise = this_.receiver.receive({
          value: element,
          windows: wvalue.windows,
          pane: wvalue.pane,
          timestamp: wvalue.timestamp,
        });
        if (maybePromise != NonPromise) {
          promises.push(maybePromise as Promise<void>);
        }
      }
      this_.paramProvider.update(undefined);
      if (promises.length == 0) {
        return NonPromise;
      } else if (promises.length == 1) {
        return promises[0];
      } else {
        return Promise.all(promises).then(() => null);
      }
    }

    if (updateResult == NonPromise) {
      return reallyProcess();
    } else {
      return (async () => {
        await updateResult;
        const updateResult2 = this.paramProvider.update(wvalue);
        if (updateResult2 != NonPromise) {
          throw new Error(
            "Expected all required promises to be resolved: " + updateResult2
          );
        }
        await reallyProcess();
      })();
    }
  }

  async finishBundle() {
    const finishBundleOutput = this.doFn.finishBundle();
    if (!finishBundleOutput) {
      return;
    }
    // The finishBundle method must return `void` or a Generator<WindowedValue<OutputT>>. It may not
    // return Generator<OutputT> without windowing information because a single bundle may contain
    // elements from different windows, so each element must specify its window.
    for (const element of finishBundleOutput) {
      const maybePromise = this.receiver.receive(element);
      if (maybePromise != NonPromise) {
        await maybePromise;
      }
    }
  }
}

export function createStateKey(
  transformId: string,
  accessPattern: string,
  sideInputId: string,
  window: BoundedWindow,
  windowCoder: Coder<BoundedWindow>
): fnApi.StateKey {
  const writer = new protobufjs.Writer();
  windowCoder.encode(window, writer, CoderContext.needsDelimiters);
  const encodedWindow = writer.finish();

  switch (accessPattern) {
    case "beam:side_input:iterable:v1":
      return {
        type: {
          oneofKind: "iterableSideInput",
          iterableSideInput: {
            transformId: transformId,
            sideInputId: sideInputId,
            // TODO: Map and encode. This is the global window.
            window: encodedWindow,
          },
        },
      };

    default:
      throw new Error("Unimplemented access pattern: " + accessPattern);
  }
}

class ParamProviderImpl implements ParamProvider {
  wvalue: WindowedValue<any> | undefined = undefined;
  prefetchCallbacks: ((window: BoundedWindow) => ProcessResult)[];
  sideInputValues: Map<string, any> = new Map();

  constructor(
    private transformId: string,
    private sideInputInfo: Map<string, SideInputInfo>,
    private getStateProvider: () => StateProvider
  ) {}

  // Avoid modifying the original object, as that could have surprising results
  // if they are widely shared.
  augmentContext(context: any) {
    this.prefetchCallbacks = [];
    if (typeof context != "object") {
      return context;
    }

    const result = Object.create(context);
    for (const [name, value] of Object.entries(context)) {
      // Is this the best way to check post serialization?
      if (
        typeof value == "object" &&
        value != null &&
        value["parDoParamName"] != undefined
      ) {
        result[name] = Object.create(value);
        result[name].provider = this;
        if ((value as ParDoParam<any>).parDoParamName == "sideInput") {
          this.prefetchCallbacks.push(
            this.prefetchSideInput(value as SideInputParam<any, any, any>)
          );
        }
      }
    }
    return result;
  }

  prefetchSideInput(
    param: SideInputParam<any, any, any>
  ): (window: BoundedWindow) => ProcessResult {
    const this_ = this;
    const stateProvider = this.getStateProvider();
    const { windowCoder, elementCoder, windowMappingFn } =
      this.sideInputInfo.get(param.sideInputId)!;
    const isGlobal = windowCoder instanceof GlobalWindowCoder;
    const decode = (encodedElements: Uint8Array) => {
      return param.accessor.toValue(
        (function* () {
          const reader = new protobufjs.Reader(encodedElements);
          while (reader.pos < reader.len) {
            yield elementCoder.decode(reader, CoderContext.needsDelimiters);
          }
        })()
      );
    };
    return (window: BoundedWindow) => {
      if (isGlobal && this_.sideInputValues.has(param.sideInputId)) {
        return NonPromise;
      }
      const stateKey = createStateKey(
        this_.transformId,
        param.accessor.accessPattern,
        param.sideInputId,
        window,
        windowCoder
      );
      const lookupResult = stateProvider.getState(stateKey, decode);
      if (lookupResult.type == "value") {
        this_.sideInputValues.set(param.sideInputId, lookupResult.value);
        return NonPromise;
      } else {
        return lookupResult.promise.then((value) => {
          this_.sideInputValues.set(param.sideInputId, value);
        });
      }
    };
  }

  update(wvalue: WindowedValue<any> | undefined): ProcessResult {
    this.wvalue = wvalue;
    if (wvalue == undefined) {
      return NonPromise;
    }
    // We have to prefetch all the side inputs.
    // TODO: Let the user's process() await them.
    if (this.prefetchCallbacks.length == 0) {
      return NonPromise;
    } else {
      const promises: Promise<void>[] = [];
      for (const cb of this.prefetchCallbacks) {
        const result = cb(wvalue!.windows[0]);
        if (result != NonPromise) {
          promises.push(result as Promise<void>);
        }
      }
      if (promises.length == 0) {
        return NonPromise;
      } else if (promises.length == 1) {
        return promises[0];
      } else {
        return Promise.all(promises).then(() => null);
      }
    }
  }

  provide(param) {
    if (this.wvalue == undefined) {
      throw new Error(
        param.parDoParamName + " not defined outside of a process() call."
      );
    }

    switch (param.parDoParamName) {
      case "window":
        // If we're here and there was more than one window, we have exploded.
        return this.wvalue.windows[0];

      case "sideInput":
        return this.sideInputValues.get(param.sideInputId);

      default:
        throw new Error("Unknown context parameter: " + param.parDoParamName);
    }
  }
}

class IdentityParDoOperator implements IOperator {
  constructor(private receiver: Receiver) {}

  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    return this.receiver.receive(wvalue);
  }

  async finishBundle() {}
}

class SplittingDoFnOperator implements IOperator {
  constructor(
    private splitter: (any) => string,
    private receivers: { [key: string]: Receiver }
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const tag = this.splitter(wvalue.value);
    const receiver = this.receivers[tag];
    if (receiver) {
      return receiver.receive(wvalue);
    } else {
      // TODO: Make this configurable.
      throw new Error(
        "Unexpected tag '" +
          tag +
          "' for " +
          wvalue.value +
          " not in " +
          [...Object.keys(this.receivers)]
      );
    }
  }

  async finishBundle() {}
}

class AssignWindowsParDoOperator implements IOperator {
  constructor(
    private receiver: Receiver,
    private windowFn: base.WindowFn<any>
  ) {}

  async startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const newWindowsOnce = this.windowFn.assignWindows(wvalue.timestamp);
    if (newWindowsOnce.length > 0) {
      const newWindows: BoundedWindow[] = [];
      for (var i = 0; i < wvalue.windows.length; i++) {
        newWindows.push(...newWindowsOnce);
      }
      return this.receiver.receive({
        value: wvalue.value,
        windows: newWindows,
        // TODO: Verify it falls in window and doesn't cause late data.
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

  process(wvalue: WindowedValue<any>) {
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
  base.ParDo.urn,
  (transformId: string, transform: PTransform, context: OperatorContext) => {
    const receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs))
    );
    const spec = runnerApi.ParDoPayload.fromBinary(transform.spec!.payload);
    // TODO: Ideally we could branch on the urn itself, but some runners have a closed set of known URNs.
    if (spec.doFn?.urn == urns.SERIALIZED_JS_DOFN_INFO) {
      return new GenericParDoOperator(
        transformId,
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        spec,
        base.fakeDeserialize(spec.doFn.payload!),
        transform,
        context
      );
    } else if (spec.doFn?.urn == urns.IDENTITY_DOFN_URN) {
      return new IdentityParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs)))
      );
    } else if (spec.doFn?.urn == urns.JS_WINDOW_INTO_DOFN_URN) {
      return new AssignWindowsParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        base.fakeDeserialize(spec.doFn.payload!).windowFn
      );
    } else if (spec.doFn?.urn == urns.JS_ASSIGN_TIMESTAMPS_DOFN_URN) {
      return new AssignTimestampsParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        base.fakeDeserialize(spec.doFn.payload!).func
      );
    } else if (spec.doFn?.urn == urns.SPLITTING_JS_DOFN_URN) {
      return new SplittingDoFnOperator(
        base.fakeDeserialize(spec.doFn.payload!).splitter,
        Object.fromEntries(
          Object.entries(transform.outputs).map(([tag, pcId]) => [
            tag,
            context.getReceiver(pcId),
          ])
        )
      );
    } else {
      throw new Error("Unknown DoFn type: " + spec);
    }
  }
);

class PassThroughOperator implements IOperator {
  transformId: string;
  transformUrn: string;
  receivers: Receiver[];

  constructor(
    transformId: string,
    transform: PTransform,
    context: OperatorContext
  ) {
    this.transformId = transformId;
    this.transformUrn = transform.spec!.urn;
    this.receivers = Object.values(transform.outputs).map(context.getReceiver);
  }

  async startBundle() {}

  process(wvalue) {
    console.log(
      "forwarding",
      wvalue.value,
      "for",
      this.transformId,
      this.transformUrn
    );
    this.receivers.map((receiver: Receiver) => receiver.receive(wvalue));
    // TODO: Delete PassThroughOperator.
    return NonPromise;
  }

  async finishBundle() {}
}

function onlyElement<Type>(arg: Type[]): Type {
  if (arg.length > 1) {
    Error("Expecting exactly one element.");
  }
  return arg[0];
}
