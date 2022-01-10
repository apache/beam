import * as protobufjs from "protobufjs";

import { PTransform, PCollection } from "../proto/beam_runner_api";
import * as runnerApi from "../proto/beam_runner_api";
import { ProcessBundleDescriptor, RemoteGrpcPort } from "../proto/beam_fn_api";
import { MultiplexingDataChannel, IDataChannel } from "./data";

import * as base from "../base";
import * as translations from "../internal/translations";
import { Coder, Context as CoderContext } from "../coders/coders";
import { BoundedWindow, Instant, PaneInfo, WindowedValue } from "../values";

export interface IOperator {
  startBundle: () => void;
  process: (wv: WindowedValue<any>) => void;
  finishBundle: () => void;
}

export class Receiver {
  constructor(private operators: IOperator[]) {}

  receive(wvalue: WindowedValue<any>) {
    for (const operator of this.operators) {
      operator.process(wvalue);
    }
  }
}

export class OperatorContext {
  pipelineContext: base.PipelineContext;
  constructor(
    public descriptor: ProcessBundleDescriptor,
    public getReceiver: (string) => Receiver,
    public getDataChannel: (string) => MultiplexingDataChannel,
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
  done: boolean;
  error?: Error;

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

  startBundle() {
    this.done = false;
    const this_ = this;
    this.multiplexingDataChannel.registerConsumer(
      this.getBundleId(),
      this.transformId,
      {
        sendData: function (data: Uint8Array) {
          console.log("Got", data);
          const reader = new protobufjs.Reader(data);
          while (reader.pos < reader.len) {
            this_.receiver.receive(
              this_.coder.decode(reader, CoderContext.needsDelimiters)
            );
          }
        },
        sendTimers: function (timerFamilyId: string, timers: Uint8Array) {
          throw Error("Not expecting timers.");
        },
        close: function () {
          this_.done = true;
        },
        onError: function (error: Error) {
          this_.done = true;
          this_.error = error;
        },
      }
    );
  }

  process(wvalue: WindowedValue<any>) {
    throw Error("Data should not come in via process.");
  }

  finishBundle() {
    // TODO: Await this condition.
    //         console.log("Waiting for all data.")
    //         await new Promise((resolve) => {
    //             setTimeout(resolve, 3000);
    //         });
    //         if (!this.done) {
    //             throw Error("Not done!");
    //         }
    //         console.log("Done waiting for all data.")
    this.multiplexingDataChannel.unregisterConsumer(
      this.getBundleId(),
      this.transformId
    );
    if (this.error) {
      throw this.error;
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

  startBundle() {
    this.channel = this.multiplexingDataChannel.getSendChannel(
      this.getBundleId(),
      this.transformId
    );
    this.buffer = new protobufjs.Writer();
  }

  process(wvalue: WindowedValue<any>) {
    this.coder.encode(wvalue, this.buffer, CoderContext.needsDelimiters);
    if (this.buffer.len > 1e6) {
      this.flush();
    }
  }

  finishBundle() {
    this.flush();
    this.channel.close();
  }

  flush() {
    if (this.buffer.len > 0) {
      this.channel.sendData(this.buffer.finish());
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

  startBundle() {}

  process(wvalue: WindowedValue<any>) {
    this.receiver.receive(wvalue);
  }

  finishBundle() {}
}

registerOperator("beam:transform:flatten:v1", FlattenOperator);

class GenericParDoOperator implements IOperator {
  constructor(
    private receiver: Receiver,
    private spec: runnerApi.ParDoPayload,
    private doFn: base.DoFn<any, any>
  ) {}

  startBundle() {
    this.doFn.startBundle();
  }

  process(wvalue: WindowedValue<any>) {
    const doFnOutput = this.doFn.process(wvalue.value);
    if (!doFnOutput) {
      return;
    }
    for (const element of doFnOutput) {
      this.receiver.receive({
        value: element,
        windows: wvalue.windows,
        pane: wvalue.pane,
        timestamp: wvalue.timestamp,
      });
    }
  }

  finishBundle() {
    const finishBundleOutput = this.doFn.finishBundle();
    if (!finishBundleOutput) {
      return;
    }
    // The finishBundle method must return `void` or a Generator<WindowedValue<OutputT>>. It may not
    // return Generator<OutputT> without windowing information because a single bundle may contain
    // elements from different windows, so each element must specify its window.
    for (const element of finishBundleOutput) {
      this.receiver.receive(element);
    }
  }
}

class IdentityParDoOperator implements IOperator {
  constructor(private receiver: Receiver) {}

  startBundle() {}

  process(wvalue: WindowedValue<any>) {
    this.receiver.receive(wvalue);
  }

  finishBundle() {}
}

class SplittingDoFnOperator implements IOperator {
  constructor(
    private splitter: (any) => string,
    private receivers: { [key: string]: Receiver }
  ) {}

  startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const tag = this.splitter(wvalue.value);
    const receiver = this.receivers[tag];
    if (receiver) {
      receiver.receive(wvalue);
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

  finishBundle() {}
}

class AssignWindowsParDoOperator implements IOperator {
  constructor(
    private receiver: Receiver,
    private windowFn: base.WindowFn<any>
  ) {}

  startBundle() {}

  process(wvalue: WindowedValue<any>) {
    const newWindowsOnce = this.windowFn.assignWindows(wvalue.timestamp);
    if (newWindowsOnce.length > 0) {
      const newWindows: BoundedWindow[] = [];
      for (var i = 0; i < wvalue.windows.length; i++) {
        newWindows.push(...newWindowsOnce);
      }
      this.receiver.receive({
        value: wvalue.value,
        windows: newWindows,
        // TODO: Verify it falls in window and doesn't cause late data.
        timestamp: wvalue.timestamp,
        pane: wvalue.pane,
      });
    }
  }

  finishBundle() {}
}

class AssignTimestampsParDoOperator implements IOperator {
  constructor(
    private receiver: Receiver,
    private func: (any, Instant) => typeof Instant
  ) {}

  startBundle() {}

  process(wvalue: WindowedValue<any>) {
    this.receiver.receive({
      value: wvalue.value,
      windows: wvalue.windows,
      // TODO: Verify it falls in window and doesn't cause late data.
      timestamp: this.func(wvalue.value, wvalue.timestamp),
      pane: wvalue.pane,
    });
  }

  finishBundle() {}
}

registerOperatorConstructor(
  base.ParDo.urn,
  (transformId: string, transform: PTransform, context: OperatorContext) => {
    const receiver = context.getReceiver(
      onlyElement(Object.values(transform.outputs))
    );
    const spec = runnerApi.ParDoPayload.fromBinary(transform.spec!.payload);
    // TODO: Ideally we could branch on the urn itself, but some runners have a closed set of known URNs.
    if (spec.doFn?.urn == translations.SERIALIZED_JS_DOFN_INFO) {
      return new GenericParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        spec,
        base.fakeDeserialize(spec.doFn.payload!)
      );
    } else if (spec.doFn?.urn == translations.IDENTITY_DOFN_URN) {
      return new IdentityParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs)))
      );
    } else if (spec.doFn?.urn == translations.JS_WINDOW_INTO_DOFN_URN) {
      return new AssignWindowsParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        base.fakeDeserialize(spec.doFn.payload!).windowFn
      );
    } else if (spec.doFn?.urn == translations.JS_ASSIGN_TIMESTAMPS_DOFN_URN) {
      return new AssignTimestampsParDoOperator(
        context.getReceiver(onlyElement(Object.values(transform.outputs))),
        base.fakeDeserialize(spec.doFn.payload!).func
      );
    } else if (spec.doFn?.urn == translations.SPLITTING_JS_DOFN_URN) {
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

  startBundle() {}

  process(wvalue) {
    console.log(
      "forwarding",
      wvalue.value,
      "for",
      this.transformId,
      this.transformUrn
    );
    this.receivers.map((receiver: Receiver) => receiver.receive(wvalue));
  }

  finishBundle() {}
}

function onlyElement<Type>(arg: Type[]): Type {
  if (arg.length > 1) {
    Error("Expecting exactly one element.");
  }
  return arg[0];
}
