import * as protobufjs from 'protobufjs';

import { PTransform, PCollection } from "../proto/beam_runner_api";
import * as runnerApi from "../proto/beam_runner_api";
import { ProcessBundleDescriptor, RemoteGrpcPort } from "../proto/beam_fn_api";
import { MultiplexingDataChannel, IDataChannel } from "./data"

import * as base from "../base"
import {BoundedWindow, PaneInfo, WindowedValue} from "../base"
import * as translations from '../internal/translations'
import { Coder, Context as CoderContext } from "../coders/coders"


export interface IOperator {
    startBundle: () => void;
    process: (wv: WindowedValue<any>) => void;
    finishBundle: () => void;
}

export class Receiver {
    constructor(private operators: IOperator[]) {
    }

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
        public getBundleId: () => string) {
        this.pipelineContext = new base.PipelineContext(descriptor);
    }
}

export function createOperator(
    transformId: string,
    context: OperatorContext,
): IOperator {
    const transform = context.descriptor.transforms[transformId];
    // Ensure receivers are eagerly created.
    Object.values(transform.outputs).map(context.getReceiver)
    let operatorClass = operatorsByUrn.get(transform.spec!.urn!);
    if (operatorClass == undefined) {
        console.log("Unknown transform type:", transform.spec?.urn);
        operatorClass = PassThroughOperator;
    }
    return new operatorClass(transformId, transform, context);
}

interface OperatorClass {
    new(transformId: string, transformProto: PTransform, context: OperatorContext): IOperator;
}

const operatorsByUrn: Map<string, OperatorClass> = new Map();

export function registerOperator(urn: string, cls: OperatorClass) {
    operatorsByUrn.set(urn, cls);
}

////////// Actual operator implementation. //////////

class DataSourceOperator implements IOperator {
    transformId: string
    getBundleId: () => string;
    multiplexingDataChannel: MultiplexingDataChannel;
    receiver: Receiver;
    coder: Coder<any>;
    done: boolean;

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        const readPort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
        this.multiplexingDataChannel = context.getDataChannel(readPort.apiServiceDescriptor!.url);
        this.transformId = transformId;
        this.getBundleId = context.getBundleId;
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
        this.coder = context.pipelineContext.getCoder(readPort.coderId);
    }

    startBundle() {
        this.done = false;
        const this_ = this;
        this.multiplexingDataChannel.registerConsumer(
            this.getBundleId(),
            this.transformId,
            {
                sendData: function(data: Uint8Array) {
                    console.log("Got", data);
                    const reader = new protobufjs.Reader(data);
                    while (reader.pos < reader.len) {
                        this_.receiver.receive(this_.coder.decode(reader, CoderContext.needsDelimiters));
                    }
                },
                sendTimers: function(timerFamilyId: string, timers: Uint8Array) {
                    throw Error("Not expecting timers.");
                },
                close: function() { this_.done = true; },
            });
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
        this.multiplexingDataChannel.unregisterConsumer(this.getBundleId(), this.transformId);
    }
}

registerOperator("beam:runner:source:v1", DataSourceOperator);



class DataSinkOperator implements IOperator {
    transformId: string
    getBundleId: () => string;
    multiplexingDataChannel: MultiplexingDataChannel;
    channel: IDataChannel;
    coder: Coder<any>;
    buffer: protobufjs.Writer;

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        const writePort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
        this.multiplexingDataChannel = context.getDataChannel(writePort.apiServiceDescriptor!.url);
        this.transformId = transformId;
        this.getBundleId = context.getBundleId;
        this.coder = context.pipelineContext.getCoder(writePort.coderId);
    }

    startBundle() {
        this.channel = this.multiplexingDataChannel.getSendChannel(this.transformId, this.getBundleId());
        this.buffer = new protobufjs.Writer();
    }

    process(wvalue: WindowedValue<any>) {
        this.coder.encode(wvalue, this.buffer, CoderContext.needsDelimiters);
        if (this.buffer.len > 1e6) {
            this.flush();
        }
    }

    finishBundle() {
        this.flush()
        this.channel.close();
    }

    flush() {
        this.channel.sendData(this.buffer.finish());
        this.buffer = new protobufjs.Writer();
    }
}

registerOperator("beam:runner:sink:v1", DataSinkOperator);


class FlattenOperator implements IOperator {
    receiver: Receiver;

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
    }

    startBundle() { };

    process(wvalue: WindowedValue<any>) {
        this.receiver.receive(wvalue);
    }

    finishBundle() { }
}

registerOperator("beam:transform:flatten:v1", FlattenOperator);


class ParDoOperator implements IOperator {
    receiver: Receiver;
    spec: runnerApi.ParDoPayload;
    doFn: base.DoFn;

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
        this.spec = runnerApi.ParDoPayload.fromBinary(transform.spec!.payload);
        if (this.spec.doFn?.urn != translations.SERIALIZED_JS_DOFN_INFO) {
            // throw new Error("Unknown DoFn type: " + this.spec);
            this.doFn = new class extends base.DoFn {
                *process(element: any) {
                    console.log("Unknown DoFn processing", element)
                    yield element;
                }
            }();
        } else {
            this.doFn = base.fakeDeserialize(this.spec.doFn.payload!);
        }
    }

    startBundle() {
        this.doFn.startBundle();
    };

    process(wvalue: WindowedValue<any>) {
        for (const element of this.doFn.process(wvalue.value)) {
            this.receiver.receive({
                value: element,
                windows: <Array<BoundedWindow>> <unknown> undefined,
                pane: <PaneInfo> <unknown> undefined,
                timestamp: <Date> <unknown> undefined
            });
        }
    }

    finishBundle() {
        this.doFn.finishBundle();
    }
}

registerOperator(base.ParDo.urn, ParDoOperator);


class PassThroughOperator implements IOperator {
    transformId: string;
    transformUrn: string;
    receivers: Receiver[];

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        this.transformId = transformId;
        this.transformUrn = transform.spec!.urn;
        this.receivers = Object.values(transform.outputs).map(context.getReceiver);
    }

    startBundle() {
    }

    process(wvalue) {
        console.log("forwarding", wvalue.value, "for", this.transformId, this.transformUrn);
        this.receivers.map((receiver: Receiver) => receiver.receive(wvalue));
    }

    finishBundle() {
    }
}




function onlyElement<Type>(arg: Type[]): Type {
    if (arg.length > 1) {
        Error("Expecting exactly one element.");
    }
    return arg[0];
}
