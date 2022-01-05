import { PTransform, PCollection } from "../proto/beam_runner_api";
import { ProcessBundleDescriptor, RemoteGrpcPort } from "../proto/beam_fn_api";
import { MultiplexingDataChannel, IDataChannel } from "./data"


export interface IOperator {
    startBundle: () => void;
    process: (WindowedValue) => void;
    finishBundle: () => void;
}

export class Receiver {
    operators: IOperator[]

    constructor(operators: IOperator[]) {
        this.operators = operators;
    }

    receive(wvalue: WindowedValue) {
        for (const operator of this.operators) {
            operator.process(wvalue);
        }
    }
}

export interface WindowedValue {
    value: any;
}

export interface OperatorContext {
    descriptor: ProcessBundleDescriptor;
    getReceiver: (string) => Receiver;
    getDataChannel: (string) => MultiplexingDataChannel;
    getBundleId: () => string;
}

export function createOperator(
    transformId: string,
    context: OperatorContext,
): IOperator {
    const transform = context.descriptor.transforms[transformId];
    let operatorClass = operatorsByUrn.get(transform.spec!.urn!);
    console.log("Creating:", transform.spec?.urn, operatorClass);
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
    done: boolean;

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        const readPort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
        this.multiplexingDataChannel = context.getDataChannel(readPort.apiServiceDescriptor!.url);
        this.transformId = transformId;
        this.getBundleId = context.getBundleId;
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
    }

    startBundle() {
        this.done = false;
        const this_ = this;
        this.multiplexingDataChannel.registerConsumer(
            this.getBundleId(),
            this.transformId,
            {
                sendData: function(data: Uint8Array) {
                    console.log("Got", data)
                    this_.receiver.receive({ value: data });
                },
                sendTimers: function(timerFamilyId: string, timers: Uint8Array) {
                    throw Error("Not expecting timers.");
                },
                close: function() { this_.done = true; },
            });
    }

    process(wvalue: WindowedValue) {
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

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        const writePort = RemoteGrpcPort.fromBinary(transform.spec!.payload);
        this.multiplexingDataChannel = context.getDataChannel(writePort.apiServiceDescriptor!.url);
        this.transformId = transformId;
        this.getBundleId = context.getBundleId;
    }

    startBundle() {
        this.channel = this.multiplexingDataChannel.getSendChannel(this.transformId, this.getBundleId());
    }

    process(wvalue: WindowedValue) {
        // TODO: Encode and buffer.
        this.channel.sendData(wvalue.value);
    }

    finishBundle() {
        this.channel.close();
    }
}

registerOperator("beam:runner:sink:v1", DataSinkOperator);


class FlattenOperator implements IOperator {
    receiver: Receiver;

    constructor(transformId: string, transform: PTransform, context: OperatorContext) {
        this.receiver = context.getReceiver(onlyElement(Object.values(transform.outputs)));
    }

    startBundle() { };

    process(wvalue: WindowedValue) {
        this.receiver.receive(wvalue);
    }

    finishBundle() { }
}

registerOperator("beam:transform:flatten:v1", FlattenOperator);


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
