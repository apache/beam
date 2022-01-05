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

export function createOperator(
    descriptor: ProcessBundleDescriptor,
    transformId: string,
    getReceiver: (string) => Receiver,
    getDataChannel: (string) => MultiplexingDataChannel,
    getBundleId: () => string,
): IOperator {
    const transform = descriptor.transforms[transformId];

    switch (transform.spec?.urn) {

        case "beam:runner:source:v1":
            const readPort = RemoteGrpcPort.fromBinary(transform.spec.payload);
            return new DataSourceOperator(
                getDataChannel(readPort.apiServiceDescriptor!.url),
                transformId,
                getBundleId,
                getReceiver(onlyElement(Object.values(transform.outputs))));

        case "beam:runner:sink:v1":
            const writePort = RemoteGrpcPort.fromBinary(transform.spec.payload);
            return new DataSinkOperator(
                getDataChannel(writePort.apiServiceDescriptor!.url),
                transformId,
                getBundleId);

        case "beam:transform:flatten:v1":
            return new FlattenOperator(getReceiver(onlyElement(Object.values(transform.outputs))));

        default:
            console.log("Unknown transform type:", transform.spec?.urn)
            const receivers = Object.values(transform.outputs).map(getReceiver);
            const sendAll = function(value: WindowedValue) {
                receivers.map((receiver: Receiver) => receiver.receive(value));
            };
            return {
                startBundle: function() {
                    console.log("startBundle", transformId);
                },
                process: function(wvalue) {
                    console.log("forwarding", transformId, wvalue.value);
                    sendAll(wvalue);
                },
                finishBundle: function() {
                    console.log("finishBundle", transformId);
                },
            };
    }
}

class DataSourceOperator implements IOperator {
    transformId: string
    getBundleId: () => string;
    multiplexingDataChannel: MultiplexingDataChannel;
    receiver: Receiver;
    done: boolean;

    constructor(multiplexingDataChannel: MultiplexingDataChannel, transformId: string, getBundleId: () => string, receiver: Receiver) {
        this.multiplexingDataChannel = multiplexingDataChannel;
        this.transformId = transformId;
        this.getBundleId = getBundleId;
        this.receiver = receiver;
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

class DataSinkOperator implements IOperator {
    transformId: string
    getBundleId: () => string;
    multiplexingDataChannel: MultiplexingDataChannel;
    channel: IDataChannel;

    constructor(multiplexingDataChannel: MultiplexingDataChannel, transformId: string, getBundleId: () => string) {
        this.multiplexingDataChannel = multiplexingDataChannel;
        this.transformId = transformId;
        this.getBundleId = getBundleId;
    }

    startBundle() {
        this.channel = this.multiplexingDataChannel.getSendChannel(this.transformId, this.getBundleId());
    }

    process(wvalue: WindowedValue) {
        this.channel.sendData(wvalue.value);
    }

    finishBundle() {
        this.channel.close();
    }
}

class FlattenOperator implements IOperator {
    receiver: Receiver;
    constructor(receiver: Receiver) {
        this.receiver = receiver;
    }
    startBundle() { };
    process(wvalue: WindowedValue) {
        this.receiver.receive(wvalue);
    }
    finishBundle() { }
}


function onlyElement<Type>(arg: Type[]): Type {
    if (arg.length > 1) {
        Error("Expecting exactly one element.");
    }
    return arg[0];
}
