import * as grpc from '@grpc/grpc-js';

import { PTransform, PCollection } from "../proto/beam_runner_api";

import { RemoteGrpcPort } from "../proto/beam_fn_api";
import { InstructionRequest, InstructionResponse } from "../proto/beam_fn_api";
import { ProcessBundleDescriptor, ProcessBundleResponse } from "../proto/beam_fn_api";
import { BeamFnControlClient, IBeamFnControlClient } from "../proto/beam_fn_api.grpc-client";

import { StartWorkerRequest, StartWorkerResponse, StopWorkerRequest, StopWorkerResponse } from "../proto/beam_fn_api";
import { beamFnExternalWorkerPoolDefinition, IBeamFnExternalWorkerPool } from "../proto/beam_fn_api.grpc-server";

import { MultiplexingDataChannel, IDataChannel } from "./data"


console.log("Starting the worker.");

const host = '0.0.0.0:5555';

const workers = new Map<string, Worker>();

class Worker {
    id: string;
    endpoints: StartWorkerRequest;
    controlClient: BeamFnControlClient;
    controlChannel: grpc.ClientDuplexStream<InstructionResponse, InstructionRequest>;

    processBundleDescriptors: Map<string, ProcessBundleDescriptor> = new Map();
    bundleProcessors: Map<string, BundleProcessor[]> = new Map();
    dataChannels: Map<string, MultiplexingDataChannel> = new Map();

    constructor(id: string, endpoints: StartWorkerRequest) {
        this.id = id;
        this.endpoints = endpoints;

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
        this.controlChannel.on('data', async request => {
            console.log(request);
            if (request.request.oneofKind == 'processBundle') {
                await this.process(request);
            } else {
                console.log("Unknown instruction type: ", request)
            }
        })
    }

    respond(response: InstructionResponse) {
        this.controlChannel.write(response)
    }

    async process(request) {
        const descriptorId = request.request.processBundle.processBundleDescriptorId;
        if (!this.processBundleDescriptors.has(descriptorId)) {
            console.log("Looking up", descriptorId)
            const call = this.controlClient.getProcessBundleDescriptor({
                processBundleDescriptorId: descriptorId,
            }, (err, value: ProcessBundleDescriptor) => {
                if (err) {
                    this.respond({
                        instructionId: request.instructionId,
                        error: "" + err,
                        response: {
                            oneofKind: undefined
                        }
                    })
                } else {
                    this.processBundleDescriptors.set(descriptorId, value);
                    this.process(request);
                }
            });
            return
        }

        const processor = this.aquireBundleProcessor(descriptorId);
        await processor.process(request.instructionId);
        this.respond({
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
            }
        })
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
            return new BundleProcessor(this.processBundleDescriptors.get(descriptorId)!, this.getDataChannel.bind(this))
        }
    }

    returnBundleProcessor(processor: BundleProcessor) {
        this.bundleProcessors.get(processor.descriptor.id)?.push(processor);
    }

    getDataChannel(endpoint: string): MultiplexingDataChannel {
        if (!this.dataChannels.has(endpoint)) {
            this.dataChannels.set(endpoint, new MultiplexingDataChannel(endpoint, this.id));
        }
        return this.dataChannels.get(endpoint)!
    }


    stop() {
        this.controlChannel.end();
    }
}

class BundleProcessor {
    descriptor: ProcessBundleDescriptor;

    topologicallyOrderedOperators: IOperator[] = [];
    operators: Map<string, IOperator> = new Map();
    receivers: Map<string, Receiver> = new Map();

    getDataChannel: (string) => MultiplexingDataChannel;
    currentBundleId?: string;

    constructor(descriptor: ProcessBundleDescriptor, getDataChannel: (string) => MultiplexingDataChannel) {
        this.descriptor = descriptor;
        this.getDataChannel = getDataChannel;

        // TODO: Consider defering this possibly expensive deserialization lazily to the worker thread.
        const this_ = this;
        const creationOrderedOperators: IOperator[] = [];

        const consumers = new Map<string, string[]>();
        Object.entries(descriptor.transforms).forEach(([transformId, transform]) => {
            Object.values(transform.inputs).forEach((pcollectionId: string) => {
                // TODO: is there a javascript defaultdict?
                if (!consumers.has(pcollectionId)) {
                    consumers.set(pcollectionId, []);
                }
                consumers.get(pcollectionId)!.push(transformId);
            })
        });

        function getReceiver(pcollectionId: string): Receiver {
            if (!this_.receivers.has(pcollectionId)) {
                this_.receivers.set(
                    pcollectionId,
                    new Receiver((consumers.get(pcollectionId) || []).map(getOperator)));
            }
            return this_.receivers.get(pcollectionId)!;
        }

        function getOperator(transformId: string): IOperator {
            if (!this_.operators.has(transformId)) {
                this_.operators.set(
                    transformId,
                    createOperator(descriptor, transformId, getReceiver, this_.getDataChannel, this_.getBundleId.bind(this_)));
                creationOrderedOperators.push(this_.operators.get(transformId)!);
            }
            return this_.operators.get(transformId)!;
        }

        Object.keys(descriptor.transforms).forEach(getOperator);
        this.topologicallyOrderedOperators = creationOrderedOperators.reverse();
    }

    getBundleId() {
        return this.currentBundleId!;
    }

    // Put this on a worker thread...
    async process(instructionId: string) {
        console.log("Processing ", this.descriptor.id, "for", instructionId);
        this.currentBundleId = instructionId;
        this.topologicallyOrderedOperators.slice().reverse().forEach((o) => o.startBundle());

        console.log("Waiting...")
        await new Promise((resolve) => {
            setTimeout(resolve, 2000);
        });
        console.log("Done waiting.")

        this.topologicallyOrderedOperators.forEach((o) => o.finishBundle());
        this.currentBundleId = undefined;
    }
}

interface IOperator {
    startBundle: () => void;
    process: (WindowedValue) => void;
    finishBundle: () => void;
}

class Receiver {
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

interface WindowedValue {
    value: any;
}

function createOperator(
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



/////////////////////////////////////////////////////////////////////////////

const workerService: IBeamFnExternalWorkerPool = {
    startWorker(call: grpc.ServerUnaryCall<StartWorkerRequest, StartWorkerResponse>, callback: grpc.sendUnaryData<StartWorkerResponse>): void {

        call.on('error', args => {
            console.log("unary() got error:", args)
        })

        console.log(call.request);
        workers.set(call.request.workerId, new Worker(call.request.workerId, call.request));
        callback(
            null,
            {
                error: "",
            },
        );

    },


    stopWorker(call: grpc.ServerUnaryCall<StopWorkerRequest, StopWorkerResponse>, callback: grpc.sendUnaryData<StopWorkerResponse>): void {
        console.log(call.request);

        workers.get(call.request.workerId)?.stop()
        workers.delete(call.request.workerId)

        callback(
            null,
            {
                error: "",
            },
        );
    },

}


function getServer(): grpc.Server {
    const server = new grpc.Server();
    server.addService(beamFnExternalWorkerPoolDefinition, workerService);
    return server;
}


if (require.main === module) {
    const server = getServer();
    server.bindAsync(
        host,
        grpc.ServerCredentials.createInsecure(),
        (err: Error | null, port: number) => {
            if (err) {
                console.error(`Server error: ${err.message}`);
            } else {
                console.log(`Server bound on port: ${port}`);
                server.start();
            }
        }
    );
}
