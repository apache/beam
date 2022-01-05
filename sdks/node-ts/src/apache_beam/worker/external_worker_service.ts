import * as grpc from '@grpc/grpc-js';

import { StartWorkerRequest, StartWorkerResponse, StopWorkerRequest, StopWorkerResponse } from "../proto/beam_fn_api";
import { beamFnExternalWorkerPoolDefinition, IBeamFnExternalWorkerPool } from "../proto/beam_fn_api.grpc-server";

import { Worker } from "./worker";

console.log("Starting the worker.");

const host = '0.0.0.0:5555';

const workers = new Map<string, Worker>();


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
