import * as grpc from '@grpc/grpc-js';

import {
  StartWorkerRequest,
  StartWorkerResponse,
  StopWorkerRequest,
  StopWorkerResponse,
} from '../proto/beam_fn_api';
import {
  beamFnExternalWorkerPoolDefinition,
  IBeamFnExternalWorkerPool,
} from '../proto/beam_fn_api.grpc-server';

import {Worker} from './worker';

export class ExternalWorkerPool {
  host: string;
  server: grpc.Server;
  workers: Map<string, Worker> = new Map();

  constructor(host: string) {
    this.host = host;
  }

  start() {
    console.log('Starting the workers at ', this.host);
    const this_ = this;

    this.server = new grpc.Server();

    const workerService: IBeamFnExternalWorkerPool = {
      startWorker(
        call: grpc.ServerUnaryCall<StartWorkerRequest, StartWorkerResponse>,
        callback: grpc.sendUnaryData<StartWorkerResponse>
      ): void {
        call.on('error', args => {
          console.log('unary() got error:', args);
        });

        console.log(call.request);
        this_.workers.set(
          call.request.workerId,
          new Worker(call.request.workerId, call.request)
        );
        callback(null, {
          error: '',
        });
      },

      stopWorker(
        call: grpc.ServerUnaryCall<StopWorkerRequest, StopWorkerResponse>,
        callback: grpc.sendUnaryData<StopWorkerResponse>
      ): void {
        console.log(call.request);

        this_.workers.get(call.request.workerId)?.stop();
        this_.workers.delete(call.request.workerId);

        callback(null, {
          error: '',
        });
      },
    };

    this.server.bindAsync(
      this.host,
      grpc.ServerCredentials.createInsecure(),
      (err: Error | null, port: number) => {
        if (err) {
          console.error(`Server error: ${err.message}`);
        } else {
          console.log(`Server bound on port: ${port}`);
          this_.server.start();
        }
      }
    );

    this.server.addService(beamFnExternalWorkerPoolDefinition, workerService);
  }

  stop() {
    this.server.forceShutdown();
  }
}

const default_host = '0.0.0.0:5555';

if (require.main === module) {
  new ExternalWorkerPool(default_host).start();
}
