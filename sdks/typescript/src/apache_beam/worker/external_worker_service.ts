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

import * as grpc from "@grpc/grpc-js";

import {
  StartWorkerRequest,
  StartWorkerResponse,
  StopWorkerRequest,
  StopWorkerResponse,
} from "../proto/beam_fn_api";
import {
  beamFnExternalWorkerPoolDefinition,
  IBeamFnExternalWorkerPool,
} from "../proto/beam_fn_api.grpc-server";

import { Worker, WorkerEndpoints } from "./worker";

export class ExternalWorkerPool {
  address: string;
  server: grpc.Server;
  workers: Map<string, Worker> = new Map();

  // TODO: (Cleanup) Choose a free port.
  constructor(address: string = "localhost:5555") {
    this.address = address;
  }

  start() {
    console.log("Starting the workers at ", this.address);
    const this_ = this;

    this.server = new grpc.Server();

    const workerService: IBeamFnExternalWorkerPool = {
      startWorker(
        call: grpc.ServerUnaryCall<StartWorkerRequest, StartWorkerResponse>,
        callback: grpc.sendUnaryData<StartWorkerResponse>
      ): void {
        call.on("error", (args) => {
          console.log("unary() got error:", args);
        });

        console.log(call.request);
        this_.workers.set(
          call.request.workerId,
          new Worker(
            call.request.workerId,
            {
              controlUrl: call.request?.controlEndpoint?.url!,
            },
            {}
          )
        );
        callback(null, {
          error: "",
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
          error: "",
        });
      },
    };

    this.server.bindAsync(
      this.address,
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

const default_host = "localhost:5555";

if (require.main === module) {
  new ExternalWorkerPool(default_host).start();
}
