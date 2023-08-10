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
import GRPC
import Logging

actor WorkerProvider : Org_Apache_Beam_Model_FnExecution_V1_BeamFnExternalWorkerPoolAsyncProvider {

    private let log = Logger(label:"WorkerProvider")
    private var workers: [String:Worker] = [:]

    private let functions: [String:SerializableFn]
    
    init(_ functions: [String:SerializableFn]) {
        self.functions = functions
    }
    
    
    
    
    func startWorker(request: Org_Apache_Beam_Model_FnExecution_V1_StartWorkerRequest, context: GRPC.GRPCAsyncServerCallContext) async throws -> Org_Apache_Beam_Model_FnExecution_V1_StartWorkerResponse {
        log.info("Got request to start worker \(request.workerID)")
        do {
            if let worker = workers[request.workerID] {
                log.info("Worker \(request.workerID) is already running.")
                return .with { _ in }
            } else {
                workers[request.workerID] = try Worker(id: request.workerID)
            }
            return .with { _ in
                
            }
        } catch {
            log.error("Unable to start worker \(request.workerID): \(error)")
            return .with {
                $0.error = "\(error)"
            }
        }
    }
    
    func stopWorker(request: Org_Apache_Beam_Model_FnExecution_V1_StopWorkerRequest, context: GRPC.GRPCAsyncServerCallContext) async throws -> Org_Apache_Beam_Model_FnExecution_V1_StopWorkerResponse {
        return .with { _ in }
    }
    
    
}
