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
import NIOCore
import Logging

actor Worker {
    private let id: String
    private let collections: [String:AnyPCollection]
    private let fns: [String:SerializableFn]
    private let control: ApiServiceDescriptor
    private let remoteLog: ApiServiceDescriptor
    
    private let log: Logging.Logger
    
    public init(id:String,control:ApiServiceDescriptor,log:ApiServiceDescriptor,collections:[String:AnyPCollection],functions: [String:SerializableFn]) {
        self.id = id
        self.collections = collections
        self.fns = functions
        self.control = control
        self.remoteLog = log
        
        self.log = Logging.Logger(label: "Worker(\(id))")
    }
    
    
    
    public func start() throws {
        let group = PlatformSupport.makeEventLoopGroup(loopCount: 1)
        let client = Org_Apache_Beam_Model_FnExecution_V1_BeamFnControlAsyncClient(channel: try GRPCChannelPool.with(endpoint: control, eventLoopGroup: group))
        let (responses,responder) = AsyncStream.makeStream(of:Org_Apache_Beam_Model_FnExecution_V1_InstructionResponse.self)
        let options = CallOptions(customMetadata: ["worker_id":id])
        let control = client.makeControlCall(callOptions: options)
        
        
        //Start the response task. This will continue until a yield call is sent from responder
        Task {
            for await r in responses {
                try await control.requestStream.send(r)
            }
        }
        //Start the actual work task
        Task {
            log.info("Waiting for control plane instructions.")
            var processors: [String:BundleProcessor] = [:]
            
            func processor(for bundle: String) async throws -> BundleProcessor {
                if let processor = processors[bundle] {
                    return processor
                }
                let descriptor = try await client.getProcessBundleDescriptor(.with { $0.processBundleDescriptorID = bundle })
                let processor = try BundleProcessor(id: id, descriptor: descriptor, collections: collections, fns: fns)
                processors[bundle] = processor
                return processor
            }
            

            //This looks a little bit reversed from the usual because response don't need an initiating call
            for try await instruction in control.responseStream {
                switch instruction.request {
                case .processBundle(let pbr):
                    try await processor(for:pbr.processBundleDescriptorID)
                        .process(instruction: instruction.instructionID,responder:responder)
                    break
                default:
                    log.warning("Ignoring instruction \(instruction.instructionID). Not yet implemented.")
                    log.warning("\(instruction)")
                    responder.yield(.with {
                        $0.instructionID = instruction.instructionID
                    })
                }
            }
            log.info("Control plane connection has closed.")
        }
        
    }
}
