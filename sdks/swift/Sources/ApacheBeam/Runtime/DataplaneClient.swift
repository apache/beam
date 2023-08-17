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

import Foundation
import GRPC
import Logging

/// Client for handling the multiplexing and demultiplexing of Dataplane messages
actor DataplaneClient {

    public struct Pair : Hashable {
        let id: String
        let transform: String
    }
    
    public enum Message {
        case data(Data)
        case timer(String,Data)
        case last(String,String)
        case flush
    }
    struct Multiplex {
        let id: String
        let transform: String
        let message: Message
    }
    typealias InternalStream = AsyncStream<Multiplex>
    typealias Stream = AsyncStream<Message>
    
    public struct MultiplexContinuation {
        let id: String
        let transform: String
        let base: InternalStream.Continuation
        
        @discardableResult
        func yield(_ value: Message) -> InternalStream.Continuation.YieldResult {
            base.yield(Multiplex(id: id, transform: transform, message: value))
        }
        
        func finish() {
            // Does nothing
        }
        

        
    }
    

    private let id: String
    private let log: Logging.Logger
    private let multiplex: (InternalStream,InternalStream.Continuation)
    private var streams: [Pair:(Stream,Stream.Continuation,MultiplexContinuation)] = [:]
    private let flush: Int

    public init(id:String,endpoint:ApiServiceDescriptor,flush:Int=100) throws {
        self.id  = id
        self.log = Logging.Logger(label: "Dataplane(\(id),\(endpoint.url))")
        self.multiplex = AsyncStream.makeStream(of:Multiplex.self)
        self.flush = flush
        let client = Org_Apache_Beam_Model_FnExecution_V1_BeamFnDataAsyncClient(channel:try GRPCChannelPool.with(endpoint: endpoint, eventLoopGroup: PlatformSupport.makeEventLoopGroup(loopCount: 1)),defaultCallOptions: CallOptions(customMetadata:["worker_id":id]))
        let stream = client.makeDataCall()

        // Mux task
        Task {
            log.info("Initiating data plane multiplexing.")
            
            let input = multiplex.0

            var elements = Org_Apache_Beam_Model_FnExecution_V1_Elements()
            for try await element in input {
                var shouldFlush: Bool = false
                switch element.message {
                    
                case .data(let payload):
                    elements.data.append(.with {
                        $0.instructionID = element.id
                        $0.transformID = element.transform
                        $0.data = payload
                    })
                case let .timer(family, payload):
                    elements.timers.append(.with {
                        $0.instructionID = element.id
                        $0.transformID = element.transform
                        $0.timerFamilyID = family
                        $0.timers = payload
                    })
                case let .last(id, transform):
                    elements.data.append(.with {
                        $0.instructionID = id
                        $0.transformID = transform
                        $0.isLast = true
                    })
                    shouldFlush = true
                case .flush:
                    shouldFlush = true
                }
                if shouldFlush || elements.data.count + elements.timers.count >= flush {
                    do {
                        try await stream.requestStream.send(elements)
                    } catch {
                        log.error("Unable to multiplex elements onto data plane: \(error)")
                    }
                    elements = Org_Apache_Beam_Model_FnExecution_V1_Elements()
                    shouldFlush = false
                }
            }
        }

        // Demux task
        Task {
            log.info("Initiating data plane demultiplexing.")
            do {
                
                for try await elements in stream.responseStream {
                    var last: [Pair:Message] = [:] // Split out last calls so they are always at the end
                    var messages: [Pair:[Message]] = [:]
                    
                    for element in elements.data {
                        let key = Pair(id: element.instructionID, transform: element.transformID)
                        //Drop zero-length elements
                        if element.data.count > 0 {
                            messages[key,default:[]].append(.data(element.data))
                        }
                        if element.isLast {
                            last[key] = .last(element.instructionID, element.transformID)
                        }
                    }
                    
                    for element in elements.timers {
                        let key = Pair(id:element.instructionID,transform:element.transformID)
                        if element.timers.count > 0 {
                            messages[key,default:[]].append(.timer(element.timerFamilyID, element.timers))
                        }
                        if element.isLast {
                            last[key] = .last(element.instructionID, element.transformID)
                        }
                    }
                    
                    // Send the messages to registered sources
                    for (key,value) in messages {
                        let output = await self.makeStream(key:key).1
                        for v in value {
                            output.yield(v)
                        }
                    }
                    // Send any last messages
                    for (key,value) in last {
                        let output = await self.makeStream(key: key).1
                        output.yield(value)
                    }
                }
            } catch {
                log.error("Lost data plane connection.")
            }
            
        }
    }
    
    /// Returns or creates a stream for a particular instruction,transform pair with both the multiplex and demultiplex continuations
    internal func makeStream(key:Pair) -> (Stream,Stream.Continuation,MultiplexContinuation) {
        if let existing = streams[key] {
            return existing
        }
        let baseStream = AsyncStream.makeStream(of:Message.self)
        let stream = (baseStream.0,baseStream.1,
                      MultiplexContinuation(id: key.id, transform: key.transform, base: multiplex.1))
        streams[key] = stream
        return stream
    }
    
    /// Returns or creates a stream for a particular instruction,transform pair with the multiplex continuation but not the demultiplex
    /// which mirrors the response from AsyncStream.makeStream
    public func makeStream(instruction:String,transform:String) -> (Stream,MultiplexContinuation) {
        let key = Pair(id:instruction,transform: transform)
        let (stream,_,continuation) = makeStream(key:key)
        return (stream,continuation)
    }
    
    func finalizeStream(instruction:String,transform:String) {
        //TODO: Implement finalization.
    }

    
    
    private static var dataplanes: [ApiServiceDescriptor:DataplaneClient] = [:]
    public static func client(for endpoint:ApiServiceDescriptor,worker id: String) throws -> DataplaneClient {
        if let client = dataplanes[endpoint] {
            return client
        } else {
            let client = try DataplaneClient(id: id, endpoint: endpoint)
            dataplanes[endpoint] = client
            return client
        }
    }
}
