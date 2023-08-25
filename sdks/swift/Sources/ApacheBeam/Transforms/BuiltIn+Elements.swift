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

public protocol PInput<Of> {
    associatedtype Of
    
    var value: Of { get }
    var timestamp: Date { get }
    var window: Window { get }
}

public protocol POutput<Of> {
    associatedtype Of
    
    func emit(_ value: Of,timestamp: Date,window: Window)
    func emit(_ value: Of)
    func emit(_ value: Of,timestamp: Date)
    func emit(_ value: Of,window: Window)
}


struct PardoInput<Of> : PInput {
    let value: Of
    let timestamp: Date
    let window: Window
    public init(_ value:(Of,Date,Window)) {
        self.value = value.0
        self.timestamp = value.1
        self.window = value.2
    }
}

struct PardoOutput<Of> : POutput {
    
    
    let stream: PCollectionStream<Of>
    let timestamp: Date
    let window: Window

    func emit(_ value: Of, timestamp: Date, window: Window) {
        stream.emit(value,timestamp:timestamp,window:window)
    }
    func emit(_ value: Of, timestamp: Date) {
        stream.emit(value,timestamp:timestamp,window:window)

    }    
    func emit(_ value: Of, window: Window) {
        stream.emit(value,timestamp:timestamp,window:window)

    }
    func emit(_ value: Of) {
        stream.emit(value,timestamp:timestamp,window:window)
    }

    
}

public extension PCollection {
    
    // No Output
    func pardo(name:String,_ fn: @Sendable @escaping (any PInput<Of>) async throws -> Void) {
        pstream(name:name) { input in
            for try await element in input {
                try await fn(PardoInput(element))
            }
        }
    }
    
    // One Output
    func pardo<O0>(name:String,_ fn: @Sendable @escaping (any PInput<Of>,any POutput<O0>) async throws -> Void) -> PCollection<O0> {
        pstream(name:name) { input,output in
            for try await element in input {
                try await fn(PardoInput(element),PardoOutput(stream:output,timestamp:element.1,window:element.2))
            }
        }
    }
    
    //Two Outputs
    func pardo<O0,O1>(name:String,_ fn: @Sendable @escaping (any PInput<Of>,any POutput<O0>,any POutput<O1>) async throws -> Void) -> (PCollection<O0>,PCollection<O1>) {
        pstream(name:name) { input,o0,o1 in
            for try await element in input {
                try await fn(PardoInput(element),
                             PardoOutput(stream:o0,timestamp:element.1,window:element.2),
                             PardoOutput(stream:o1,timestamp:element.1,window:element.2)
                             
                )
            }
        }
    }

    
}
