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

/// A SerializableFn that holds a reference to a function that takes a single input and produces a variable number of outputs
public final class ClosureFn : SerializableFn {
    let processClosure: (SerializableFnBundleContext,[AnyPCollectionStream],[AnyPCollectionStream]) async throws -> Void

    //TODO: Replace this with a parameter pack version once I figure out how to do that

    public init<Of>(_ fn: @Sendable @escaping (PCollection<Of>.Stream) async throws -> Void) {
        self.processClosure = { context,inputs,outputs in
            try await fn(inputs[0].stream())
        }
    }
    
    public init<Of,O0>(_ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream) async throws -> Void) {
        self.processClosure = { context,inputs,outputs in
            try await fn(inputs[0].stream(),outputs[0].stream())
        }
    }

    public init<Of,O0,O1>(_ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream) async throws -> Void) {
        self.processClosure = { context,inputs,outputs in
            try await fn(inputs[0].stream(),outputs[0].stream(),outputs[1].stream())
        }
    }
    
    public init<Of,O0,O1,O2>(_ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream,PCollection<O2>.Stream) async throws -> Void) {
        self.processClosure = { context,inputs,outputs in
            try await fn(inputs[0].stream(),outputs[0].stream(),outputs[1].stream(),outputs[2].stream())
        }
    }

    public init<Of,O0,O1,O2,O3>(_ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream,PCollection<O2>.Stream,PCollection<O3>.Stream) async throws -> Void) {
        self.processClosure = { context,inputs,outputs in
            try await fn(inputs[0].stream(),outputs[0].stream(),outputs[1].stream(),outputs[2].stream(),outputs[3].stream())
        }
    }
    
    public func process(context: SerializableFnBundleContext, inputs: [AnyPCollectionStream], outputs: [AnyPCollectionStream]) async throws -> (String, String) {
        try await processClosure(context,inputs,outputs)
        outputs.finish()
        return (context.instruction,context.transform)
    }
    
}
