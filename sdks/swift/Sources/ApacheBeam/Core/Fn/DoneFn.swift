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

public final class DoneFn<Of> : SerializableFn {

    private let fn: (PCollectionStream<Of>) async throws -> Void
    public init(_ fn: @Sendable @escaping (PCollectionStream<Of>) async throws -> Void) {
        self.fn = fn
    }
    
    
    public func process(context: SerializableFnBundleContext, inputs: [AnyPCollectionStream], outputs: [AnyPCollectionStream]) async throws -> (String, String) {
        try await fn(inputs[0].stream())
        for output in outputs {
            output.finish()
        }
        return (context.instruction,context.transform)
    }
}

public final class ParameterizedDoneFn<Of,Param:Codable> : SerializableFn {
    
    
    private let param: Param
    private let fn: (Param,PCollectionStream<Of>) async throws -> Void
    
    public init(_ param: Param,_ fn: @Sendable @escaping (Param,PCollectionStream<Of>) async throws -> Void){
        self.param = param
        self.fn = fn
    }
    
    public var payload: Data {
        get throws {
            try JSONEncoder().encode(param)
        }
    }
    
    public func process(context: SerializableFnBundleContext, inputs: [AnyPCollectionStream], outputs: [AnyPCollectionStream]) async throws -> (String, String) {
        try await fn(try JSONDecoder().decode(Param.self, from: context.payload),inputs[0].stream())
        for output in outputs {
            output.finish()
        }
        return (context.instruction,context.transform)
    }

}
