/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 *  License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an  AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

@propertyWrapper
public struct PValue<Value> : DynamicProperty {

    enum UpdateStrategy {
        case pipeline
        case first(Any.Type)
        case named(String,Any.Type)
    }
    
    enum Storage {
        case uninitialized
        case constant(Value)
    }
    
    private let updater:UpdateStrategy
    private var storage:Storage = .uninitialized
    
    public var wrappedValue: Value {
        get {
            switch storage {
            case .constant(let v):
                return v
            case .uninitialized:
                fatalError("Storage component of a PValue is uninitialized.")
            }
        }
        nonmutating set {  
        }
    }
    
    public var projectedValue: PValue<Value> { self }
    
    public init() where Value == PipelineRoot {
        updater = .pipeline
    }
    
    public init<Of>() where Value == PCollection<Of> {
        updater = .first(Of.self)
    }
    
    public init<Of>(named: String) where Value == PCollection<Of> {
        updater = .named(named,Of.self)
    }

    public mutating func update(from root: PipelineRoot) throws where Value == PipelineRoot {
        if case .pipeline = updater {
            storage = .constant(root)
        }
    }
    
    public mutating func update<Of>(from collection: PCollection<Of>)  throws where Value == PipelineRoot {
        if case .pipeline = updater {
            if let root = collection.roots.first {
                storage = .constant(root)
            } else {
                throw ApacheBeamError.runtimeError("Unable to retrieve pipeline from PCollection")
            }
        }
    }
    
    public mutating func update<Of>(from collection: PCollection<Of>) throws where Value == PCollection<Of> {
        if case let .first(type) = updater {
            if type == Of.self {
                storage = .constant(collection)
            }
        }
    }
    
}
