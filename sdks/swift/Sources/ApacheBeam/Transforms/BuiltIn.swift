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

public extension PCollection where Of == Never {
    /// Impulse the most basic transform. It can only be attached to PCollections of type Never,
    /// which is the root transform used by Pipelines.
    func impulse() -> PCollection<Data> {
        let output = PCollection<Data>()
        self.apply(.impulse(AnyPCollection(self),AnyPCollection(output)))
        return output
    }
}

/// ParDo is the core user operator that pretty much everything else gets built on. We provide two versions here
public extension PCollection {
    // TODO: Replace with parameter pack version once https://github.com/apple/swift/issues/67192 is resolved
    
    // No Output
    func pstream<F:SerializableFn>(name:String,_ fn: F) {
        self.apply(.pardo(AnyPCollection(self),name, fn, []))
    }
    func pstream(name:String,_ fn: @Sendable @escaping (PCollection<Of>.Stream) async throws -> Void) {
        self.apply(.pardo(AnyPCollection(self),name, ClosureFn(fn),[]))
    }
    func pstream<Param:Codable>(name:String,_ param:Param,_ fn: @Sendable @escaping (Param,PCollection<Of>.Stream) async throws -> Void) {
        self.apply(.pardo(AnyPCollection(self),name, ParameterizedClosureFn(param,fn), []))
    }
    
    // No Output Generated Names
    func pstream<F:SerializableFn>(_file:String=#fileID,_line:Int=#line,_ fn: F) {
        pstream(name:"\(_file):\(_line)",fn)
    }
    func pstream(_file:String=#fileID,_line:Int=#line,_ fn: @Sendable @escaping (PCollection<Of>.Stream) async throws -> Void) {
        pstream(name:"\(_file):\(_line)",fn)
    }
    func pstream<Param:Codable>(_file:String=#fileID,_line:Int=#line,_ param:Param,_ fn: @Sendable @escaping (Param,PCollection<Of>.Stream) async throws -> Void) {
        pstream(name:"\(_file):\(_line)",param,fn)
    }


    // Single Output
    func pstream<F:SerializableFn,O0>(name:String,_ fn: F,
                                    _ o0:PCollection<O0>) {
        self.apply(.pardo(AnyPCollection(self),name, fn, [AnyPCollection(o0)]))
    }
    func pstream<O0>(name:String,_ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream) async throws -> Void) -> (PCollection<O0>) {
        let output = PCollection<O0>()
        self.apply(.pardo(AnyPCollection(self),name,ClosureFn(fn),[AnyPCollection(output)]))
        return output
    }
    func pstream<Param:Codable,O0>(name:String,_ param: Param,_ fn: @Sendable @escaping (Param,PCollection<Of>.Stream,PCollection<O0>.Stream) async throws -> Void) -> (PCollection<O0>) {
        let output = PCollection<O0>()
        self.apply(.pardo(AnyPCollection(self),name,ParameterizedClosureFn(param,fn),[AnyPCollection(output)]))
        return output
    }
    
    // Single Output Generated Names
    func pstream<F:SerializableFn,O0>(_file:String=#fileID,_line:Int=#line,fn: F,
                                    _ o0:PCollection<O0>) {
        pstream(name:"\(_file):\(_line)",fn,o0)
    }
    func pstream<O0>(_file:String=#fileID,_line:Int=#line,_ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream) async throws -> Void) -> (PCollection<O0>) {
        pstream(name:"\(_file):\(_line)",fn)
    }
    func pstream<Param:Codable,O0>(_file:String=#fileID,_line:Int=#line,_ param: Param,_ fn: @Sendable @escaping (Param,PCollection<Of>.Stream,PCollection<O0>.Stream) async throws -> Void) -> (PCollection<O0>) {
        pstream(name:"\(_file):\(_line)",param,fn)
    }

    
    
    // Two Outputs
    func pstream<F:SerializableFn,O0,O1>(name:String,_ fn: F,
                                    _ o0:PCollection<O0>,_ o1:PCollection<O1>) {
        self.apply(.pardo(AnyPCollection(self),name, fn, [AnyPCollection(o0),AnyPCollection(o1)]))
    }
    func pstream<O0,O1>(name:String,
                      _ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>) {
        let output = (PCollection<O0>(),PCollection<O1>())
        let parent = self.apply(.pardo(AnyPCollection(self),name,ClosureFn(fn),[AnyPCollection(output.0),AnyPCollection(output.1)]))
        output.0.parent(parent)
        output.1.parent(parent)
        return output
    }
    func pstream<Param:Codable,O0,O1>(name:String,_ param: Param,
                                    _ fn: @Sendable @escaping (Param,PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>) {
        let output = (PCollection<O0>(),PCollection<O1>())
        let parent = self.apply(.pardo(AnyPCollection(self),name,ParameterizedClosureFn(param,fn),[AnyPCollection(output.0),AnyPCollection(output.1)]))
        output.0.parent(parent)
        output.1.parent(parent)
        return output
    }
    
    // Two Outputs Generated Names
    func pstream<F:SerializableFn,O0,O1>(_file:String=#fileID,_line:Int=#line,_ fn: F,
                                    _ o0:PCollection<O0>,_ o1:PCollection<O1>) {
        pstream(name:"\(_file):\(_line)",fn,o0,o1)
    }
    func pstream<O0,O1>(_file:String=#fileID,_line:Int=#line,
                      _ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>) {
        pstream(name:"\(_file):\(_line)",fn)
    }
    func pstream<Param:Codable,O0,O1>(_file:String=#fileID,_line:Int=#line,_ param: Param,
                                    _ fn: @Sendable @escaping (Param,PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>) {
        pstream(name:"\(_file):\(_line)",param,fn)
    }

    

    // Three Outputs
    func pstream<F:SerializableFn,O0,O1,O2>(name:String,_ fn: F,
                                          _ o0:PCollection<O0>,_ o1:PCollection<O1>,_ o2:PCollection<O2>) {
        self.apply(.pardo(AnyPCollection(self),name, fn, [AnyPCollection(o0),AnyPCollection(o1),AnyPCollection(o2)]))
    }
    func pstream<O0,O1,O2>(name:String,
                         _ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream,PCollection<O2>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>,PCollection<O2>) {
        let output = (PCollection<O0>(),PCollection<O1>(),PCollection<O2>())
        let parent = self.apply(.pardo(AnyPCollection(self),name,ClosureFn(fn),[AnyPCollection(output.0),AnyPCollection(output.1),AnyPCollection(output.2)]))
        output.0.parent(parent)
        output.1.parent(parent)
        output.2.parent(parent)
        return output
    }
    func pstream<Param:Codable,O0,O1,O2>(name:String,_ param: Param,
                                       _ fn: @Sendable @escaping (Param,PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream,PCollection<O2>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>,PCollection<O2>) {
        let output = (PCollection<O0>(),PCollection<O1>(),PCollection<O2>())
        let parent = self.apply(.pardo(AnyPCollection(self),name,ParameterizedClosureFn(param,fn),[AnyPCollection(output.0),AnyPCollection(output.1),AnyPCollection(output.2)]))
        output.0.parent(parent)
        output.1.parent(parent)
        output.2.parent(parent)
        return output
    }
    
    // Three Outputs Generated Names
    func pstream<F:SerializableFn,O0,O1,O2>(_file:String=#fileID,_line:Int=#line,_ fn: F,
                                          _ o0:PCollection<O0>,_ o1:PCollection<O1>,_ o2:PCollection<O2>) {
        pstream(name:"\(_file):\(_line)",fn,o0,o1,o2)
    }
    func pstream<O0,O1,O2>(_file:String=#fileID,_line:Int=#line,
                         _ fn: @Sendable @escaping (PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream,PCollection<O2>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>,PCollection<O2>) {
        pstream(name:"\(_file):\(_line)",fn)
    }
    func pstream<Param:Codable,O0,O1,O2>(_file:String=#fileID,_line:Int=#line,_ param: Param,
                                       _ fn: @Sendable @escaping (Param,PCollection<Of>.Stream,PCollection<O0>.Stream,PCollection<O1>.Stream,PCollection<O2>.Stream) async throws -> Void) -> (PCollection<O0>,PCollection<O1>,PCollection<O2>) {
        pstream(name:"\(_file):\(_line)",param,fn)
    }


    //TODO: Add more as needed
}

public extension PCollection {
    /// Core GroupByKey transform. Requires a pair input
    func groupByKey<K,V>() -> PCollection<KV<K,V>> where Of == KV<K,V> {
        // Adjust the coder for the pcollection to reflect GBK semantcs
        let output = PCollection<KV<K,V>>(coder:.keyvalue(.of(type: K.self)!, .of(type: Array<V>.self)!))
        self.apply(.groupByKey(AnyPCollection(self),AnyPCollection(output)))
        return output
    }
    
    
    
}
