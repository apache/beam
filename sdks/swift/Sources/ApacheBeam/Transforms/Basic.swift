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

/// Creating Static Values
public extension PCollection {

    /// Each time the input fires output all of the values in this list.
    func create<Value:Codable>(_ values: [Value],_ name:String? = nil,_file:String=#fileID,_line:Int=#line) -> PCollection<Value> {
        
        return pardo(name,_file:_file,_line:_line,values) { values,input,output in
            for try await (_,ts,w) in input {
                for v in values {
                    output.emit(v,timestamp:ts,window:w)
                }
            }
        }
    }
}

/// Convenience logging mappers
public extension PCollection {
    
    @discardableResult
    func log(prefix:String,_ name:String? = nil,_file:String=#fileID,_line:Int=#line) -> PCollection<Of> where Of == String {
        pardo(name,_file:_file,_line:_line,prefix) { prefix,input,output in
            for await element in input {
                print("\(prefix): \(element)")
                output.emit(element)
            }
        }
    }
    
    @discardableResult
    func log<K,V>(prefix:String,_ name:String? = nil,_file:String=#fileID,_line:Int=#line) -> PCollection<KV<K,V>> where Of == KV<K,V> {
        pardo(name,_file:_file,_line:_line,prefix) { prefix,input,output in
            for await element in input {
                let kv = element.0
                for v in kv.values {
                    print("\(prefix): \(kv.key),\(v)")
                }
                output.emit(element)
            }
        }
    }
}

/// Modifying Values
public extension PCollection {
    
    /// Modify a value without changing its window or timestamp
    func map<Out>(_ name:String? = nil,_file:String=#fileID,_line:Int=#line,_ fn: @Sendable @escaping (Of) -> Out) -> PCollection<Out> {
        return pardo(name,_file:_file,_line:_line) { input,output in
            for try await (v,ts,w) in input {
                output.emit(fn(v),timestamp:ts,window:w)
            }
        }
    }
    
    func map<K,V>(_ name:String? = nil,_file:String=#fileID,_line:Int=#line,_ fn: @Sendable @escaping (Of) -> (K,V)) -> PCollection<KV<K,V>> {
        return pardo(name,_file:_file,_line:_line) { input,output in
            for try await (i,ts,w) in input {
                let (key,value) = fn(i)
                output.emit(KV(key,value),timestamp:ts,window:w)
            }
        }
    }

    /// Produce multiple outputs as a single value without modifying window or timestamp
    func flatMap<Out>(name:String? = nil,_file:String=#fileID,_line:Int=#line,_ fn: @Sendable @escaping (Of) -> [Out]) -> PCollection<Out> {
        return pardo(name,_file:_file,_line:_line) { input,output in
            for try await (v,ts,w) in input {
                for i in fn(v) {
                    output.emit(i,timestamp:ts,window:w)
                }
            }
        }
    }

}

public extension PCollection<Never> {
    /// Convenience function to add an impulse when we are at the root of the pipeline
    func create<Value:Codable>(_ values: [Value],_ name:String? = nil,_file:String=#fileID,_line:Int=#line) -> PCollection<Value> {
        return impulse().create(values,name,_file:_file,_line:_line)
    }
}
