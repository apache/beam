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

/// Creating Static Values
public extension PCollection {
    /// Each time the input fires output all of the values in this list.
    func create<Value: Codable>(_ values: [Value], name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Value> {
        pstream(name: name ?? "\(_file):\(_line)", type: .bounded, values) { values, input, output in
            for try await (_, ts, w) in input {
                for v in values {
                    output.emit(v, timestamp: ts, window: w)
                }
            }
        }
    }
}

/// Convenience logging mappers
public extension PCollection {
    @discardableResult
    func log(prefix: String, _ name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Of> where Of == String {
        pstream(name: name ?? "\(_file):\(_line)", prefix) { prefix, input, output in
            for await element in input {
                print("\(prefix): \(element)")
                output.emit(element)
            }
        }
    }

    @discardableResult
    func log<K, V>(prefix: String, _ name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<KV<K, V>> where Of == KV<K, V> {
        pstream(name: name ?? "\(_file):\(_line)", prefix) { prefix, input, output in
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
    func map<Out>(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> Out) -> PCollection<Out> {
        pardo(name: name ?? "\(_file):\(_line)") { input, output in
            output.emit(fn(input.value))
        }
    }

    /// Map function to convert a tuple pair into an encodable key-value pair
    func map<K, V>(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> (K, V)) -> PCollection<KV<K, V>> {
        pardo(name: name ?? "\(_file):\(_line)") { input, output in
            let (key, value) = fn(input.value)
            output.emit(KV(key, value))
        }
    }

    /// Produce multiple outputs as a single value without modifying window or timestamp
    func flatMap<Out>(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> [Out]) -> PCollection<Out> {
        pardo(name: name ?? "\(_file):\(_line)") { input, output in
            for i in fn(input.value) {
                output.emit(i)
            }
        }
    }
}

/// Modifying Timestamps
public extension PCollection {
    func timestamp(name: String? = nil, _file: String = #fileID, _line: Int = #line, _ fn: @Sendable @escaping (Of) -> Date) -> PCollection<Of> {
        pstream(name: name ?? "\(_file):\(_line)") { input, output in
            for try await (value, _, w) in input {
                output.emit(value, timestamp: fn(value), window: w)
            }
        }
    }
}

public extension PCollection<Never> {
    /// Convenience function to add an impulse when we are at the root of the pipeline
    func create<Value: Codable>(_ values: [Value], name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Value> {
        impulse().create(values, name: name, _file: _file, _line: _line)
    }
}

public func create<Value: Codable>(_ values: [Value], name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Value> {
    let root = PCollection<Never>(type: .bounded)
    return root.create(values, name: name, _file: _file, _line: _line)
}
