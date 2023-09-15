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

public protocol AnyKeyValue {
    var anyKey: Any { get }
    var anyValues: [Any] { get }
    var anyValue: Any? { get }
}

/// A structure representing an array of values grouped by key
public struct KV<Key, Value>: AnyKeyValue {
    public let key: Key
    public let values: [Value]
    public var value: Value? { values.first }

    public init(_ key: Key, _ value: Value) {
        self.key = key
        values = [value]
    }

    public init(_ key: Key, values: [Value]) {
        self.key = key
        self.values = values
    }

    public init(beam value: BeamValue) throws {
        switch value {
        case let .windowed(v, _, _, _):
            self = try KV<Key, Value>(beam: v)
        case let .kv(k, v):
            key = k.baseValue! as! Key
            values = v.baseValue as! [Value]
        default:
            throw ApacheBeamError.runtimeError("KV can only accept kv or windowed kv types")
        }
    }

    public var anyKey: Any { key }
    public var anyValues: [Any] { values.map { $0 as Any } }
    public var anyValue: Any? { value }
}

extension KV: Beamable {
    public static var coder: Coder {
        .keyvalue(.of(type: Key.self)!, .of(type: Value.self)!)
    }
}
