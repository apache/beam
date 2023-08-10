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

public extension Coder {

    func encode(_ value: Any?) throws -> Data {
        var data = Data()
        try self.encode(value,data: &data)
        return data
    }
    func encode(_ value: Any?,data: inout Data) throws {
        switch self {
            // Scalar values check for size 0 input data and return null if that's a problem
                
            // TODO: Endian and other encoding checks
                
        case .bytes:
            if let v = value as? Data {
                data.varint(v.count)
                data.append(v)
            }
        case .string:
            if let v = value as? String {
                let d = Data(v.utf8)
                data.varint(d.count)
                data.append(d)
            }
        case .varint:
            if let v = value as? Int {
                data.varint(v)
            }
        case .fixedint:
            if let v = value as? Int {
                data.next(v)
            }
        case .byte:
            if let v = value as? UInt8 {
                data.next(v)
            }
        case .boolean:
            if let v = value as? Bool {
                let byte: UInt8 = v ? 1 : 0
                data.next(byte)
            }
        case .double:
            if let v = value as? Double {
                data.next(v)
            }
        case .globalwindow:
            break
        case .lengthprefix(let coder):
            let subData = try coder.encode(value)
            data.varint(subData.count)
            data.append(subData)
        case let .keyvalue(keyCoder, valueCoder):
            if let v = value as? AnyKeyValue {
                try keyCoder.encode(v.anyKey, data: &data)
                // We do a special case check here to account for the fact that
                // keyvalue is used both for group by as well as a pair type
                switch valueCoder {
                case .iterable(_):
                    try valueCoder.encode(v.anyValues,data:&data)
                default:
                    try valueCoder.encode(v.anyValue,data:&data)
                }
            }
        case let .iterable(coder):
            if let v = value as? [Any] {
                data.next(Int32(truncatingIfNeeded: v.count))
                for item in v {
                    try coder.encode(item, data: &data)
                }
            }
        case let .windowedvalue(valueCoder, windowCoder):
            if let (v,ts,w) = value as? (Any,Date,Window) {
                //Timestamp
                data.next( (ts.millisecondsSince1970 &- Int64(-9223372036854775808)).bigEndian )
                switch w {
                case .global:
                    data.next(Int32(0))
                default:
                    data.next(Int32(1))
                    try windowCoder.encode(w,data:&data)
                }
                // TODO: Real Panes
                data.append(UInt8(1 >> 5 | 1 >> 6 | 1 >> 7))
                try valueCoder.encode(v, data: &data)
            }
            default:
                throw ApacheBeamError.runtimeError("Encoding of \(self.urn) coders not supported.")
        }
    }
}
