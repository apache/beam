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

extension FieldValue {
    static func from(_ proto: FieldValueProto) throws -> FieldValue {
        switch proto.fieldValue {
        case let .atomicValue(value):
            switch value.value {
            case let .boolean(b):
                .boolean(b)
            case let .byte(b):
                .int(Int(b), .byte)
            case let .bytes(d):
                .bytes(d)
            case let .double(d):
                .float(d, .double)
            case let .float(f):
                .float(Double(f), .float)
            case let .int16(i):
                .int(Int(i), .int16)
            case let .int32(i):
                .int(Int(i), .int32)
            case let .int64(i):
                .int(Int(i), .int64)
            case let .string(s):
                .string(s)
            default:
                .null
            }
        case let .arrayValue(arrayValue):
            try .array(arrayValue.element.map { try .from($0) })
        case let .mapValue(mapValue):
            try .map(mapValue.entries.map {
                try (.from($0.key), .from($0.value))
            })
        default:
            .null
        }
    }
}
