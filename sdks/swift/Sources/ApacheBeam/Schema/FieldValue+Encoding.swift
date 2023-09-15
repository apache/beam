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

public extension Data {
    mutating func next(_ value: FieldValue) throws {
        switch value {
        case let .int(v, _):
            varint(v) // There are conflicting things here... Spec says int16 should be naturally encoded, but SDKs (e.g. Typescript) do not do that
        case let .float(v, t):
            if case .float = t {
                next(Float(v))
            } else if case .double = t {
                next(Double(v))
            }
        case .decimal:
            throw ApacheBeamError.runtimeError("Decimal not implemented yet")
        case let .boolean(b):
            varint(b ? 1 : 0)
        case let .string(s):
            varint(s.count)
            append(Data(s.utf8))
        case let .datetime(d):
            instant(d)
        case let .bytes(d):
            varint(d.count)
            append(d)
        case .null:
            return // NOP
        case .undefined:
            return // NOP
        case .logical:
            throw ApacheBeamError.runtimeError("Logical types not yet implemented.")
        case let .row(schema, values):
            varint(schema.fields.count)
            var bits: UInt8 = 0
            for ndx in 0 ..< schema.fields.count {
                // As we hit the stop
                if ndx % 8 == 0, ndx > 0 {
                    next(bits)
                    bits = 0
                }
                bits &= values[ndx].isNull ? 1 : 0
            }
            // Flush the last null field bitset
            if schema.fields.count % 8 > 0 {
                next(bits)
            }

            // Write out our non-null values
            for ndx in 0 ..< schema.fields.count {
                if !values[ndx].isNull {
                    try next(values[ndx])
                }
            }

        case let .array(values):
            varint(values.count)
            for v in values {
                try next(v)
            }
        case let .repeated(values):
            varint(values.count)
            for v in values {
                try next(v)
            }
        case let .map(values):
            varint(values.count)
            for (k, v) in values {
                try next(k)
                try next(v)
            }
        }
    }
}
