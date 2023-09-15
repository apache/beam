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
    mutating func next(_ schema: Schema) throws -> FieldValue {
        try .from(data: &self, as: .row(schema))!
    }
}

public extension FieldValue {
    static func from(data: inout Data, as type: FieldType) throws -> FieldValue? {
        switch type {
        case .unspecified:
            return .undefined
        case .byte, .int16, .int32, .int64:
            return try .int(data.varint(), type)
        case .float:
            return try .float(Double(data.next(Float.self)), type)
        case .double:
            return try .float(data.next(Double.self), type)
        case .string:
            return try .string(String(data: data.subdata(), encoding: .utf8)!)
        case .datetime:
            return try .datetime(data.instant())
        case .boolean:
            return try .boolean(data.next(UInt8.self) == 1)
        case .bytes:
            return try .bytes(data.subdata())
        case .decimal:
            throw ApacheBeamError.runtimeError("Decimal types not yet implemented")
        case .logical:
            throw ApacheBeamError.runtimeError("Logical Types not yet supported")
        case let .row(schema):

            let count = try data.varint()

            // Consume the null byte pattern
            let nullBytes = Int(ceil(Double(count) / 8.0))
            let nullBits = try data.subdata()
            data = data.advanced(by: nullBytes)

            var fields: [FieldValue] = (0 ..< count).map { i in
                let byte = Int(floor(Double(i) / 8.0))
                let bit = i % 8
                return (Int(nullBits[byte]) & bit) > 0 ? .null : .undefined
            }
            for field in schema.fields {
                // TODO: Null check
                try fields.append(.from(data: &data, as: field.type)!)
            }
            return .row(schema, fields)
        case let .nullable(baseType):
            return try .from(data: &data, as: baseType)
        case .array:
            let count = try Int(data.next(Int32.self))
            if count < 1 {
                throw ApacheBeamError.runtimeError("Indeterminate length not implement yet.")
            }
            return try .array((0 ..< count).map { _ in try .from(data: &data, as: type)! })
        case let .repeated(type):
            let count = try Int(data.next(Int32.self))
            if count < 1 {
                throw ApacheBeamError.runtimeError("Indeterminate length not implement yet.")
            }
            return try .repeated((0 ..< count).map { _ in try .from(data: &data, as: type)! })
        case let .map(key, value):
            let count = try Int(data.next(UInt32.self))
            return try .map((0 ..< count).map { _ in
                try (.from(data: &data, as: key)!, .from(data: &data, as: value)!)
            })
        }
    }
}
