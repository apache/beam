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

/// This extension contains all of the decoding implementation. File separation is for clarity.
public extension Coder {
    /// Decodes a raw data block into a BeamValue for further processing
    func decode(_ data: inout Data) throws -> BeamValue {
        switch self {
        // Scalar values check for size 0 input data and return null if that's a problem

        case .bytes:
            return try .bytes(data.count == 0 ? Data() : data.subdata())
        case .string:
            return try .string(data.count == 0 ? "" : String(data: data.subdata(), encoding: .utf8))
        case .varint:
            return try .integer(data.count == 0 ? nil : data.varint())
        case .fixedint:
            return try .integer(data.count == 0 ? nil : data.next(Int.self))
        case .byte:
            return try .integer(data.count == 0 ? nil : Int(data.next(UInt8.self)))
        case .boolean:
            return try .boolean(data.count == 0 ? nil : data.next(UInt8.self) != 0)
        case .double:
            return try .double(data.count == 0 ? nil : data.next(Double.self))
        case .globalwindow:
            return .window(.global)
        case let .lengthprefix(coder): // Length prefix basically serves to make values nullable
            var subdata = try data.subdata()
            return try coder.decode(&subdata)
        case let .keyvalue(keyCoder, valueCoder):
            return try .kv(keyCoder.decode(&data), valueCoder.decode(&data))
        case let .iterable(coder):
            let length = try data.next(Int32.self)
            return try .array((0 ..< length).map { _ in try coder.decode(&data) })
        case let .windowedvalue(valueCoder, windowCoder):
            // This will be big endian to match java
            let timestamp = try data.instant()

            let windowCount = try data.next(Int32.self)
            if windowCount > 1 {
                throw ApacheBeamError.runtimeError("Windowed values with > 1 window not yet supported")
            }
            let window = try windowCoder.decode(&data)

            // TODO: Actually handle pane info
            let pane = try data.next(UInt8.self)
            switch (pane >> 4) & 0x0F {
            case 0x0:
                break
            case 0x1:
                _ = try data.varint()
            case 0x2:
                _ = try data.varint()
                _ = try data.varint()
            default:
                throw ApacheBeamError.runtimeError("Invalid pane encoding \(String(pane, radix: 2))")
            }
            return try .windowed(valueCoder.decode(&data), timestamp, pane, window)
        case let .row(schema):
            return try .row(.from(data: &data, as: .row(schema))!)
        default:
            throw ApacheBeamError.runtimeError("Decoding of \(urn) coders not supported.")
        }
    }
}
