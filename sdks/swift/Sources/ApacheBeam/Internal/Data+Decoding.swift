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

extension Data {
    /// advanced(by:) has issues on non-macOS Foundation implementations
    @inlinable
    func safeAdvance(by: Int) -> Data {
        #if os(macOS)
            return advanced(by: by)
        #else
            if by == 0 {
                return self
            } else if by >= count {
                return Data()
            } else {
                return advanced(by: by)
            }
        #endif
    }

    /// Read a variable length integer from the current data
    mutating func varint() throws -> Int {
        var advance = 0
        let result = try withUnsafeBytes {
            try $0.baseAddress!.withMemoryRebound(to: UInt8.self, capacity: 4) {
                var p = $0
                if p.pointee & 0x80 == 0 {
                    advance += 1
                    return Int(UInt64(p.pointee))
                }
                var value = UInt64(p.pointee & 0x7F)
                var shift = UInt64(7)
                var count = 1
                p = p.successor()
                while true {
                    if shift > 63 {
                        throw ApacheBeamError.runtimeError("Malformed Varint. Too large.")
                    }
                    count += 1
                    value |= UInt64(p.pointee & 0x7F) << shift
                    if p.pointee & 0x80 == 0 {
                        advance += count
                        return Int(value)
                    }
                    p = p.successor()
                    shift += 7
                }
            }
        }
        self = safeAdvance(by: advance)
        return result
    }

    mutating func instant() throws -> Date {
        let millis = try next(Int64.self) &+ Int64(-9_223_372_036_854_775_808)
        return Date(millisecondsSince1970: millis)
    }

    /// Read a length prefixed chunk of data
    mutating func subdata() throws -> Data {
        let length = try varint()
        let result = subdata(in: 0 ..< length)
        self = safeAdvance(by: length)
        return result
    }

    /// Extracts a fixed width type. Coming off the wire this will always be bigendian
    mutating func next<T: FixedWidthInteger>(_: T.Type) throws -> T {
        let size = MemoryLayout<T>.size
        let bigEndian = withUnsafeBytes {
            $0.load(as: T.self)
        }
        self = safeAdvance(by: size)
        return T(bigEndian: bigEndian)
    }

    mutating func next<T: FloatingPoint>(_: T.Type) throws -> T {
        let result = withUnsafeBytes {
            $0.load(as: T.self)
        }
        self = safeAdvance(by: MemoryLayout<T>.size)
        return result
    }
}
