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

extension Data {
    /// Read a variable length integer from the current data
    mutating func varint() throws -> Int {
        var advance: Int = 0
        let result = try self.withUnsafeBytes {
            try $0.baseAddress!.withMemoryRebound(to: UInt8.self, capacity: 4) {
                var p = $0
                if(p.pointee & 0x80 == 0) {
                    advance += 1
                    return Int(UInt64(p.pointee))
                }
                var value = UInt64(p.pointee & 0x7f)
                var shift = UInt64(7)
                var count = 1
                p = p.successor()
                while true {
                    if shift > 63 {
                        throw ApacheBeamError.runtimeError("Malformed Varint. Too large.")
                    }
                    count += 1
                    value |= UInt64(p.pointee & 0x7f) << shift
                    if(p.pointee & 0x80 == 0) {
                        advance += count
                        return Int(value)
                    }
                    p = p.successor()
                    shift += 7
                }
            }
        }
        // On non-macOS platforms this crashes due to differences in Foundation implementations
        #if os(macOS)
        self = self.advanced(by: advance)
        #else
        self = advance == count ? Data() : self.advanced(by: advance)
        #endif
        return result
    }
    
    /// Read a length prefixed chunk of data
    mutating func subdata() throws -> Data {
        let length = try self.varint()
        let result = self.subdata(in: 0..<length)
        self = self.advanced(by: length)
        return result
    }
    
    /// Extract a fixed length value from the data
    mutating func next<T>(_ type: T.Type) throws -> T {
        let size = MemoryLayout<T>.size
        let result = self.withUnsafeBytes {
            $0.baseAddress!.withMemoryRebound(to: T.self, capacity: 1) { $0.pointee }
        }
        self = self.advanced(by: size)
        return result
    }
    
}
