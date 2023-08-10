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
    mutating func varint(_ value: Int) {
        var current = UInt64(value)
        while current >= 0x80 {
            self.append(UInt8(truncatingIfNeeded: current)|0x80)
            current >>= 7
        }
        self.append(UInt8(current))
    }
    
    mutating func next<T>(_ value: T) {
        Swift.withUnsafeBytes(of:value) {
            self.append(contentsOf: $0)
        }
    }
}
