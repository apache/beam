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

/// An enum representing values coming over the FnApi Data Plane.
public indirect enum BeamValue {
    /// A value not representable in the Swift SDK
    case invalid(String)
    
    // Scalar values
    
    /// Bytes coded
    case bytes(Data?)
    /// UTF8 Strings
    case string(String?)
    /// Integers (Signed 64-bit)
    case integer(Int?)
    /// Doubles
    case double(Double?)
    /// Booleans
    case boolean(Bool?)
    /// A window
    case window(Window)
    
    //TODO: Custom Values and Row Values
    
    // Composite Values
    
    /// An iterable
    case array([BeamValue])
    /// A key-value pair
    case kv(BeamValue,BeamValue)
    /// A windowed value
    case windowed(BeamValue,Date,UInt8,BeamValue)
    
    /// Convenience method for extacting the base value from one
    /// of the scalar representations.
    var baseValue : Any? {
        switch self {
        case let .bytes(d):
            d
        case let .string(s):
            s
        case let .integer(i):
            i
        case let .double(d):
            d
        case let .boolean(b):
            b
        case let .window(w):
            w
        default:
            nil as Any?
        }
    }
 
}
