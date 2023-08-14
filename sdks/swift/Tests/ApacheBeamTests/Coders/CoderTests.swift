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


import XCTest
@testable import ApacheBeam

final class CoderTests: XCTestCase {
    
    func testSimpleScalarConversions() throws {
        XCTAssertTrue(Coder.of(type: Data.self) == .bytes)
        XCTAssertTrue(Coder.of(type: String.self) == .string)
        XCTAssertTrue(Coder.of(type: Bool.self) == .boolean)
        XCTAssertTrue(Coder.of(type: Int.self) == .varint)
    }
    
    
    func testDefaultImpulseDecode() throws {
        var impulse = Data([0x7f,0xdf,0x3b,0x64,0x5a,0x1c,0xac,0x09,0x00,0x00,0x00,0x01,0x0f,0x00])
        let impulseCoder = Coder.windowedvalue(.bytes, .globalwindow)

        let value = try impulseCoder.decode(&impulse)
        switch value {
        case let .windowed(value, _, _, window):
            let data = value.baseValue as! Data
            XCTAssertTrue(data.count == 0)
            
            let w = window.baseValue as! Window
            switch w {
            case .global:
                break
            default:
                throw ApacheBeamError.runtimeError("Expected window to be global not \(w)")
            }
            
        default:
            throw ApacheBeamError.runtimeError("Expecting a windowed value, got \(value)")
        }
    }
    
    func testWindowedValue() throws {
        let coder = Coder.windowedvalue(.bytes, .globalwindow)
        let timestamp = Date.now
        var data = try coder.encode((Data(),timestamp,Window.global))
        XCTAssertEqual(data.count, 14)
        let value = try coder.decode(&data)
        switch value {
        case let .windowed(_,ts,_,_):
            XCTAssertTrue("\(timestamp)" == "\(ts)")
        default:
            throw ApacheBeamError.runtimeError("Expected a windowed value, got \(value)")
        }
    }
}

