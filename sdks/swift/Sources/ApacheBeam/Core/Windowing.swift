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


public enum Window {
    case global
    case bounded(Date)
    case interval(Date,Date)

    var maxTimestamp : Date {
        get {
            switch(self) {
                case .global: return Date.distantFuture
                case .bounded(let end): return end
                case .interval(_, let end): return end
            }
        }
    }
}

public enum WindowingStrategy {
    
}

public enum Timing : String {
    case early = "EARLY"
    case onTime = "ON_TIME"
    case late = "LATE"
    case unknown = "UNKNOWN"
}

public protocol PaneInfo {
    var timing : Timing { get }
    var index : UInt64 { get }
    var onTimeIndex: UInt64 { get }
    var first : Bool { get }
    var last : Bool { get }
}
