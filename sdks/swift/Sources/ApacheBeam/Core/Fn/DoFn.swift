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

/// A higher level interface to SerializableFn using dependency injected dynamic properties in the same
/// way as we define Composite PTransforms
public protocol DoFn {
    func process() async throws
    func finishBundle() async throws
}

public extension DoFn {
    func finishBundle() async throws {}
}

public struct PInput<Of> {
    public let value: Of
    public let timestamp: Date
    public let window: Window

    public init(_ value: Of, _ timestamp: Date, _ window: Window) {
        self.value = value
        self.timestamp = timestamp
        self.window = window
    }

    public init(_ element: (Of, Date, Window)) {
        value = element.0
        timestamp = element.1
        window = element.2
    }
}

public struct POutput<Of> {
    let stream: PCollectionStream<Of>
    let timestamp: Date
    let window: Window

    func emit(_ value: Of, timestamp: Date? = nil, window: Window? = nil) {
        stream.emit(value, timestamp: timestamp ?? self.timestamp, window: window ?? self.window)
    }
}
