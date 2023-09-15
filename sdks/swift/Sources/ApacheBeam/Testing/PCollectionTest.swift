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

import Logging

/// Test harness for PCollections. Primarily designed for unit testing.
public struct PCollectionTest {
    let output: AnyPCollection
    let fn: (Logger, [AnyPCollectionStream], [AnyPCollectionStream]) async throws -> Void

    public init(_ collection: PCollection<some Any>, _ fn: @escaping (Logger, [AnyPCollectionStream], [AnyPCollectionStream]) async throws -> Void) {
        output = AnyPCollection(collection)
        self.fn = fn
    }

    public func run() async throws {
        let log = Logger(label: "Test")
        if let transform = output.parent {
            switch transform {
            case let .pardo(parent, _, fn, outputs):
                let context = try SerializableFnBundleContext(instruction: "1", transform: "test", payload: fn.payload, log: log)
                let input = parent.anyStream
                let streams = outputs.map(\.anyStream)

                try await withThrowingTaskGroup(of: Void.self) { group in
                    log.info("Starting process task")
                    group.addTask {
                        _ = try await fn.process(context: context, inputs: [input], outputs: streams)
                    }
                    log.info("Calling Stream")
                    group.addTask {
                        try await self.fn(log, [input], streams)
                    }
                    for try await _ in group {}
                }
            default:
                throw ApacheBeamError.runtimeError("Not able to test this type of transform \(transform)")
            }
        } else {
            throw ApacheBeamError.runtimeError("Unable to determine parent transform to test")
        }
    }
}
