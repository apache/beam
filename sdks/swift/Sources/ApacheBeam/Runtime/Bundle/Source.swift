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
import Logging

/// Custom SerializableFn that reads/writes from an external data stream using a defined coder. It assumes that a given
/// data element might contain more than one coder
final class Source: SerializableFn {
    let client: DataplaneClient
    let coder: Coder
    let log: Logger

    public init(client: DataplaneClient, coder: Coder) {
        self.client = client
        self.coder = coder
        log = Logger(label: "Source")
    }

    func process(context: SerializableFnBundleContext,
                 inputs _: [AnyPCollectionStream], outputs: [AnyPCollectionStream]) async throws -> (String, String)
    {
        log.info("Waiting for input on \(context.instruction)-\(context.transform)")
        let (stream, _) = await client.makeStream(instruction: context.instruction, transform: context.transform)

        var messages = 0
        var count = 0
        for await message in stream {
            messages += 1
            switch message {
            case let .data(data):
                var d = data
                while d.count > 0 {
                    let value = try coder.decode(&d)
                    for output in outputs {
                        try output.emit(value: value)
                        count += 1
                    }
                }
            case let .last(id, transform):
                for output in outputs {
                    output.finish()
                }
                await client.finalizeStream(instruction: id, transform: transform)
                log.info("Source \(context.instruction),\(context.transform) handled \(count) items over \(messages) messages")
                return (id, transform)
            // TODO: Handle timer messages
            default:
                log.info("Unhanled message \(message)")
            }
        }
        return (context.instruction, context.transform)
    }
}
