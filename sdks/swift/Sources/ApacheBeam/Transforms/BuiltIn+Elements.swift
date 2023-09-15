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

public extension PCollection {
    // No Output
    func pardo(name: String, _ fn: @Sendable @escaping (PInput<Of>) async throws -> Void) {
        pstream(name: name) { input in
            for try await element in input {
                try await fn(PInput(element))
            }
        }
    }

    // One Output
    func pardo<O0>(name: String, _ fn: @Sendable @escaping (PInput<Of>, POutput<O0>) async throws -> Void) -> PCollection<O0> {
        pstream(name: name) { input, output in
            for try await element in input {
                try await fn(PInput(element),
                             POutput(stream: output, timestamp: element.1, window: element.2))
            }
        }
    }

    // Two Outputs
    func pardo<O0, O1>(name: String, _ fn: @Sendable @escaping (PInput<Of>, POutput<O0>, POutput<O1>) async throws -> Void) -> (PCollection<O0>, PCollection<O1>) {
        pstream(name: name) { input, o0, o1 in
            for try await element in input {
                try await fn(PInput(element),
                             POutput(stream: o0, timestamp: element.1, window: element.2),
                             POutput(stream: o1, timestamp: element.1, window: element.2))
            }
        }
    }
}
