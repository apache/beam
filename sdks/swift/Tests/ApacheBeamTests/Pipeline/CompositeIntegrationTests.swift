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

import ApacheBeam
import XCTest

/// Simple composite. Interesting the type resolution in composites doesn't work as well as other things? Not sure why that is. Not a huge deal.
public struct FixtureWordCount: PTransform {

    let fixtures: [String]

    @PValue
    var pipeline : PipelineRoot
    
    public init(fixtures:[String]) {
        self.fixtures = fixtures
    } 

    public var expand: some PTransform {
        let (contents, errors) = pipeline
            .create(fixtures)
            .pstream(name: "Read Files") { (filenames, output: PCollectionStream<String>, errors: PCollectionStream<String>) in
                for await (filename, ts, w) in filenames {
                    do {
                        try output.emit(String(decoding: fixtureData(filename), as: UTF8.self), timestamp: ts, window: w)
                    } catch {
                        errors.emit("Unable to read \(filename): \(error)", timestamp: ts, window: w)
                    }
                }
            }

        let baseCount = contents.pstream { (contents, lines: PCollectionStream<String>) in
            for await (content, ts, w) in contents {
                content.enumerateLines { line, _ in
                    lines.emit(line, timestamp: ts, window: w)
                }
            }
        }
        .flatMap { $0.components(separatedBy: .whitespaces) }
        .groupBy { ($0, 1) }
        .sum()

        Output("counts") {
            baseCount.groupBy {
                ($0.key.lowercased().trimmingCharacters(in: .punctuationCharacters),
                 $0.value ?? 1)
            }.sum()
        }

        Output("errors") {
            errors
        }
    }
}

/// Test cases for composite test
final class CompositeIntegrationTests: XCTestCase {
    override func setUpWithError() throws {}

    override func tearDownWithError() throws {}

    func testCompositeWordCount() async throws {
        throw XCTSkip()
        try await Pipeline {
            FixtureWordCount(fixtures: ["file1.txt", "file2.txt", "missing.txt"])
        }.run(PortableRunner(loopback: true))
    }
}
