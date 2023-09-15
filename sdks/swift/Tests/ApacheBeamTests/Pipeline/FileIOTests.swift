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

//
//  FileIOTests.swift
//
//
//  Created by Byron Ellis on 8/22/23.
//
import ApacheBeam
import Logging
import XCTest

final class FileIOTests: XCTestCase {
    override func setUpWithError() throws {}

    override func tearDownWithError() throws {}

    func testGoogleStorageListFiles() async throws {
        throw XCTSkip()
        try await PCollectionTest(PCollection<KV<String, String>>().listFiles(in: GoogleStorage.self)) { log, inputs, outputs in
            log.info("Sending value")
            try inputs[0].emit(value: KV("dataflow-samples", "shakespeare"))
            log.info("Value sent")
            inputs[0].finish()
            for try await (output, _, _) in outputs[0] {
                log.info("Output: \(output)")
            }
        }.run()
    }

    func testGoogleStorageReadFiles() async throws {
        throw XCTSkip()
        try await PCollectionTest(PCollection<KV<String, String>>().readFiles(in: GoogleStorage.self)) { log, inputs, outputs in
            log.info("Sending value")
            try inputs[0].emit(value: KV("dataflow-samples", "shakespeare/asyoulikeit.txt"))
            log.info("Value sent")
            inputs[0].finish()
            for try await (output, _, _) in outputs[0] {
                log.info("Output: \(String(data: output as! Data, encoding: .utf8)!)")
            }
        }.run()
    }

    func testShakespeareWordcount() async throws {
        throw XCTSkip()
        try await Pipeline { pipeline in
            let contents = pipeline
                .create(["dataflow-samples/shakespeare"])
                .map { value in
                    let parts = value.split(separator: "/", maxSplits: 1)
                    print("Got filename \(parts) from \(value)")
                    return KV(parts[0].lowercased(), parts[1].lowercased())
                }
                .listFiles(in: GoogleStorage.self)
                .readFiles(in: GoogleStorage.self)

            // Simple ParDo that takes advantage of enumerateLines. No name to test name generation of pardos
            let lines = contents.pstream { contents, lines in
                for await (content, ts, w) in contents {
                    String(data: content, encoding: .utf8)!.enumerateLines { line, _ in
                        lines.emit(line, timestamp: ts, window: w)
                    }
                }
            }

            // Our first group by operation
            let baseCount = lines
                .flatMap { (line: String) in line.components(separatedBy: .whitespaces) }
                .groupBy { ($0, 1) }
                .sum()

            let normalizedCounts = baseCount.groupBy {
                ($0.key.lowercased().trimmingCharacters(in: .punctuationCharacters),
                 $0.value ?? 1)
            }.sum()

            normalizedCounts.log(prefix: "COUNT OUTPUT")

        }.run(PortableRunner(loopback: true))
    }
}
