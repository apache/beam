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

import XCTest
import ApacheBeam

func fixtureData(_ fixture: String) throws -> Data {
    try Data(contentsOf: fixtureUrl(fixture))
}


func fixtureUrl(_ fixture: String) -> URL {
    fixturesDirectory().appendingPathComponent(fixture)
}


func fixturesDirectory(path: String = #file) -> URL {
    let url = URL(fileURLWithPath: path)
    let testsDir = url.deletingLastPathComponent()
    let res = testsDir.appendingPathComponent("Fixtures")
    return res
}

final class IntegrationTests: XCTestCase {

    override func setUpWithError() throws {
    }

    override func tearDownWithError() throws {
    }

    func testPortableWordcount() async throws {
        try await Pipeline { pipeline in
            let (contents,errors) = pipeline
                .create(["file1.txt","file2.txt","missing.txt"])
                .pardo(name:"Read Files") { filenames,output,errors in
                    for await (filename,_,_) in filenames {
                        do {
                            output.emit(String(decoding:try fixtureData(filename),as:UTF8.self))
                        } catch {
                            errors.emit("Unable to read \(filename): \(error)")
                        }
                    }
                }
            
            // Simple ParDo that takes advantage of enumerateLines. No name to test name generation of pardos 
            let lines = contents.pardo { contents,lines in
                for await (content,_,_) in contents {
                    content.enumerateLines { line,_ in
                        lines.emit(line)
                    }
                }
            }
            
            // Our first group by operation
            let baseCount = lines
                .flatMap({ $0.components(separatedBy: .whitespaces) })
                .groupBy({ ($0,1) })
                .sum()
                .log(prefix:"INTERMEDIATE OUTPUT")
            
            let normalizedCounts = baseCount.groupBy {
                ($0.key.lowercased().trimmingCharacters(in: .punctuationCharacters),
                 $0.value ?? 1)
            }.sum()
            
            normalizedCounts.log(prefix:"COUNT OUTPUT")
            errors.log(prefix:"ERROR OUTPUT")
            
        }.run(PortableRunner(loopback:true)) 
        
        
    }


}
