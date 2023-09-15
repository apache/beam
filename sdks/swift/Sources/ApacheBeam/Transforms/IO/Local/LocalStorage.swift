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

public struct LocalStorage: FileIOSource {
    public static func readFiles(matching: PCollection<KV<String, String>>) -> PCollection<Data> {
        matching.pstream(type: .bounded) { input, output in
            let fm = FileManager.default
            for try await (dirAndFile, ts, w) in input {
                for file in dirAndFile.values {
                    output.emit(fm.contents(atPath: file)!, timestamp: ts, window: w)
                }
            }
        }
    }

    public static func listFiles(matching: PCollection<KV<String, String>>) -> PCollection<KV<String, String>> {
        matching.pstream(type: .bounded) { input, output in
            let fm = FileManager.default
            for try await (pathAndPattern, ts, w) in input {
                let path = pathAndPattern.key
                let patterns = try pathAndPattern.values.map { try Regex($0) }

                for file in try FileManager.default.contentsOfDirectory(atPath: path) {
                    for p in patterns {
                        do {
                            if try p.firstMatch(in: file) != nil {
                                output.emit(KV(path, file), timestamp: ts, window: w)
                                break
                            }
                        } catch {}
                    }
                }
            }
        }
    }
}
