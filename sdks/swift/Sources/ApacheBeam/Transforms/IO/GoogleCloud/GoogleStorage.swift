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

import Dispatch
import Foundation
import OAuth2

struct ListFilesResponse: Codable {
    struct Item: Codable {
        let kind: String
        let selfLink: String
        let mediaLink: String
        let name: String
        let bucket: String
        let size: String
    }

    let kind: String
    let items: [Item]
}

public struct GoogleStorage: FileIOSource {
    public static func readFiles(matching: PCollection<KV<String, String>>) -> PCollection<Data> {
        matching.pstream(type: .bounded) { matching, output in
            guard let tokenProvider = DefaultTokenProvider(scopes: ["storage.objects.get"]) else {
                throw ApacheBeamError.runtimeError("Unable to get OAuth2 token.")
            }
            let connection = Connection(provider: tokenProvider)
            for await (file, ts, w) in matching {
                let bucket = file.key
                for name in file.values {
                    let url = "https://storage.googleapis.com/storage/v1/b/\(bucket)/o/\(name.addingPercentEncoding(withAllowedCharacters: .urlHostAllowed)!)"
                    let response: Data? = try await withCheckedThrowingContinuation { continuation in
                        do {
                            try connection.performRequest(method: "GET",
                                                          urlString: url,
                                                          parameters: ["alt": "media"], body: nil)
                            {
                                data, _, error in
                                if let e = error {
                                    continuation.resume(throwing: e)
                                } else {
                                    continuation.resume(returning: data)
                                }
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    if let d = response {
                        output.emit(d, timestamp: ts, window: w)
                    }
                }
            }
        }
    }

    public static func listFiles(matching: PCollection<KV<String, String>>) -> PCollection<KV<String, String>> {
        matching.pstream(type: .bounded) { matching, output in

            guard let tokenProvider = DefaultTokenProvider(scopes: ["storage.objects.list"]) else {
                throw ApacheBeamError.runtimeError("Unable to get OAuth2 token.")
            }
            let connection = Connection(provider: tokenProvider)
            for await (match, ts, w) in matching {
                let bucket = match.key
                for prefix in match.values {
                    let response: Data? = try await withCheckedThrowingContinuation { continuation in
                        do {
                            try connection.performRequest(
                                method: "GET",
                                urlString: "https://storage.googleapis.com/storage/v1/b/\(bucket)/o",
                                parameters: ["prefix": prefix],
                                body: nil
                            ) { data, _, error in
                                if let e = error {
                                    continuation.resume(throwing: e)
                                } else {
                                    continuation.resume(returning: data)
                                }
                            }
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    if let data = response {
                        let listfiles = try JSONDecoder().decode(ListFilesResponse.self, from: data)
                        for item in listfiles.items {
                            if item.size != "0" {
                                output.emit(KV(item.bucket, item.name), timestamp: ts, window: w)
                            }
                        }
                    }
                }
            }
        }
    }
}
