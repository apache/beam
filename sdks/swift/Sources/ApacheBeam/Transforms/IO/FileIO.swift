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

public protocol FileIOSource {
    static func readFiles(matching: PCollection<KV<String, String>>) -> PCollection<Data>
    static func listFiles(matching: PCollection<KV<String, String>>) -> PCollection<KV<String, String>>
}

public extension PCollection<KV<String, String>> {
    func readFiles<Source: FileIOSource>(in _: Source.Type) -> PCollection<Data> {
        Source.readFiles(matching: self)
    }

    /// Takes a KV pair of (bucket,prefix) and returns a list of (bucket,filename)
    func listFiles<Source: FileIOSource>(in _: Source.Type) -> PCollection<KV<String, String>> {
        Source.listFiles(matching: self)
    }
}
