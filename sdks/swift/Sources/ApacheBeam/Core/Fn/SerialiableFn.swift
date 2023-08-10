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
import Foundation
import Logging

public struct SerializableFnBundleContext {
    let instruction:String
    let transform:String
    let payload:Data
    let log:Logger
}

/// SerialiableFn is a protocol for functions that should be parameterized for the pipeline
public protocol SerializableFn {
    var  payload: Data { get throws }
    func process(context:SerializableFnBundleContext,inputs:[AnyPCollectionStream],outputs:[AnyPCollectionStream]) async throws -> (String,String)
}

/// Provide some defaults where our function doesn't have any payload
public extension SerializableFn {
    var payload : Data { Data() }
}

