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

/// The worker side realization of a PCollection that supports reading and writing
public final class PCollectionStream<Of> : AsyncSequence {
    public typealias Element = (Of,Date,Window)
    
    private let stream: AsyncStream<Element>
    private let emitter: AsyncStream<Element>.Continuation
    
    public init() {
        //Construct a stream, capturing the emit continuation
        var tmp: AsyncStream<Element>.Continuation?
        self.stream = AsyncStream<Element> { tmp = $0 }
        self.emitter = tmp!
    }
    
    public func makeAsyncIterator() -> AsyncStream<Element>.Iterator {
        return stream.makeAsyncIterator()
    }
    
    public func finish() {
        emitter.finish()
    }
    

    public func emit(_ value: Element) {
        emitter.yield(value)
    }
    
    public func emit(_ value: Of,timestamp: Date = .now,window:Window = .global) {
        emit((value,timestamp,window))
    }
    
    public func emit(_ value: BeamValue) {
        
    }
    
}
