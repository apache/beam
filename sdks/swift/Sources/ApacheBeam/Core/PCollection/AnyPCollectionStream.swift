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

public struct AnyPCollectionStream : AsyncSequence {
    public typealias Element = Iterator.Element
    public typealias AsyncIterator = Iterator
    
    public struct Iterator : AsyncIteratorProtocol {
        public typealias Element = (Any,Date,Window)
        
        let nextClosure: () async throws -> Element?
        public mutating func next() async throws -> Element? {
            return try await nextClosure()
        }
    }
    
    let value: Any
    let nextGenerator: (Any) -> (() async throws -> Iterator.Element?)
    let emitClosure: (Any,Any) -> Void
    let finishClosure: (Any) -> Void
    
    public func makeAsyncIterator() -> Iterator {
        return Iterator(nextClosure: nextGenerator(value))
    }

    public init(_ value: AnyPCollectionStream) {
        self = value
    }

    public init<Of>(_ value: PCollectionStream<Of>) {
        self.value = value

        self.emitClosure = {
            let stream = ($0 as! PCollectionStream<Of>)
            if let beamValue = $1 as? BeamValue {
                stream.emit(beamValue)
            } else if let element = $1 as? Element {
                stream.emit((element.0 as! Of,element.1,element.2))
            } else if let element = $1 as? PCollectionStream<Of>.Element {
                stream.emit(element)
            }
        }
        
        self.finishClosure = {
            ($0 as! PCollectionStream<Of>).finish()
        }
        
        self.nextGenerator = {
            var iterator = ($0 as! PCollectionStream<Of>).makeAsyncIterator()
            return {
                if let element = await iterator.next() {
                    return (element.0 as Any,element.1,element.2)
                } else {
                    return nil
                }
            }
        }
    }
    
    public func stream<Out>() -> PCollectionStream<Out> {
        return value as! PCollectionStream<Out>
    }
    
    public func finish() {
        finishClosure(self.value)
    }
    
    
}
