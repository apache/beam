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

public struct AnyPCollection : PCollectionProtocol {
    
    
    
    let type: Any.Type
    let ofType: Any.Type
    let collection: Any
    
    let applyClosure: (Any,PipelineTransform) -> Void
    let consumersClosure: (Any) -> [PipelineTransform]
    let coderClosure: (Any) -> Coder
    let streamClosure: (Any) -> AnyPCollectionStream
    
    public init<C>(_ collection: C) where C : PCollectionProtocol {
        if let anyCollection = collection as? AnyPCollection {
            self = anyCollection
        } else {
            self.type = C.self
            self.ofType = C.Of.self
            self.collection = collection
            
            self.applyClosure = { ($0 as! C).apply($1) }
            self.consumersClosure = { ($0 as! C).consumers }
            self.coderClosure = { ($0 as! C).coder }
            self.streamClosure = { AnyPCollectionStream(($0 as! C).stream) }
        }
    }
    
    
    public var consumers: [PipelineTransform] {
        consumersClosure(collection)
    }
    
    public func apply(_ transform: PipelineTransform) {
        applyClosure(collection,transform)
    }

    
    public var coder: Coder {
        coderClosure(collection)
    }

    public var stream: PCollectionStream<Never> {
        fatalError("Do not use `stream` on AnyPCollection. Use `anyStream` instead.")
    }
    
    public var anyStream: AnyPCollectionStream {
        streamClosure(collection)
    }
    
}

extension AnyPCollection : Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self.collection as AnyObject))
    }
    public static func ==(lhs: AnyPCollection,rhs:AnyPCollection) -> Bool {
        return ObjectIdentifier(lhs.collection as AnyObject) == ObjectIdentifier(rhs.collection as AnyObject)
    }
}
