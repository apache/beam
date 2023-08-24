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

public protocol PCollectionProtocol {
    associatedtype Of
    
    typealias Stream = PCollectionStream<Of>
    
    var parent: PipelineTransform? { get }
    var consumers: [PipelineTransform] { get }
    var coder: Coder { get }
    var stream: Stream { get }
    
    @discardableResult
    func apply(_ transform: PipelineTransform) -> PipelineTransform
}


public final class PCollection<Of> : PCollectionProtocol {

    public let coder: Coder
    public var consumers: [PipelineTransform]
    public private(set) var parent: PipelineTransform?
    
    public init(coder: Coder = .of(type: Of.self)!,parent: PipelineTransform? = nil,consumers:[PipelineTransform] = []) {
        self.coder = coder
        self.consumers = consumers
        self.parent = parent
    }

    public var stream: PCollectionStream<Of> {
        return PCollectionStream<Of>()
    }
    
    @discardableResult
    public func apply(_ transform: PipelineTransform) -> PipelineTransform {
        consumers.append(transform)
        return transform
    }
    
    func parent(_ transform: PipelineTransform) {
        self.parent = transform
    }
    
    
}

extension PCollection : PipelineMember {
    var roots: [PCollection<Never>] { 
        if let p = parent {
            return p.roots
        } else if let p = self as? PCollection<Never> {
            return [p]
        } else {
            return []
        }
    }
    
    
}

