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

public enum StreamType {
    case bounded
    case unbounded
    case unspecified
}

public enum WindowingStrategy {
    case unspecified
}

public protocol PCollectionProtocol {
    associatedtype Of

    typealias Stream = PCollectionStream<Of>

    var parent: PipelineTransform? { get }
    var consumers: [PipelineTransform] { get }
    var coder: Coder { get }
    var stream: Stream { get }
    var streamType: StreamType { get }

    @discardableResult
    func apply(_ transform: PipelineTransform) -> PipelineTransform
}

public final class PCollection<Of>: PCollectionProtocol {
    public let coder: Coder
    public let streamType: StreamType
    public var consumers: [PipelineTransform]
    public private(set) var parent: PipelineTransform?

    private let staticStream: PCollectionStream<Of>?

    public init(coder: Coder = .of(type: Of.self)!,
                type: StreamType = .unspecified,
                parent: PipelineTransform? = nil,
                consumers: [PipelineTransform] = [],
                stream: PCollectionStream<Of>? = nil)
    {
        self.coder = coder
        self.consumers = consumers
        self.parent = parent
        staticStream = stream
        streamType = type
    }

    public var stream: PCollectionStream<Of> {
        staticStream ?? PCollectionStream<Of>()
    }

    @discardableResult
    public func apply(_ transform: PipelineTransform) -> PipelineTransform {
        consumers.append(transform)
        return transform
    }

    func parent(_ transform: PipelineTransform) {
        parent = transform
    }
}

public typealias PipelineRoot = PCollection<Never>

extension PCollection: PipelineMember {
    var roots: [PipelineRoot] {
        if let p = parent {
            return p.roots
        } else if let p = self as? PipelineRoot {
            return [p]
        } else {
            return []
        }
    }
}

public extension PCollection {
    static func empty() -> PCollection<Of> {
        PCollection<Of>()
    }
}
