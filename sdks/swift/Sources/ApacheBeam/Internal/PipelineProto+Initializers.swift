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

enum PipelineComponent {
    case none
    case transform(String, PTransformProto)
    case collection(String, PCollectionProto)
    case coder(String, CoderProto)
    case windowingStrategy(String, WindowingStrategyProto)
    case environment(String, EnvironmentProto)

    var name: String {
        switch self {
        case .none:
            fatalError("PipelineComponent not properly initialized")
        case let .transform(n, _):
            n
        case let .collection(n, _):
            n
        case let .coder(n, _):
            n
        case let .windowingStrategy(n, _):
            n
        case let .environment(n, _):
            n
        }
    }

    var transform: PTransformProto? {
        if case let .transform(_, pTransformProto) = self {
            return pTransformProto
        } else {
            return nil
        }
    }
}

/// Convenience function for creating new pipeline elements. Note that these shouldn't be accessed concurrently
/// but this isn't a problem itself since trying to access the proto concurrently throws an error.
extension PipelineProto {
    mutating func transform(_ mapper: @escaping (String) throws -> PTransformProto) throws -> PipelineComponent {
        let name = "ref_PTransform_\(components.transforms.count + 1)"
        let proto = try mapper(name)
        components.transforms[name] = proto
        return .transform(name, proto)
    }

    mutating func collection(_ mapper: @escaping (String) throws -> PCollectionProto) throws -> PipelineComponent {
        let name = "ref_PCollection_\(components.pcollections.count + 1)"
        let proto = try mapper(name)
        components.pcollections[name] = proto
        return .collection(name, proto)
    }

    mutating func coder(_ mapper: @escaping (String) -> CoderProto) -> PipelineComponent {
        let name = "ref_Coder_\(components.coders.count + 1)"
        let proto = mapper(name)
        components.coders[name] = proto
        return .coder(name, proto)
    }

    mutating func windowingStrategy(_ mapper: @escaping (String) -> WindowingStrategyProto) -> PipelineComponent {
        let name = "ref_WindowingStrategy_\(components.coders.count + 1)"
        let proto = mapper(name)
        components.windowingStrategies[name] = proto
        return .windowingStrategy(name, proto)
    }

    mutating func environment(_ mapper: @escaping (String) throws -> EnvironmentProto) throws -> PipelineComponent {
        let name = "ref_Environment_\(components.coders.count + 1)"
        let proto = try mapper(name)
        components.environments[name] = proto
        return .environment(name, proto)
    }
}
