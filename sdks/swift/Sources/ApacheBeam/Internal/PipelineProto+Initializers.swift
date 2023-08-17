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

enum PipelineComponent {
    case none
    case transform(String,PTransformProto)
    case collection(String,PCollectionProto)
    case coder(String,CoderProto)
    case windowingStrategy(String,WindowingStrategyProto)
    case environment(String,EnvironmentProto)
    
    var name: String {
        switch self {
        case .none:
            fatalError("PipelineComponent not properly initialized")
        case .transform(let n, _):
            return n
        case .collection(let n, _):
            return n
        case .coder(let n, _):
            return n
        case .windowingStrategy(let n, _):
            return n
        case .environment(let n, _):
            return n
        }

    }
    
    var transform: PTransformProto? {
        if case .transform(_, let pTransformProto) = self {
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
        let name = "ref_PTransform_\(self.components.transforms.count+1)"
        let proto = try mapper(name)
        self.components.transforms[name] = proto
        return .transform(name,proto)
    }
    
    mutating func collection(_ mapper: @escaping (String) -> PCollectionProto) -> PipelineComponent {
        let name  = "ref_PCollection_\(self.components.pcollections.count+1)"
        let proto = mapper(name)
        self.components.pcollections[name] = proto
        return .collection(name, proto)
    }
    
    mutating func coder(_ mapper: @escaping (String) -> CoderProto) -> PipelineComponent {
        let name = "ref_Coder_\(self.components.coders.count+1)"
        let proto = mapper(name)
        self.components.coders[name] = proto
        return .coder(name, proto)
    }
    
    mutating func windowingStrategy(_ mapper: @escaping (String) -> WindowingStrategyProto) -> PipelineComponent {
        let name = "ref_WindowingStrategy_\(self.components.coders.count+1)"
        let proto = mapper(name)
        self.components.windowingStrategies[name] = proto
        return .windowingStrategy(name, proto)
    }
    
    mutating func environment(_ mapper: @escaping (String) throws -> EnvironmentProto) throws -> PipelineComponent {
        let name = "ref_Environment_\(self.components.coders.count+1)"
        let proto = try mapper(name)
        self.components.environments[name] = proto
        return .environment(name, proto)
    }
    
}
