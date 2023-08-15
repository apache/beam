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
