extension PipelineProto {
    
    mutating func environment(from: Environment) throws -> PipelineComponent {
        return try environment { _ in
            try .with { p in
                p.urn = switch from.category {
                case .docker(_): .beamUrn("docker",type:"env")
                case .system: .beamUrn("default",type:"env")
                case .process(_, _, _, _): .beamUrn("process",type:"env")
                case .external(_): .beamUrn("external",type:"env")
                }
                p.capabilities = from.capabilities
                p.dependencies = from.dependencies.map({ $0.proto })
                
                if case let .docker(containerImage) = from.category {
                    p.payload = try Org_Apache_Beam_Model_Pipeline_V1_DockerPayload.with {
                        $0.containerImage = containerImage
                    }.serializedData()
                }
                
                if case let .external(endpoint) = from.category {
                    p.payload = try Org_Apache_Beam_Model_Pipeline_V1_ExternalPayload.with {
                        $0.endpoint = endpoint.proto
                    }.serializedData()
                }
                
                if case let .process(command,arch,os,env) = from.category {
                    p.payload = try Org_Apache_Beam_Model_Pipeline_V1_ProcessPayload.with {
                        $0.arch = arch
                        $0.command = command
                        $0.os = os
                        $0.env = env
                    }.serializedData()
                }
            }
        }
    }
}
