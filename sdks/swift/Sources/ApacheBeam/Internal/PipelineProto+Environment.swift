extension PipelineProto {
    
    mutating func environment(from: Environment) throws -> PipelineComponent {
        return try environment { _ in
            try .with {
                try from.populate(&$0)
            }
        }
    }
}
