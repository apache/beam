public final class PipelineContext {

    var proto : PipelineProto
    let defaultEnvironmentId: String

    init(_ proto:PipelineProto,_ defaultEnvironmentId: String) {
        self.proto = proto
        self.defaultEnvironmentId = defaultEnvironmentId
    }
    
}
