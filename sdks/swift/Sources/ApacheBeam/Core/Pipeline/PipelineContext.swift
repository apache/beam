public final class PipelineContext {

    var proto : PipelineProto
    let defaultEnvironmentId: String
    let collections: [String:AnyPCollection]
    let pardoFns: [String:SerializableFn]

    init(_ proto:PipelineProto,_ defaultEnvironmentId: String,_ collections: [String:AnyPCollection],_ fns:[String:SerializableFn]) {
        self.proto = proto
        self.defaultEnvironmentId = defaultEnvironmentId
        self.collections = collections
        self.pardoFns = fns
    }
    
}
