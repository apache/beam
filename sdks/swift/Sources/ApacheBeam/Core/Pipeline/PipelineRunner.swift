public protocol PipelineRunner {
    func run(_ context: PipelineContext) async throws 
}
