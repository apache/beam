import Foundation

final class Sink : SerializableFn {
    let client: DataplaneClient
    let coder: Coder
    
    public init(client: DataplaneClient,coder:Coder) {
        self.client = client
        self.coder = coder

    }
    
    
    func process(context: SerializableFnBundleContext,
                 inputs: [AnyPCollectionStream], outputs: [AnyPCollectionStream]) async throws -> (String, String) {
        let (_,emitter) = await client.makeStream(instruction: context.instruction, transform: context.transform)
        for try await element in inputs[0] {
            var output = Data()
            try coder.encode(element, data: &output)
            emitter.yield(.data(output))
        }
        emitter.yield(.last(context.instruction, context.transform))
        emitter.finish()
        return (context.instruction,context.transform)
    }
}
