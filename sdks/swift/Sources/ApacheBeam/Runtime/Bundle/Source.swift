import Foundation

/// Custom SerializableFn that reads/writes from an external data stream using a defined coder. It assumes that a given
/// data element might contain more than one coder
final class Source : SerializableFn {

    let client: DataplaneClient
    let coder: Coder
    
    public init(client: DataplaneClient,coder:Coder) {
        self.client = client
        self.coder = coder

    }
    
    
    func process(context: SerializableFnBundleContext,
                 inputs: [AnyPCollectionStream], outputs: [AnyPCollectionStream]) async throws -> (String, String) {
        let (stream,_) = await client.makeStream(instruction: context.instruction, transform: context.transform)
        for await message in stream {
            switch message {
            case let .data(data):
                var d = data
                while d.count > 0 {
                    let value = try coder.decode(&d)
                    for output in outputs {
                        try output.emit(value: value)
                    }
                }
            case let .last(id, transform):
                for output in outputs {
                    output.finish()
                }
                await client.finalizeStream(instruction: id, transform: transform)
                return (id,transform)
            //TODO: Handle timer messages
            default:
                break
            }
        }
        return (context.instruction,context.transform)
    }
}
