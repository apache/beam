import Logging

/// Test harness for PCollections. Primarily designed for unit testing.
public struct PCollectionTest {
    let output: AnyPCollection
    let fn: (Logger,[AnyPCollectionStream],[AnyPCollectionStream]) async throws -> Void
    
    public init<Of>(_ collection: PCollection<Of>,_ fn: @escaping (Logger,[AnyPCollectionStream],[AnyPCollectionStream]) async throws -> Void) {
        self.output = AnyPCollection(collection)
        self.fn = fn
    }
    
    public func run() async throws {
        let log = Logger(label:"Test")
        if let transform = output.parent {
            switch transform {
            case let .pardo(parent,_,fn,outputs):
                let context = SerializableFnBundleContext(instruction:"1",transform:"test",payload:try fn.payload,log: log)
                let input = parent.anyStream
                let streams = outputs.map({$0.anyStream})
                
                try await withThrowingTaskGroup(of: Void.self) { group in
                    log.info("Starting process task")
                    group.addTask {
                        _ = try await fn.process(context: context, inputs:[input], outputs: streams)
                    }
                    log.info("Calling Stream")
                    group.addTask {
                        try await self.fn(log,[input],streams)
                    }
                    for try await _ in group {
                    }
                }
            default:
                throw ApacheBeamError.runtimeError("Not able to test this type of transform \(transform)")
            }
        } else {
            throw ApacheBeamError.runtimeError("Unable to determine parent transform to test")
        }
    }
    
    
}
