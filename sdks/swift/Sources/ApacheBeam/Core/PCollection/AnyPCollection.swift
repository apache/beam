public struct AnyPCollection : PCollectionProtocol {
    
    
    
    let type: Any.Type
    let ofType: Any.Type
    let collection: Any
    
    let applyClosure: (Any,PipelineTransform) -> Void
    let consumersClosure: (Any) -> [PipelineTransform]
    let coderClosure: (Any) -> Coder
    let streamClosure: (Any) -> AnyPCollectionStream
    
    public init<C>(_ collection: C) where C : PCollectionProtocol {
        if let anyCollection = collection as? AnyPCollection {
            self = anyCollection
        } else {
            self.type = C.self
            self.ofType = C.Of.self
            self.collection = collection
            
            self.applyClosure = { ($0 as! C).apply($1) }
            self.consumersClosure = { ($0 as! C).consumers }
            self.coderClosure = { ($0 as! C).coder }
            self.streamClosure = { AnyPCollectionStream(($0 as! C).stream) }
        }
    }
    
    
    public var consumers: [PipelineTransform] {
        consumersClosure(collection)
    }
    
    public func apply(_ transform: PipelineTransform) {
        applyClosure(collection,transform)
    }

    
    public var coder: Coder {
        coderClosure(collection)
    }

    public var stream: PCollectionStream<Never> {
        fatalError("Do not use `stream` on AnyPCollection. Use `anyStream` instead.")
    }
    
    public var anyStream: AnyPCollectionStream {
        streamClosure(collection)
    }
    
}
