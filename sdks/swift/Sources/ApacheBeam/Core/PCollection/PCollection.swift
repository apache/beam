public protocol PCollectionProtocol {
    associatedtype Of
    
    typealias Stream = PCollectionStream<Of>
    
    var consumers: [PipelineTransform] { get }
    var coder: Coder { get }
    var stream: Stream { get }
    
    func apply(_ transform: PipelineTransform)
}


public final class PCollection<Of> : PCollectionProtocol {

    public let coder: Coder
    public var consumers: [PipelineTransform]
    
    public init(coder: Coder = .of(type: Of.self)!,consumers:[PipelineTransform] = []) {
        self.coder = coder
        self.consumers = consumers
    }

    public var stream: PCollectionStream<Of> {
        return PCollectionStream<Of>()
    }
    
    public func apply(_ transform: PipelineTransform) {
        consumers.append(transform)
    }
    
}
