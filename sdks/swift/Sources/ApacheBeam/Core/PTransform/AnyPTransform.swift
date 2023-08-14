public struct AnyPTransform : _PrimitivePTransform {
    let type: Any.Type
    var transform: Any
    
    let expandClosure: (Any) -> AnyPTransform
    let expansionType: Any.Type
    
    
    public init<T>(_ transform: T) where T: PTransform {
        if let anyTransform = transform as? AnyPTransform {
            self = anyTransform
        } else {
            self.type = T.self
            self.expansionType = T.Expansion.self
            self.transform = transform
            self.expandClosure = { AnyPTransform(($0 as! T).expand) }
        }
    }
}

extension AnyPTransform : ParentPTransform {
    public var children: [AnyPTransform] {
        (transform as? ParentPTransform)?.children ?? []
    }
}
