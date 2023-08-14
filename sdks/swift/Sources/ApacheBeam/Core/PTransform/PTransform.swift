/// Represents a composite transform
public protocol PTransform {
    associatedtype Expansion: PTransform
    
    var expand: Expansion { get }
}

public extension Never {
    var expand: Never {
        fatalError()
    }
}

extension Never : PTransform { }

/// Represents PTransforms that can't be expanded further. When constructing the pipeline the expansion
/// happens until we hit this point
public protocol _PrimitivePTransform : PTransform where Expansion == Never { }
public extension _PrimitivePTransform {
    var expand: Never {
        neverExpand(String(reflecting: Self.self)) }
}


public protocol ParentPTransform {
    var children: [AnyPTransform] { get }
}

protocol GroupPTransform : ParentPTransform { }

public func neverExpand(_ type: String) -> Never {
    fatalError("\(type) is a primitive PTransform and cannot be expanded.")
}
