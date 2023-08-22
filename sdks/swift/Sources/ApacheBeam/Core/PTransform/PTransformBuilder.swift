public struct EmptyPTransform : _PrimitivePTransform {
    public init() { }
}

public struct _ConditionalPTransform<TruePTransform,FalsePTransform> : _PrimitivePTransform
    where TruePTransform : PTransform, FalsePTransform: PTransform 
{
    enum Storage {
        case trueTransform(TruePTransform)
        case falseTransform(FalsePTransform)
    }
    let storage: Storage
}

extension _ConditionalPTransform {
    public var children: [AnyPTransform] {
        switch storage {
            
        case .trueTransform(let transform):
            return [AnyPTransform(transform)]
        case .falseTransform(let transform):
            return [AnyPTransform(transform)]
        }
    }
}

@resultBuilder
public struct PTransformBuilder {
    
    public static func buildBlock() -> EmptyPTransform {
        EmptyPTransform()
    }
    
    public static func buildBlock<Transform>(_ transform: Transform) -> Transform where Transform: PTransform {
        transform
    }
    
    public static func buildEither<TrueT,FalseT>(first: TrueT) -> _ConditionalPTransform<TrueT,FalseT> where TrueT: PTransform, FalseT: PTransform {
        .init(storage: .trueTransform(first))
    }

    public static func buildEither<TrueT,FalseT>(second: FalseT) -> _ConditionalPTransform<TrueT,FalseT> where TrueT: PTransform, FalseT: PTransform {
        .init(storage: .falseTransform(second))
    }
}

public extension PTransformBuilder {
    static func buildBlock<T0,T1>(_ t0: T0,_ t1: T1) -> TuplePTransform<(T0,T1)> where T0: PTransform,T1: PTransform {
        TuplePTransform<(T0,T1)>(t0,t1)
    }
}
