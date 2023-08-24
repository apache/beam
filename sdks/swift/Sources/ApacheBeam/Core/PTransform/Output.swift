public struct NamedCollectionPTransform<Of> : _PrimitivePTransform {
    let name: String
    let collection: PCollection<Of>
}

/// Captures a PCollection and gives it a name so it can be used as an output
public struct Output<Of> : PTransform {
    let name: String
    let fn: () -> PCollection<Of>
    public init(_ name:String,_ fn: @escaping () -> PCollection<Of>) {
        self.name = name
        self.fn   = fn
    }
    public var expand: NamedCollectionPTransform<Of> {
        NamedCollectionPTransform(name: name, collection: fn())
    }
}

