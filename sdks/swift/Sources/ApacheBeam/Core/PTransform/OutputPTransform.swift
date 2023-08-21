public struct OutputPTransform<Of> : _PrimitivePTransform {
    let name: String
    let applyClosure: (Any) -> PCollection<Of>

    public init<In>(_ name:String,_ fn: @escaping (PCollection<In>) -> PCollection<Of>) {
        self.name = name
        self.applyClosure = { fn($0 as! PCollection<In>) }
    }
    
}
