public struct TuplePTransform<T>: _PrimitivePTransform {
    public let value: T
    let _children: [AnyPTransform]
    
    public init(value: T){
        self.value = value
        _children = []
    }
    
    public init(value: T,children: [AnyPTransform]){
        self.value = value
        _children = children
    }
    
    init<T0:PTransform,T1:PTransform>(_ t0: T0,_ t1:T1) where T == (T0,T1) {
        value = (t0,t1)
        _children = [AnyPTransform(t0),AnyPTransform(t1)]
    }
    
}
