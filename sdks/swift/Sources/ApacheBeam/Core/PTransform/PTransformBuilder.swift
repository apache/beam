public struct EmptyPTransform : _PrimitivePTransform {
    
}




@resultBuilder
public struct PTransformBuilder {
    
    public static func buildBlock() -> EmptyPTransform {
        EmptyPTransform()
    }
    
    public static func buildBlock<Transform>(_ transform: Transform) -> Transform where Transform: PTransform {
        transform
    }
    
    
}
