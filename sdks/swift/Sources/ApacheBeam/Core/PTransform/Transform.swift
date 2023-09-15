/// Groups transforms together. Does not expose any of the pcollections are accessible outputs
public struct Transform<Subtransform> {
    let subtransform: Subtransform
    public init(@PTransformBuilder subtransform: () -> Subtransform) {
        self.subtransform = subtransform()
    }
}
