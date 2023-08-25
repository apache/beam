import Foundation

public extension PCollection {
    
    /// Create a PCollection whose stream has been preloaded with some values for testing
    static func testValues<V:Beamable>(_ values:[V]) -> PCollection<V> {
        let stream = PCollectionStream<V>()
        for v in values {
            stream.emit(v,timestamp:.now,window:.global)
        }
        return PCollection<V>(stream:stream)
    }
    
    /// Convenience function that simulates an impulse
    static func testImpulse() -> PCollection<Data> {
        testValues([Data()])
    }
    
}
