
import Logging

/// A higher level interface to SerializableFn using dependency injected dynamic properties in the same
/// way as we define Composite PTransforms
public protocol DoFn {
    func process() async throws
    func finishBundle() async throws
}

public extension DoFn {
    func finishBundle() async throws { }
}


