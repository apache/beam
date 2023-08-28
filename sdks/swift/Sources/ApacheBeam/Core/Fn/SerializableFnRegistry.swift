import Foundation

public final class SerializableFnRegistry {
    private init() { }
    private var registry: [String:SerializableFn] = [:]
}


extension SerializableFnRegistry {
    func register<T:SerializableFn>(_ object: T, id: String = UUID().uuidString) -> String {
        registry[id] = object
        return id
    }
}
