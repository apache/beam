import Logging

public protocol DynamicProperty { }

@propertyWrapper
public struct PInput<Of> : DynamicProperty {
    public var wrappedValue: PCollectionStream<Of>
    
    public init(wrappedValue: PCollectionStream<Of> = .init()) {
        self.wrappedValue = wrappedValue
    }
}

@propertyWrapper
public struct POutput<Of> : DynamicProperty {
    public var wrappedValue: PCollectionStream<Of>
    public init(wrappedValue: PCollectionStream<Of> = .init()) {
        self.wrappedValue = wrappedValue
    }
}

@propertyWrapper
public struct Logger : DynamicProperty {
    public var wrappedValue: Logging.Logger
    public init(wrappedValue: Logging.Logger = Logging.Logger(label: "TEST")) {
        self.wrappedValue = wrappedValue
    }
}

@propertyWrapper
public struct Serialized<Value:Codable> : DynamicProperty {
    public var wrappedValue: Value?
    public init(wrappedValue: Value? = nil) {
        self.wrappedValue = wrappedValue
    }
}
