extension PipelineProto {
    
    mutating func coder(from: Coder) -> PipelineComponent {
        let componentCoders:[String] = switch from {
        case let .keyvalue(keyCoder, valueCoder):
            [coder(from:keyCoder).name,coder(from:valueCoder).name]
        case let .iterable(valueCoder):
            [coder(from:valueCoder).name]
        case let .lengthprefix(valueCoder):
            [coder(from:valueCoder).name]
        case let .windowedvalue(valueCoder, windowCoder):
            [coder(from:valueCoder).name,coder(from:windowCoder).name]
        default:
            []
        }
        return coder { _ in
                .with {
                    $0.spec = .with {
                        $0.urn = from.urn
                        if case .custom(let data) = from {
                            $0.payload = data
                        }
                    }
                    $0.componentCoderIds = componentCoders
                }
        }
    }
}
