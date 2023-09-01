extension FieldValue {
    static func from(_ proto: FieldValueProto) throws -> FieldValue {
        switch proto.fieldValue {
        case .atomicValue(let value):
            switch value.value {
            case .boolean(let b):
                return .boolean(b)
            case .byte(let b):
                return .int(Int(b), .byte)
            case .bytes(let d):
                return .bytes(d)
            case .double(let d):
                return .float(d, .double)
            case .float(let f):
                return .float(Double(f), .float)
            case .int16(let i):
                return .int(Int(i), .int16)
            case .int32(let i):
                return .int(Int(i), .int32)
            case .int64(let i):
                return .int(Int(i), .int64)
            case .string(let s):
                return .string(s)
            default:
                return .null
            }
        case .arrayValue(let arrayValue):
            return try .array(arrayValue.element.map { try .from($0) })
        case .mapValue(let mapValue):
            return try .map(mapValue.entries.map({
                (try .from($0.key),try .from($0.value))
            }))
        default:
            return .null
        }
    }
}
