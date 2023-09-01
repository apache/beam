extension FieldType {
    static func from(_ proto: Org_Apache_Beam_Model_Pipeline_V1_FieldType) -> FieldType {
        let baseType: FieldType = switch proto.typeInfo {
        case .atomicType(let type):
            switch type {
            case .byte:
                    .byte
            case .int16:
                    .int16
            case .int32:
                    .int32
            case .int64:
                    .int64
            case .float:
                    .float
            case .double:
                    .double
            case .string:
                    .string
            case .boolean:
                    .boolean
            case .bytes:
                    .bytes
            default:
                    .unspecified
            }
        case .rowType(let type):
                .row(.from(type.schema))
        case .mapType(let type):
                .map(.from(type.keyType),.from(type.valueType))
        case .iterableType(let type):
                .repeated(.from(type.elementType))
        case .arrayType(let type):
                .array(.from(type.elementType))
        default:
                .unspecified
        }
        return proto.nullable ? .nullable(baseType) : baseType
    }
}
