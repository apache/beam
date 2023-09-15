extension FieldType {
    
    var proto: Org_Apache_Beam_Model_Pipeline_V1_FieldType {
        get throws {
            try self.proto()
        }
    }
     
    func proto(_ nullable: Bool = false) throws -> Org_Apache_Beam_Model_Pipeline_V1_FieldType {
        switch self {
        case .unspecified:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .byte:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .int16:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .int32:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .int64:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .float:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .double:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .string:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .datetime:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .boolean:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .bytes:
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .decimal(_, _):
            return .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .logical(_, _):
            throw ApacheBeamError.runtimeError("Logical types not specified yet")
        case .row(let schema):
                return try .with {
                    $0.nullable = nullable
                    $0.rowType = try .with {
                        $0.schema = try schema.proto
                    }
                }
        case .nullable(let base):
            return try base.proto(true)
        case .array(let type):
            return try .with {
                $0.nullable = nullable
                $0.arrayType = try .with {
                    $0.elementType = try type.proto
                }
            }
        case .repeated(let type):
            return try .with {
                $0.nullable = nullable
                $0.iterableType = try .with {
                    $0.elementType = try type.proto
                }
            }
        case .map(let key, let value):
            return try .with {
                $0.nullable = nullable
                $0.mapType = try .with {
                    $0.keyType = try key.proto
                    $0.valueType = try value.proto
                }
            }
        }
    }
    
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
