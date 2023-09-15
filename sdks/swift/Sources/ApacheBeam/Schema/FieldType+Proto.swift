/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 *  License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an  AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extension FieldType {
    var proto: Org_Apache_Beam_Model_Pipeline_V1_FieldType {
        get throws {
            try self.proto()
        }
    }

    func proto(_ nullable: Bool = false) throws -> Org_Apache_Beam_Model_Pipeline_V1_FieldType {
        switch self {
        case .unspecified:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .byte:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .int16:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .int32:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .int64:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .float:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .double:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .string:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .datetime:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .boolean:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .bytes:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .decimal:
            .with {
                $0.nullable = nullable
                $0.atomicType = .unspecified
            }
        case .logical:
            throw ApacheBeamError.runtimeError("Logical types not specified yet")
        case let .row(schema):
            try .with {
                $0.nullable = nullable
                $0.rowType = try .with {
                    $0.schema = try schema.proto
                }
            }
        case let .nullable(base):
            try base.proto(true)
        case let .array(type):
            try .with {
                $0.nullable = nullable
                $0.arrayType = try .with {
                    $0.elementType = try type.proto
                }
            }
        case let .repeated(type):
            try .with {
                $0.nullable = nullable
                $0.iterableType = try .with {
                    $0.elementType = try type.proto
                }
            }
        case let .map(key, value):
            try .with {
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
        case let .atomicType(type):
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
        case let .rowType(type):
            .row(.from(type.schema))
        case let .mapType(type):
            .map(.from(type.keyType), .from(type.valueType))
        case let .iterableType(type):
            .repeated(.from(type.elementType))
        case let .arrayType(type):
            .array(.from(type.elementType))
        default:
            .unspecified
        }
        return proto.nullable ? .nullable(baseType) : baseType
    }
}
