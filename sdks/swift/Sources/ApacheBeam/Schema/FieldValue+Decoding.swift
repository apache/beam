import Foundation

public extension FieldValue {

    static func from(data: inout Data,as type:FieldType) throws -> FieldValue? {
        switch type {
        case .unspecified:
            return .undefined
        case .byte,.int16,.int32,.int64:
            return .int(try data.varint(), type)
        case .float:
            return .float(Double(try data.next(Float.self)), type)
        case .double:
            return .float(try data.next(Double.self), type)
        case .string:
            return .string(String(data:try data.subdata(),encoding: .utf8)!)
        case .datetime:
            return .datetime(try data.instant())
        case .boolean:
            return .boolean(try data.next(UInt8.self) == 1)
        case .bytes:
            return .bytes(try data.subdata())
        case .decimal(_, _):
            throw ApacheBeamError.runtimeError("Decimal types not yet implemented")
        case .logical(_, _):
            throw ApacheBeamError.runtimeError("Logical Types not yet supported")
        case .row(let schema):
            
            let count = try data.varint()
            
            //Consume the null byte pattern
            let nullBytes = Int(ceil(Double(count) / 8.0))
            let nullBits = try data.subdata()
            data = data.advanced(by: nullBytes)
            
            var fields: [FieldValue] = (0..<count).map { i in
                let byte = Int(floor(Double(i) / 8.0))
                let bit = i % 8
                return (Int(nullBits[byte]) & bit) > 0 ? .null : .undefined
            }
            for field in schema.fields {
                //TODO: Null check
                fields.append(try .from(data: &data, as: field.type)!)
            }
            return .row(schema, fields)
        case .nullable(let baseType):
            return try .from(data: &data, as: baseType)
        case .array(_):
            let count = Int(try data.next(Int32.self))
            if count < 1 {
                throw ApacheBeamError.runtimeError("Indeterminate length not implement yet.")
            }
            return try .array((0..<count).map { _ in try .from(data: &data,as:type)! })
        case .repeated(let type):
            let count = Int(try data.next(Int32.self))
            if count < 1 {
                throw ApacheBeamError.runtimeError("Indeterminate length not implement yet.")
            }
            return try .repeated((0..<count).map { _ in try .from(data: &data,as:type)! })
        case .map(let key, let value):
            let count = Int(try data.next(UInt32.self))
            return try .map((0..<count).map { _ in
                (try .from(data: &data,as: key)!, try .from(data: &data,as: value)!)
            })
        }
    }
}
