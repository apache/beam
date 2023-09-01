import Foundation

public extension FieldType {
    func decode<O:FixedWidthInteger>(_ type: O.Type,from: inout Data) throws -> O? {
        switch self {
        case .byte:
            return O(try from.next(UInt8.self))
        case .int16:
            return O(try from.next(UInt16.self))
        case .int32:
            return O(try from.varint()) // TODO: Maybe actually check width?
        case .int64:
            return O(try from.varint())
        default:
            return nil
        }
    }
    
    func decode<O:FloatingPoint>(_ type: O.Type,from: inout Data) throws -> O? {
        switch self {
        case .float,.double:
            return try from.next(type)
        default:
            return nil
        }
    }

    func discard(from: inout Data) throws {
        switch self {
        case .unspecified:
            return
        case .byte:
            from = from.safeAdvance(by: MemoryLayout<UInt8>.size)
        case .int16:
            from = from.safeAdvance(by: MemoryLayout<UInt16>.size)
        case .int32:
            _ = try from.varint()
        case .int64:
            _ = try from.varint()
        case .float:
            from = from.safeAdvance(by: MemoryLayout<Float>.size)
        case .double:
            from = from.safeAdvance(by: MemoryLayout<Double>.size)
        case .string:
            let len = try from.varint()
            from = from.safeAdvance(by: len)
        case .datetime:
            from = from.safeAdvance(by: MemoryLayout<Int64>.size)
        case .boolean:
            from = from.safeAdvance(by: MemoryLayout<UInt8>.size)
        case .bytes:
            let len = try from.varint()
            from = from.safeAdvance(by: len)
        case .decimal(_, _):
            _ = try from.varint()
            _ = try from.varint()
        case .logical(_,let schema):
            for field in schema.fields {
                try field.type.discard(from:&from)
            }
        case .row(let schema):
            for field in schema.fields {
                try field.type.discard(from:&from)
            }
        case .array(let base):
            let size = try from.varint()
            for _ in 0..<size {
                _ = try base.discard(from:&from)
            }
        case .repeated(let base):
            let size = try from.varint()
            for _ in 0..<size {
                _ = try base.discard(from:&from)
            }
        case .map(let k, let v):
            let size = try from.varint()
            for _ in 0..<size {
                _ = try k.discard(from:&from)
                _ = try v.discard(from:&from)
            }
        case .nullable(let base):
            try base.discard(from:&from)
        }
    }
}
