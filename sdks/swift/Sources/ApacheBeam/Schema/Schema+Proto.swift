import Foundation

extension Field {
    var proto: FieldProto {
        get throws {
            try .with {
                $0.name = self.name
                $0.description_p = self.description ?? ""
                if let p = self.position {
                    $0.encodingPosition = Int32(p)
                }
                $0.type = try self.type.proto
            }
        }
    }
    
    
    static func from(_ proto: FieldProto) -> Field {
        Field(name:proto.name,description: proto.description_p,type:.from(proto.type), options:[:], position: Int(proto.encodingPosition))
    }
}

extension Schema {
    var proto: SchemaProto {
        get throws {
            .with { _ in }
        }
    }
    
    static func from(_ proto: SchemaProto) -> Schema {
        let fields:[Field] = proto.fields.map {
            .from($0)
        }
        return Schema(id:UUID(uuidString: proto.id)!,options:[:],fields:fields)
    }
}
