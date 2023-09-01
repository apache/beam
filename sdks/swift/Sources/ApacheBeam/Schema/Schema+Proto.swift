import Foundation

extension Field {
    static func from(_ proto: FieldProto) -> Field {
        Field(name:proto.name,description: proto.description_p,type:.from(proto.type), options:[:], position: Int(proto.encodingPosition))
    }
}

extension Schema {
    static func from(_ proto: SchemaProto) -> Schema {
        let fields:[Field] = proto.fields.map {
            .from($0)
        }
        return Schema(id:UUID(uuidString: proto.id)!,options:[:],fields:fields)
    }
}
