import Foundation


public struct Field {
    let name: String
    let description: String?
    let type: FieldType
    var options: [String:(FieldType,FieldValue)]
    let position: Int?
}

public class FieldsBuilder {
    var fields: [Field] = []
}


public struct Schema {
    let id: UUID
    let fields: [Field]
    let options: [String:(FieldType,FieldValue)]
    
    public init(id:UUID = UUID(),options:[String:(FieldType,FieldValue)] = [:],fields:[Field]) {
        self.id = id
        self.options = options
        self.fields = fields
    }
    
    public init(id:UUID = UUID(),options:[String:(FieldType,FieldValue)] = [:],_ fn: (inout FieldsBuilder) -> Void) {
        var builder = FieldsBuilder()
        fn(&builder)
        self = .init(id:id,options:options,fields:builder.fields)
    }
    
    
    public func row(_ populator: (inout FieldValue) -> Void) -> FieldValue {
        return FieldValue(self,populator)
    }
}

//TODO: Make macros to autogenerate these
/// Scalar value convience functions
public extension FieldsBuilder {
    @discardableResult
    func byte(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.byte,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func int16(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.int16,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func int32(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.int32,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func int64(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.int64,
                            options:[:],
                            position:fields.count))
        return self
    }
   
    @discardableResult
    func float(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.float,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func double(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.double,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func string(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.string,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func datetime(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.datetime,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func boolean(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.boolean,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func bytes(_ name:String,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.bytes,
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func decimal(_ name:String,_ precision: Int,_ scale: Int,description:String? = nil) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.decimal(precision,scale),
                            options:[:],
                            position:fields.count))
        return self
    }
    
    @discardableResult
    func row(_ name:String,description:String? = nil,_ fn: (inout FieldsBuilder) -> Void) -> FieldsBuilder {
        fields.append(Field(name:name,
                            description:description,
                            type:.row(Schema() { fn(&$0) }),
                            options:[:],
                            position:fields.count))
        return self
    }
}

