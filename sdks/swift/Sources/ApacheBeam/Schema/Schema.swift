
public indirect enum FieldType {
    case byte,int16,int32,int64,float,double,string,datetime,boolean,bytes
    case decimal(Int,Int)
    
    case logical(Schema)
    case row(Schema)
    
    case array(FieldType)
    case repeated(FieldType)
    case map(FieldType,FieldType)
}

public struct Field {
    let name: String
    let type: FieldType
}

public struct Schema {
    
}
