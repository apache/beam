import DuckDB

public struct DuckDB {
    public enum ExternalTable : Codable {
        case csv(String,String)
        case json(String,String)
        case parquet(String,String)
    }
    
    var tables: [ExternalTable]
}
