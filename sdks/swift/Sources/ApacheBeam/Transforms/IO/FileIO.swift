import Foundation

public protocol FileIOSource {
    static func readFiles(matching:PCollection<KV<String,String>>) -> PCollection<Data>
    static func listFiles(matching:PCollection<KV<String,String>>) -> PCollection<KV<String,String>>
}

public extension PCollection<KV<String,String>> {
    func readFiles<Source:FileIOSource>(in source:Source.Type) -> PCollection<Data> {
        Source.readFiles(matching:self)
    }
    
    /// Takes a KV pair of (bucket,prefix) and returns a list of (bucket,filename)
    func listFiles<Source:FileIOSource>(in source:Source.Type) -> PCollection<KV<String,String>> {
        Source.listFiles(matching:self)
    }
}
