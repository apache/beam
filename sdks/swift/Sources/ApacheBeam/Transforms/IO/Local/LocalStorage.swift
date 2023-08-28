import Foundation

public struct LocalStorage : FileIOSource {
    
    public static func readFiles(matching: PCollection<KV<String, String>>) -> PCollection<Data> {
        matching.pstream(type:.bounded) { input,output in
            let fm = FileManager.default
            for try await (dirAndFile,ts,w) in input {
                for file in dirAndFile.values {
                    output.emit(fm.contents(atPath: file)!,timestamp:ts,window:w)
                }
            }
        }
    }
    
    public static func listFiles(matching: PCollection<KV<String, String>>) -> PCollection<KV<String, String>> {
        matching.pstream(type:.bounded) { input,output in
            let fm = FileManager.default
            for try await (pathAndPattern,ts,w) in input {
                let path = pathAndPattern.key
                let patterns = try pathAndPattern.values.map { try Regex($0) }
                
                for file in try FileManager.default.contentsOfDirectory(atPath: path) {
                    for p in patterns {
                        do {
                            if try p.firstMatch(in:file) != nil {
                                output.emit(KV(path,file),timestamp:ts,window:w)
                                break
                            }
                        } catch {
                            
                        }
                    }
                }
            }
        }
    }
    
    
}
