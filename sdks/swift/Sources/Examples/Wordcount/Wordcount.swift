import ApacheBeam
import ArgumentParser

@main
public struct Wordcount : PipelineCommand {

    @Argument
    var path: String?
    
    public var pipeline : Pipeline {
        Pipeline { pipeline in
            let contents = pipeline
                .create([path!])
                .map({ value in
                    let parts = value.split(separator: "/",maxSplits: 1)
                    print("Got filename \(parts) from \(value)")
                    return KV(parts[0].lowercased(),parts[1].lowercased())
                })
                .listFiles(in: GoogleStorage.self)
                .readFiles(in: GoogleStorage.self)
            
            // Simple ParDo that takes advantage of enumerateLines. No name to test name generation of pardos
            let lines = contents.pstream { contents,lines in
                for await (content,ts,w) in contents {
                    String(data:content,encoding:.utf8)!.enumerateLines { line,_ in
                        lines.emit(line,timestamp:ts,window:w)
                    }
                }
            }
            
            // Our first group by operation
            let baseCount = lines
                .flatMap({ (line:String) in line.components(separatedBy: .whitespaces) })
                .groupBy({ ($0,1) })
                .sum()
            
            let normalizedCounts = baseCount.groupBy {
                ($0.key.lowercased().trimmingCharacters(in: .punctuationCharacters),
                 $0.value ?? 1)
            }.sum()
            
            normalizedCounts.log(prefix:"COUNT OUTPUT")
            
        }
    }
    
    public init() { }

}
