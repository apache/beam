
/// Creating Static Values
public extension PCollection {

    /// Each time the input fires output all of the values in this list.
    func create<Value:Codable>(_ values: [Value],_ name:String = "\(#file):\(#line)") -> PCollection<Value> {
        return pardo(name,values) { values,input,output in
            for try await (_,ts,w) in input {
                for v in values {
                    output.emit(v,timestamp:ts,window:w)
                }
            }
        }
    }
}

/// Convenience logging mappers
public extension PCollection {
    func log(prefix:String,name:String = "\(#file):\(#line)") -> PCollection<Of> where Of == String {
        pardo(name,prefix) { prefix,input,output in
            for await element in input {
                print("\(prefix): \(element)")
                output.emit(element)
            }
        }
    }
}

/// Modifying Values
public extension PCollection {
    
    /// Modify a value without changing its window or timestamp
    func map<Out>(name:String = "\(#file):\(#line)",_ fn: @Sendable @escaping (Of) -> Out) -> PCollection<Out> {
        return pardo(name) { input,output in
            for try await (v,ts,w) in input {
                output.emit(fn(v),timestamp:ts,window:w)
            }
        }
    }
    
    func map<K,V>(name:String = "\(#file):\(#line)",_ fn: @Sendable @escaping (Of) -> (K,V)) -> PCollection<KV<K,V>> {
        return pardo(name) { input,output in
            for try await (i,ts,w) in input {
                let (key,value) = fn(i)
                output.emit(KV(key,value),timestamp:ts,window:w)
            }
        }
    }

    /// Produce multiple outputs as a single value without modifying window or timestamp
    func flatMap<Out>(name:String = "\(#file):\(#line)",_ fn: @Sendable @escaping (Of) -> [Out]) -> PCollection<Out> {
        return pardo(name) { input,output in
            for try await (v,ts,w) in input {
                for i in fn(v) {
                    output.emit(i,timestamp:ts,window:w)
                }
            }
        }
    }

}

public extension PCollection<Never> {
    /// Convenience function to add an impulse when we are at the root of the pipeline
    func create<Value:Codable>(_ values: [Value],_ name:String = "\(#file):\(#line)") -> PCollection<Value> {
        return impulse().create(values,name)
    }
}
