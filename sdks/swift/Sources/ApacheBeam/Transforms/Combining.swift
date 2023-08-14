/// Basic reducers
public extension PCollection {
    func reduce<Result:Codable,K,V>(name:String = "\(#file):\(#line)",into:Result,_ accumulator: @Sendable @escaping (V,inout Result) -> Void) -> PCollection<KV<K,Result>> where Of == KV<K,V> {
        return pardo(name,into) { initialValue,input,output in
            for await (kv,ts,w) in input {
                var result = initialValue
                for v in kv.values {
                    accumulator(v,&result)
                }
                output.emit(KV(kv.key,result),timestamp:ts,window:w)
            }
                
        }
    }
}

/// Convenience functions
public extension PCollection {
    func sum<K,V:Numeric&Codable>() -> PCollection<KV<K,V>> where Of == KV<K,V> {
        return reduce(into: 0,{ a,b in b = b + a })
    }
}
