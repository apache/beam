/// Basic grouping functionality
///
public extension PCollection {
    func groupBy<K,V>(name: String = "\(#file):\(#line)",_ fn: @Sendable @escaping (Of) -> (K,V)) -> PCollection<KV<K,V>> {
        return map(name:name,fn)
            .groupByKey()
    }
}
