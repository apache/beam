public extension Dictionary where Key:Comparable {
    /// Return key-value pairs sorted by key. 
    func sorted() -> [(Key,Value)] {
        self.map({ ($0,$1)}).sorted(by: { $0.0 < $1.0 })
    }
}
