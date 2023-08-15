extension Array where Element == (String,String) {
    func dict() -> [String:String] {
        reduce(into:[:],{ $0[$1.0] = $1.1} )
    }
}
