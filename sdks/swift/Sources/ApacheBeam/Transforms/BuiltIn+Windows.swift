public extension PCollection {
    func window(type:StreamType = .unspecified) -> PCollection<Of> {
        let output = PCollection<Of>(coder:coder,type:_resolve(self, type))
        //TODO: Implement windowing strategy changes
        return output
    }
}
