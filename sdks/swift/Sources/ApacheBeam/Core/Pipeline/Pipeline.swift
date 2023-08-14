import GRPC
import Logging

public final class Pipeline {
    let content: (inout PCollection<Never>) -> Void
    let log: Logging.Logger
    
    public init(log: Logging.Logger = .init(label:"Pipeline"),_ content: @escaping (inout PCollection<Never>) -> Void) {
        self.log = log
        self.content = content
    }
    
    
}
