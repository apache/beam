import Logging
import GRPC
import NIOCore

public struct PortableRunner : PipelineRunner {
    let loopback:Bool
    let log: Logging.Logger
    let host: String
    let port: Int
    
    public init(host:String="localhost",port:Int=8073,loopback:Bool=false) {
        self.loopback = loopback
        self.log = .init(label: "PortableRunner")
        self.host = host
        self.port = port
    }
    
    public func run(_ context: PipelineContext) async throws {
        var proto = context.proto
        if loopback {
            //If we are in loopback mode we want to replace the default environment
            //with an external environment that points to our local worker server
            let worker = try WorkerServer(context.collections,context.pardoFns)
            log.info("Running in LOOPBACK mode with a worker server at \(worker.endpoint).")
            proto.components.environments[context.defaultEnvironmentId] = try .with {
                try Environment.external(worker.endpoint).populate(&$0)
            }
        }
        log.info("\(proto)")
        log.info("Connecting to Portable Runner at \(host):\(port).")
        let group = PlatformSupport.makeEventLoopGroup(loopCount: 1)
        let channel = try GRPCChannelPool.with(target: .host(host, port: port),
                                           transportSecurity: .plaintext, eventLoopGroup: group)
        let client = Org_Apache_Beam_Model_JobManagement_V1_JobServiceAsyncClient(channel: channel)
        let prepared = try await client.prepare(.with {
            $0.pipeline = proto
        })
        let job = try await client.run(.with {
            $0.preparationID = prepared.preparationID
        })
        log.info("Submitted job \(job.jobID)")
        var done = false
        while !done {
            let status = try await client.getState(.with {
                $0.jobID = job.jobID
            })
            log.info("Job \(job.jobID) status: \(status.state)")
            switch status.state {
            case .stopped,.failed,.done:
                done = true
            default:
                try await Task.sleep(for: .seconds(5))
            }
        }
        log.info("Job completed.")
        
    }
    
}
