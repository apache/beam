import Logging

import Foundation

struct BundleProcessor {
    let log: Logging.Logger
    
    struct Step {
        let transformId: String
        let fn: SerializableFn
        let inputs: [AnyPCollectionStream]
        let outputs: [AnyPCollectionStream]
        let payload: Data
    }
    
    let steps:[Step]
    
    init(id:String,
         descriptor:Org_Apache_Beam_Model_FnExecution_V1_ProcessBundleDescriptor,
         collections: [String:AnyPCollection],
         fns: [String:SerializableFn]) throws {
        self.log = Logging.Logger(label: "BundleProcessor(\(descriptor.id))")
        
        var temp: [Step] = []
        var coders =  BundleCoderContainer(bundle:descriptor)
        
        var streams: [String:AnyPCollectionStream] = [:]
        // First make streams for everything in this bundle (maybe I could use the pcollection array for this?)
        for (_,transform) in descriptor.transforms {
            for id in transform.inputs.values {
                if streams[id] == nil {
                    streams[id] = collections[id]!.anyStream
                }
            }
            for id in transform.outputs.values {
                if streams[id] == nil {
                    streams[id] = collections[id]!.anyStream
                }
            }
        }
        
        
        
        for (_,transform) in descriptor.transforms {
            let urn = transform.spec.urn
            //Map the input and output streams in the correct order
            let inputs = transform.inputs.sorted().map { streams[$0.1]! }
            let outputs = transform.outputs.sorted().map { streams[$0.1]! }
            if urn == "beam:runner:source:v1" {
                let remotePort = try RemoteGrpcPort(serializedData: transform.spec.payload)
                let coder = try Coder.of(name: remotePort.coderID, in: coders)
                temp.append(Step(
                    transformId: transform.uniqueName,
                    fn:Source(client: try .client(for: ApiServiceDescriptor(proto:remotePort.apiServiceDescriptor), worker: id), coder: coder),
                    inputs:inputs,
                    outputs:outputs,
                    payload:Data()
                ))
            } else if urn == "beam:runner:sink:v1" {
                let remotePort = try RemoteGrpcPort(serializedData: transform.spec.payload)
                let coder = try Coder.of(name: remotePort.coderID, in: coders)
                temp.append(Step(
                    transformId: transform.uniqueName,
                    fn:Sink(client: try .client(for: ApiServiceDescriptor(proto:remotePort.apiServiceDescriptor), worker: id), coder: coder),
                    inputs:inputs,
                    outputs:outputs,
                    payload:Data()
                ))

            } else if urn == "beam:transform:pardo:v1" {
                let pardoPayload = try Org_Apache_Beam_Model_Pipeline_V1_ParDoPayload(serializedData: transform.spec.payload)
                if let fn = fns[transform.uniqueName] {
                    temp.append(Step(transformId: transform.uniqueName,
                                     fn: fn,
                                     inputs: inputs,
                                     outputs: outputs,
                                     payload: pardoPayload.doFn.payload))
                } else {
                    log.warning("Unable to map \(transform.uniqueName) to a known SerializableFn. Will be skipped during processing.")
                }
            } else {
                log.warning("Unable to map \(urn). Will be skipped during processing.")
            }
            
        }
        self.steps = temp
    }
    
    public func process(instruction: String,responder: AsyncStream<Org_Apache_Beam_Model_FnExecution_V1_InstructionResponse>.Continuation) async {
        _ = await withThrowingTaskGroup(of: (String,String).self) { group in
            do {
                for step in self.steps {
                    let context = SerializableFnBundleContext(instruction: instruction, transform: step.transformId, payload: step.payload, log: self.log)
                    group.addTask {
                        return try await step.fn.process(context: context, inputs: step.inputs, outputs: step.outputs)
                    }
                }
                for try await (instruction,transform) in group {
                    log.info("Task Completed (\(instruction),\(transform))")
                }
                responder.yield(.with {
                    $0.instructionID = instruction
                })
            } catch {
                responder.yield(.with {
                    $0.instructionID = instruction
                    $0.error = "\(error)"
                })
            }
        }
    }
    
}
