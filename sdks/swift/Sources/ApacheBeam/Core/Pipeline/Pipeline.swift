import GRPC
import Logging

public final class Pipeline {
    let content: (inout PCollection<Never>) -> Void
    let log: Logging.Logger
    
    public init(log: Logging.Logger = .init(label:"Pipeline"),_ content: @escaping (inout PCollection<Never>) -> Void) {
        self.log = log
        self.content = content
    }
    
    public func run(_ runner: PipelineRunner) async throws {
        try await runner.run(try self.context)        
    }
    
    
    /// For managing the pipeline items to visit
    enum Visit {
        case transform([PipelineComponent],PipelineTransform)
        case collection(AnyPCollection)
    }
    
    var context: PipelineContext {
        get throws {
            // Grab the pipeline content using an new root
            var root = PCollection<Never>(coder:.unknown(.coderUrn("never")))
            _ = content(&root)
            
            // These get passed to the pipeline context
            var collections: [String:AnyPCollection] = [:]
            var fns: [String:SerializableFn] = [:]
            var coders: [String:Coder] = [:]
            var counter: Int = 1
            
            // These caches are just used internally
            var collectionCache: [AnyPCollection:PipelineComponent] = [:]
            var coderCache: [Coder:PipelineComponent] = [:]
            var rootIds: [String] = []
            
            var defaultEnvironment: PipelineComponent = .none
            
            //TODO: Support for composite PTransforms
            let pipeline: PipelineProto = try .with { proto in
                
                func uniqueName(_ prefix: String = "id") -> String {
                    let output = "\(prefix)\(counter)"
                    counter = counter + 1
                    return output
                }
                
                
                
                /// We need to define this inside the with to prevent concurrent access errors.
                func coder(from:Coder) -> PipelineComponent {
                    if let cached = coderCache[from] {
                        return cached
                    }
                    let componentCoders:[String] = switch from {
                    case let .keyvalue(keyCoder, valueCoder):
                        [coder(from:keyCoder).name,coder(from:valueCoder).name]
                    case let .iterable(valueCoder):
                        [coder(from:valueCoder).name]
                    case let .lengthprefix(valueCoder):
                        [coder(from:valueCoder).name]
                    case let .windowedvalue(valueCoder, windowCoder):
                        [coder(from:valueCoder).name,coder(from:windowCoder).name]
                    default:
                        []
                    }
                    let baseCoder = proto.coder { _ in
                            .with {
                                $0.spec = .with {
                                    $0.urn = from.urn
                                    if case .custom(let data) = from {
                                        $0.payload = data
                                    }
                                }
                                $0.componentCoderIds = componentCoders
                            }
                    }
                    coderCache[from] = baseCoder
                    coders[baseCoder.name] = from
                    return baseCoder
                }
                
                /// Define the default environment for this pipeline
                defaultEnvironment = try proto.environment(from:.init(.docker("swift:image"),
                                                                      capabilities:Coder.capabilities,
                                                                      dependencies:[]))
                
                /// Define the default strategy
                let globalWindow = coder(from:.globalwindow)
                let defaultStrategy = proto.windowingStrategy { _ in
                        .with {
                            $0.windowCoderID = globalWindow.name
                            $0.windowFn = .with {
                                $0.urn = .beamUrn("global_windows",type:"window_fn")
                            }
                            $0.mergeStatus = .nonMerging
                            $0.trigger = .with {
                                $0.default = .init()
                            }
                            $0.accumulationMode = .discarding
                            $0.outputTime = .endOfWindow
                            $0.closingBehavior = .emitIfNonempty
                            $0.onTimeBehavior = .fireIfNonempty
                            $0.environmentID = defaultEnvironment.name
                        }
                }
                
                
                /// As above we define this within the "with" to prevent concurrent access errors.
                func collection(from collection:AnyPCollection) -> PipelineComponent {
                    if let cached = collectionCache[collection] {
                        return cached
                    }
                    let coder = coder(from:collection.coder)
                    let output = proto.collection { _ in
                            .with {
                                $0.uniqueName = uniqueName("c")
                                $0.coderID = coder.name
                                $0.windowingStrategyID = defaultStrategy.name
                                $0.isBounded = .bounded //TODO: Get this from the coder
                            }
                    }
                    collectionCache[collection] = output
                    collections[output.name] = collection
                    return output
                }
                
                func transform(name:String = "",_ fn: @escaping (String,String) throws -> PTransformProto) throws -> PipelineComponent {
                    return try proto.transform { ref in
                        return try fn(ref,name.count > 0 ? name+uniqueName(".t") : uniqueName("t"))
                    }
                }
                
                var toVisit: [Visit] = root.consumers.map({ .transform([], $0)})
                var visited = Set<AnyPCollection>() // Cycle detection, etc
                
                while toVisit.count > 0 {
                    let item = toVisit.removeFirst()
                    if case .transform(let parents, let pipelineTransform) = item {
                        let inputs = parents.enumerated().map({ ("\($0)","\($1.name)")}).dict()
                        switch pipelineTransform {
                            
                        case let .pardo(n, fn, o):
                            let outputs = o.enumerated().map {
                                ("\($0)",collection(from: $1).name)
                            }.dict()
                            let p = try transform(name:n) { _,name in
                                    try .with {
                                        $0.uniqueName = name
                                        $0.inputs = inputs
                                        $0.outputs = outputs
                                        $0.spec = try .with {
                                            $0.urn = .transformUrn("pardo")
                                            $0.payload = try Org_Apache_Beam_Model_Pipeline_V1_ParDoPayload.with {
                                                $0.doFn = try .with {
                                                    $0.urn = fn.urn
                                                    $0.payload = try fn.payload

                                                }
                                            }.serializedData()
                                        }
                                        $0.environmentID = defaultEnvironment.name
                                    }
                            }
                            rootIds.append(p.name) //TODO: Composite transform handling
                            fns[p.transform!.uniqueName] = fn
                            toVisit.append(contentsOf: o.map { .collection($0) })
                        case .impulse(let o):
                            let outputs = [o].enumerated().map {
                                ("\($0)",collection(from: $1).name)
                            }.dict()
                            let p = try transform { _,name in
                                    .with {
                                        $0.uniqueName = name
                                        $0.outputs = outputs
                                        $0.spec = .with {
                                            $0.urn = .transformUrn("impulse")
                                        }
                                        $0.environmentID = defaultEnvironment.name
                                    }
                            }
                            rootIds.append(p.name)
                            toVisit.append(.collection(o))
                        case .flatten(_, _):
                            throw ApacheBeamError.runtimeError("flatten not implemented yet")
                        case .groupByKey(let o):
                            let outputs = [o].enumerated().map {
                                ("\($0)",collection(from: $1).name)
                            }.dict()
                            let p = try transform { _,name in
                                    .with {
                                        $0.uniqueName = name
                                        $0.inputs = inputs
                                        $0.outputs = outputs
                                        $0.spec = .with {
                                            $0.urn = .transformUrn("group_by_key")
                                        }
                                        $0.environmentID = defaultEnvironment.name
                                    }
                            }
                            rootIds.append(p.name)
                            toVisit.append(.collection(o))
                        case let .custom(urn,payload,env,o):
                            let outputs = o.enumerated().map {
                                ("\($0)",collection(from: $1).name)
                            }.dict()
                            let environment = if let e = env {
                                try proto.environment(from: e)
                            } else {
                                defaultEnvironment
                            }
                            let p = try transform { _,name in
                                    .with {
                                        $0.uniqueName = name
                                        $0.inputs = inputs
                                        $0.outputs = outputs
                                        $0.spec = .with {
                                            $0.urn = urn
                                            $0.payload = payload
                                        }
                                        $0.environmentID = environment.name
                                    }
                            }
                            rootIds.append(p.name)
                            toVisit.append(contentsOf:o.map { .collection($0) })
                        case .composite(_):
                            throw ApacheBeamError.runtimeError("Composite transforms are not yet implemented")
                        }
                    } else if case .collection(let anyPCollection) = item {
                        if visited.contains(anyPCollection) {
                            throw ApacheBeamError.runtimeError("Pipeline definition contains a cycle.")
                        }
                        visited.insert(anyPCollection)
                        //TODO: Remove this to see if we can recreate the error I was seeing earlier for robertwb
                        if anyPCollection.consumers.count > 0 {
                            let me = collection(from:anyPCollection)
                            toVisit.append(contentsOf: anyPCollection.consumers.map({ .transform([me], $0)}))
                        }
                    }
                }
                proto.rootTransformIds = rootIds
                
            }
            return PipelineContext(pipeline,defaultEnvironment.name,collections,fns)
        }
    }
}
