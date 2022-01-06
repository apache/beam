import * as runnerApi from './proto/beam_runner_api';
import { BytesCoder, Coder, IterableCoder, KVCoder } from './coders/standard_coders';
import * as util from 'util';
import * as translations from './internal/translations'


// TODO(pabloem): Use something better, hah.
var _pcollection_counter = -1;

export function pcollectionName() {
    _pcollection_counter += 1;
    return 'ref_PCollection_' + _pcollection_counter;
}

var _transform_counter = -1;
export function transformName() {
    _transform_counter += 1;
    return 'transform_' + _transform_counter;
}

/**
 * Represents an 'edge' in a graph. These may be PCollections, PCollection views,
 * and Pipelines themselves.
 */
class PValue {
    // TODO: Have a reference to its graph representation
    type: string = "unknown";
    name: string;
    pipeline: Pipeline;

    constructor(name: string) {
        this.name = name;
    }

    isPipeline(): boolean {
        return this.type === "pipeline";
    }

    // TODO(pabloem): What about multiple outputs? (not strictly necessary ATM. Can do with Filters)
    apply(transform: PTransform): PCollection {
        const outPcoll = transform.expand(this);
        if (outPcoll.type !== 'pcollection') {
            throw new Error(util.format('Trahsform %s does not return a PCollection', transform));
        }
        return outPcoll as PCollection;
    }

    map(callable: DoFn | GenericCallable): PCollection {
        return this.apply(new ParDo(callable));
    }

    // Top-level functions:
    // - flatMap
    // - filter?
}

/**
 * A Pipeline is the base object to start building a Beam DAG. It is the
 * first object that a user creates, and then they may start applying
 * transformations to it to build a DAG.
 */
export class Pipeline extends PValue {
    type: string = "pipeline";
    proto: runnerApi.Pipeline;

    // A map of coder ID to Coder object
    coders: {[key: string]: Coder} = {}

    constructor() {
        super("root");
        this.proto = runnerApi.Pipeline.create(
            {'components': runnerApi.Components.create()}
        );
        this.pipeline = this;
    }
}

export class PCollection extends PValue {
    type: string = "pcollection";
    proto: runnerApi.PCollection;

    constructor(name: string, pcollectionProto: runnerApi.PCollection, pipeline: Pipeline) {
        super(name)
        this.proto = pcollectionProto;
        this.pipeline = pipeline;
    }
}

export class PTransform {
    expand(input: PValue): PValue {
        throw new Error('Method expand has not been implemented.');
    }
}

export class DoFn {
    type: string = "dofn";
    process(element: any) {
        throw new Error('Method process has not been implemented!');
    }

    startBundle(element: any) {
        throw new Error('Method process has not been implemented!');
    }

    finishBundle(element: any) {
        throw new Error('Method process has not been implemented!');
    }
}

export interface GenericCallable {
    (input: any): any
}


    export class Impulse extends PTransform {
        // static urn: string = runnerApi.StandardPTransforms_Primitives.IMPULSE.urn;
        // TODO: use above line, not below line.
        static urn: string = "beam:transform:impulse:v1";
        expand(input: Pipeline): PCollection {
            if (!input.isPipeline()) {
                throw new Error("User is attempting to apply Impulse transform to a non-pipeline object.");
            }
            const pipeline = input as Pipeline;
    
            const pcollName = pcollectionName();
    
            const coderId = translations.registerPipelineCoder(runnerApi.Coder.create({'spec': runnerApi.FunctionSpec.create({'urn': BytesCoder.URN})}), pipeline.proto.components!);
            pipeline.coders[coderId] = new BytesCoder();
    
            const outputProto = runnerApi.PCollection.create({
                'uniqueName': pcollName,
                'coderId': coderId,
                'isBounded': runnerApi.IsBounded_Enum.BOUNDED
            });
    
            const impulseProto = runnerApi.PTransform.create({
                // TODO(pabloem): Get the name for the PTransform
                'uniqueName': 'todouniquename',
                'spec': runnerApi.FunctionSpec.create({
                    'urn': translations.DATA_INPUT_URN,
                    'payload': translations.IMPULSE_BUFFER
                }),
                'outputs': {'out': pcollName}
            });
            
            return new PCollection(impulseProto.outputs.out, outputProto, input.pipeline);
        }
    }
    
    /**
     * @returns true if the input is a `DoFn`.
     * 
     * Since type information is lost at runtime, we check the object's attributes
     * to determine whether it's a DoFn or not.
     * 
     * @example
     * Prints "true" for a new `DoFn` but "false" for a function:
     * ```ts
     * console.log(new DoFn());
     * console.log(in => in * 2));
     * ```
     * @param callableOrDoFn 
     * @returns 
     */
     function isDoFn(callableOrDoFn: DoFn | GenericCallable) {
        const df = (callableOrDoFn as DoFn)
        if (df.type !== undefined && df.type === "dofn") {
            return true;
        } else {
            return false;
        }
    }
    
    export class ParDo extends PTransform {
        static _CallableWrapperDoFn = class extends DoFn {
            private fn;
            constructor(fn: GenericCallable) {
                super();
                this.fn = fn;
            }
            process(element: any) {
                return this.fn(element);
            }
        }
    
        private doFn;
        // static urn: string = runnerApi.StandardPTransforms_Primitives.PAR_DO.urn;
        // TODO: use above line, not below line.
        static urn: string = "beam:transform:pardo:v1";
        constructor(callableOrDoFn: DoFn | GenericCallable) {
            super()
            if (isDoFn(callableOrDoFn)) {
                this.doFn = callableOrDoFn;
            } else {
                this.doFn = new ParDo._CallableWrapperDoFn(callableOrDoFn as GenericCallable);
            }
        }
    
        expand(input: PCollection): PCollection {
            
            if (input.type !== 'pcollection') {
                throw new Error('ParDo received the wrong input.');
            }
    
            const pcollName = pcollectionName();
    
            const outputProto = runnerApi.PCollection.create({
                'uniqueName': pcollName,
                'coderId': BytesCoder.URN, // TODO: Get coder URN
                'isBounded': runnerApi.IsBounded_Enum.BOUNDED
            });
    
            const inputPCollName = (input as PCollection).proto.uniqueName;
    
            // TODO(pabloem): Get the name for the PTransform
            const pardoName = 'todouniquename';
            const inputId = pardoName + '1';
    
            const pardoProto = runnerApi.PTransform.create({
                'uniqueName': pardoName,
                'spec': runnerApi.FunctionSpec.create({
                    'urn': ParDo.urn,
                    'payload': runnerApi.ParDoPayload.toBinary(
                    runnerApi.ParDoPayload.create({
                        'doFn': runnerApi.FunctionSpec.create({
                            'urn': translations.SERIALIZED_JS_DOFN_INFO,
                            'payload': new Uint8Array()
                        })
                    }))
                }),
                'inputs': {inputId: inputPCollName},
                'outputs': {'out': pcollName}
            });
    
            // TODO(pablom): Do this properly
            return new PCollection(pardoProto.outputs.out, outputProto, input.pipeline);
        }
    }
    
    // TODO(pabloem): Consider not exporting the GBK
    export class GroupByKey extends PTransform {
        // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
        // TODO: use above line, not below line.
        static urn: string = "beam:transform:group_by_key:v1";
    
        expand(input: PCollection): PCollection {
            const inputPCollectionProto: runnerApi.PCollection = input.type == 'pcollection' ? (input as PCollection).proto : undefined!;
            if (inputPCollectionProto === undefined) {
                throw new Error('Input is not a PCollection object.');
            }
    
            const pipelineComponents: runnerApi.Components = input.pipeline.proto.components!;
    
            const keyCoderId = pipelineComponents.coders[inputPCollectionProto.coderId].componentCoderIds[0];
            const valueCoderId = pipelineComponents.coders[inputPCollectionProto.coderId].componentCoderIds[1];
    
            const iterableValueCoderProto = runnerApi.Coder.create({
                'spec': {'urn': IterableCoder.URN,},
                'componentCoderIds': [valueCoderId]
            });
            const iterableValueCoderId = translations.registerPipelineCoder(iterableValueCoderProto, pipelineComponents)!;
            const iterableValueCoder = new IterableCoder(input.pipeline.coders[valueCoderId]);
            input.pipeline.coders[iterableValueCoderId] = iterableValueCoder;
    
            const outputCoderProto = runnerApi.Coder.create({
                'spec': runnerApi.FunctionSpec.create({'urn': KVCoder.URN}),
                'componentCoderIds': [keyCoderId, iterableValueCoderId]
            })
            const outputPcollCoderId = translations.registerPipelineCoder(outputCoderProto, pipelineComponents)!;
    
            const outputPCollectionProto = runnerApi.PCollection.create({
                'uniqueName': pcollectionName(),
                'isBounded': inputPCollectionProto.isBounded,
                'coderId': outputPcollCoderId
            });
            pipelineComponents.pcollections[outputPCollectionProto.uniqueName] = outputPCollectionProto;
    
            const ptransformProto = runnerApi.PTransform.create({
                'uniqueName': 'TODO NAME',
                'spec': runnerApi.FunctionSpec.create({
                    'urn': GroupByKey.urn,
                    'payload': null!  // TODO(GBK payload????)
                }),
                'outputs': {'out': outputPCollectionProto.uniqueName}
            });
            pipelineComponents.transforms[ptransformProto.uniqueName] = ptransformProto;

            input.pipeline.coders[outputPcollCoderId] = new KVCoder(
                input.pipeline.coders[keyCoderId],
                input.pipeline.coders[iterableValueCoderId]);
    
            return new PCollection(outputPCollectionProto.uniqueName, outputPCollectionProto, input.pipeline);
        }
    }