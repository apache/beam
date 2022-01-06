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
    return 'transformId(' + _transform_counter + ')';
}

interface PipelineResult { }

export interface Runner {
    run: (pipeline: (Root) => PValueish) => PipelineResult;
}

export class ProtoPrintingRunner implements Runner {
    run(pipelineFunc) {
        const p = new Pipeline();
        pipelineFunc(new Root(p));
        console.dir(p.proto, { depth: null });
        return {}
    }
}

/**
 * Represents an 'edge' in a graph. These may be PCollections, PCollection views,
 * and Pipelines themselves.
 */
// TODO: Remove PValue or replace it with PValueish?
class PValue {
    // TODO: Have a reference to its graph representation
    type: string = "unknown";
    name: string;
    pipeline: Pipeline;

    constructor(name: string) {
        this.name = name;
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
    transformStack: string[] = [];

    // A map of coder ID to Coder object
    // TODO: Is this needed?
    coders: { [key: string]: Coder } = {}

    constructor() {
        super("root");
        this.proto = runnerApi.Pipeline.create(
            { 'components': runnerApi.Components.create() }
        );
        this.pipeline = this;
    }

    // TODO: Remove once test are fixed.
    apply<OutputT extends PValueish>(transform: PTransform<Root, OutputT>): OutputT {
        return new Root(this).apply(transform);
    }

    apply2<InputT extends PValueish, OutputT extends PValueish>(transform: PTransform<InputT, OutputT>, pvalueish: InputT, name: string) {

        function objectMap(obj, func) {
            return Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, func(v)]));
        }

        const this_ = this;
        const transformId = transformName();
        if (this.transformStack.length) {
            this.proto!.components!.transforms![this.transformStack[this.transformStack.length - 1]].subtransforms.push(transformId);
        } else {
            this.proto.rootTransformIds.push(transformId);
        }
        const transformProto: runnerApi.PTransform = {
            uniqueName: this.transformStack.map((id) => this_.proto?.components?.transforms![id].uniqueName).concat([name || transform.name]).join('/'),
            subtransforms: [],
            inputs: objectMap(flattenPValueish(pvalueish), (pc) => pc.id),
            outputs: {},
            environmentId: "",
            displayData: [],
            annotations: {},
        }
        this.proto!.components!.transforms![transformId] = transformProto;
        this.transformStack.push(transformId);
        const result = transform.expandInternal(this, transformProto, pvalueish); // TODO: try-catch
        this.transformStack.pop();
        transformProto.outputs = objectMap(flattenPValueish(result), (pc) => pc.id)
        return result;
    }

    createPCollectionInternal(coder: Coder | string, isBounded = runnerApi.IsBounded_Enum.BOUNDED) {
        const pcollId = pcollectionName();
        let coderId: string;
        if (typeof coder == "string") {
            coderId = coder;
        } else {
            coderId = translations.registerPipelineCoder((coder as Coder).toProto(this.pipeline.proto.components!), this.pipeline.proto.components!);
            // TODO: Do we need this?
            this.coders[coderId] = coder;
        }
        this.pipeline.proto!.components!.pcollections[pcollId] = {
            uniqueName: pcollId, // TODO: name according to producing transform?
            coderId: coderId,
            isBounded: isBounded,
            windowingStrategyId: 'global',
            displayData: [],
        }
        return new PCollection(this, pcollId);
    }
}

export class PCollection extends PValue {
    type: string = "pcollection";
    id: string;
    proto: runnerApi.PCollection;

    constructor(pipeline: Pipeline, id: string) {
        super("unusedName")
        this.proto = pipeline.proto!.components!.pcollections[id];  // TODO: redundant?
        this.pipeline = pipeline;
        this.id = id;
    }

    apply<OutputT extends PValueish>(transform: PTransform<PCollection, OutputT> | ((PCollection) => OutputT)) {
        if (!(transform instanceof PTransform)) {
            transform = new PTransformFromCallable(transform, "" + transform);
        }
        return this.pipeline.apply2(transform, this, "");
    }

    map(fn: (any) => any): PCollection {
        // TODO(robertwb): Should PTransforms have generics?
        return this.apply(new ParDo(new MapDoFn(fn))) as PCollection;
    }

    flatMap(fn: (any) => Generator<any, void, void>): PCollection {
        // TODO(robertwb): Should PTransforms have generics?
        return this.apply(new ParDo(new FlatMapDoFn(fn))) as PCollection;
    }
}

/**
 * The base object on which one can start building a Beam DAG.
 * Generally followed by a source-like transform such as a read or impulse.
 */
export class Root {
    pipeline: Pipeline;

    constructor(pipeline: Pipeline) {
        this.pipeline = pipeline;
    }

    apply<OutputT extends PValueish>(transform: PTransform<Root, OutputT> | ((Root) => OutputT)) {
        if (!(transform instanceof PTransform)) {
            transform = new PTransformFromCallable(transform, "" + transform);
        }
        return this.pipeline.apply2(transform, this, "");
    }
}

type PValueish = void | Root | PCollection | PValueish[] | { [key: string]: PValueish };

function flattenPValueish(pvalueish: PValueish, prefix: string = ""): { [key: string]: PCollection } {
    const result: { [key: string]: PCollection } = {}
    if (pvalueish == null) {
        // pass
    } else if (pvalueish instanceof Root) {
        // pass
    } else if (pvalueish instanceof PCollection) {
        if (prefix) {
            result[prefix] = pvalueish
        } else {
            result.main = pvalueish;
        }
    } else {
        if (prefix) {
            prefix += ".";
        }
        if (pvalueish instanceof Array) {
            for (var i = 0; i < pvalueish.length; i++) {
                Object.assign(result, flattenPValueish(pvalueish[i], prefix + i));
            }
        } else {
            for (const [key, value] of Object.entries(pvalueish)) {
                Object.assign(result, flattenPValueish(value, prefix + key));
            }
        }
    }
    return result;
}

export class PTransform<InputT extends PValueish, OutputT extends PValueish> {
    name: string;

    constructor(name: string | null = null) {
        this.name = name || (typeof this);
    }

    expand(input: InputT): OutputT {
        throw new Error('Method expand has not been implemented.');
    }

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: InputT): OutputT {
        return this.expand(input);
    }
}

class PTransformFromCallable<InputT extends PValueish, OutputT extends PValueish> extends PTransform<InputT, OutputT> {
    name: string;
    expander: (InputT) => OutputT;

    constructor(expander: (InputT) => OutputT, name: string) {
        super(name);
        this.expander = expander;
    }

    expand(input: InputT) {
        return this.expander(input);
    }
}

export class DoFn {
    *process(element: any): Generator<any, void, never> {
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

export class Impulse extends PTransform<Root, PCollection> {
    // static urn: string = runnerApi.StandardPTransforms_Primitives.IMPULSE.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:impulse:v1";

    constructor() {
        super("Impulse");  // TODO: pass null/nothing and get from reflection
    }

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: PValueish) {
        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': translations.DATA_INPUT_URN,
            'payload': translations.IMPULSE_BUFFER
        });
        return pipeline.createPCollectionInternal(new BytesCoder())
    }
}

export class ParDo extends PTransform<PCollection, PCollection> {
    private doFn: DoFn;
    // static urn: string = runnerApi.StandardPTransforms_Primitives.PAR_DO.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:pardo:v1";
    constructor(doFn: DoFn) {
        super("ParDo(" + doFn + ")");
        this.doFn = doFn;
    }

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: PCollection) {
        // Might not be needed due to generics.
        if (!(input instanceof PCollection)) {
            throw new Error('ParDo received the wrong input.');
        }

        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': ParDo.urn,
            'payload': runnerApi.ParDoPayload.toBinary(
                runnerApi.ParDoPayload.create({
                    'doFn': runnerApi.FunctionSpec.create({
                        'urn': translations.SERIALIZED_JS_DOFN_INFO,
                        'payload': fakeSeralize(this.doFn),
                    })
                }))
        });

        // TODO(paboem): How do we infer the proper coder for this transform?. For now we use the same as input.
        return pipeline.createPCollectionInternal(input.proto.coderId);
    }
}

class MapDoFn extends DoFn {
    private fn: (any) => any;
    constructor(fn: (any) => any) {
        super();
        this.fn = fn;
    }
    *process(element: any) {
        yield this.fn(element);
    }
}

class FlatMapDoFn extends DoFn {
    private fn;
    constructor(fn: (any) => Generator<any, void, void>) {
        super();
        this.fn = fn;
    }
    *process(element: any) {
        yield* this.fn(element);
    }
}


// TODO(pabloem): Consider not exporting the GBK
export class GroupByKey extends PTransform<PCollection, PCollection> {
    // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:group_by_key:v1";

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: PCollection) {

        const pipelineComponents: runnerApi.Components = pipeline.proto.components!;
        const inputPCollectionProto = pipelineComponents.pcollections[input.id];

        // TODO: How to ensure the input is a KV coder?
        let keyCoderId: string;
        let valueCoderId: string;
        if (pipelineComponents.coders[inputPCollectionProto.coderId].componentCoderIds.length == 2) {
            keyCoderId = pipelineComponents.coders[inputPCollectionProto.coderId].componentCoderIds[0];
            valueCoderId = pipelineComponents.coders[inputPCollectionProto.coderId].componentCoderIds[1];
        }
        else {
            keyCoderId = valueCoderId = inputPCollectionProto.coderId; // JsonCoder?
        }

        const iterableValueCoderProto = runnerApi.Coder.create({
            'spec': { 'urn': IterableCoder.URN, },
            'componentCoderIds': [valueCoderId]
        });
        const iterableValueCoderId = translations.registerPipelineCoder(iterableValueCoderProto, pipelineComponents)!;
        const iterableValueCoder = new IterableCoder(pipeline.coders[valueCoderId]);
        pipeline.coders[iterableValueCoderId] = iterableValueCoder;

        const outputCoderProto = runnerApi.Coder.create({
            'spec': runnerApi.FunctionSpec.create({ 'urn': KVCoder.URN }),
            'componentCoderIds': [keyCoderId, iterableValueCoderId]
        })
        const outputPcollCoderId = translations.registerPipelineCoder(outputCoderProto, pipelineComponents)!;

        pipeline.coders[outputPcollCoderId] = new KVCoder(
            pipeline.coders[keyCoderId],
            pipeline.coders[iterableValueCoderId]);

        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': GroupByKey.urn,
            'payload': null!,
        });

        return pipeline.createPCollectionInternal(outputPcollCoderId);
    }
}


let fakeSerializeCounter = 0;
const fakeSerializeMap = new Map<string, any>();
export function fakeSeralize(obj) {
    fakeSerializeCounter += 1;
    const id = "serialized_" + fakeSerializeCounter;
    fakeSerializeMap.set(id, obj);
    return new TextEncoder().encode(id);
}
export function fakeDeserialize(s) {
    return fakeSerializeMap.get(new TextDecoder().decode(s));
}
