import * as runnerApi from './proto/beam_runner_api';
import { Coder } from './coders/coders'
import { BytesCoder, IterableCoder, KVCoder } from './coders/standard_coders';
import * as util from 'util';
import * as translations from './internal/translations'
import { GeneralObjectCoder } from './coders/js_coders';


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

export class Runner {
    async run(pipeline: ((Root) => PValue)): Promise<PipelineResult> {
        const p = new Pipeline();
        pipeline(new Root(p));
        return this.runPipeline(p);
    }
    async runPipeline(pipeline: Pipeline): Promise<PipelineResult> {
        throw new Error("Not implemented.");
    }
}

export class ProtoPrintingRunner extends Runner {
    async runPipeline(pipeline) {
        console.dir(pipeline.proto, { depth: null });
        return {}
    }
}

/**
 * A Pipeline is the base object to start building a Beam DAG. It is the
 * first object that a user creates, and then they may start applying
 * transformations to it to build a DAG.
 */
export class Pipeline {
    proto: runnerApi.Pipeline;
    transformStack: string[] = [];

    // A map of coder ID to Coder object
    // TODO: Is this needed?
    coders: { [key: string]: Coder<any> } = {}

    constructor() {
        this.proto = runnerApi.Pipeline.create(
            { 'components': runnerApi.Components.create() }
        );
    }

    // TODO: Remove once test are fixed.
    apply<OutputT extends PValue>(transform: PTransform<Root, OutputT>): OutputT {
        return new Root(this).apply(transform);
    }

    apply2<InputT extends PValue, OutputT extends PValue>(transform: PTransform<InputT, OutputT>, PValue: InputT, name: string) {

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
            inputs: objectMap(flattenPValue(PValue), (pc) => pc.id),
            outputs: {},
            environmentId: "",
            displayData: [],
            annotations: {},
        }
        this.proto!.components!.transforms![transformId] = transformProto;
        this.transformStack.push(transformId);
        const result = transform.expandInternal(this, transformProto, PValue); // TODO: try-catch
        this.transformStack.pop();
        transformProto.outputs = objectMap(flattenPValue(result), (pc) => pc.id)
        return result;
    }

    createPCollectionInternal(coder: Coder<any> | string, isBounded = runnerApi.IsBounded_Enum.BOUNDED) {
        const pcollId = pcollectionName();
        let coderId: string;
        if (typeof coder == "string") {
            coderId = coder;
        } else {
            coderId = translations.registerPipelineCoder((coder as Coder<any>).toProto!(this.proto.components!), this.proto.components!);
            // TODO: Do we need this?
            this.coders[coderId] = coder;
        }
        this.proto!.components!.pcollections[pcollId] = {
            uniqueName: pcollId, // TODO: name according to producing transform?
            coderId: coderId,
            isBounded: isBounded,
            windowingStrategyId: 'global',
            displayData: [],
        }
        return new PCollection(this, pcollId);
    }
}

export class PCollection {
    type: string = "pcollection";
    id: string;
    proto: runnerApi.PCollection;
    pipeline: Pipeline;

    constructor(pipeline: Pipeline, id: string) {
        this.proto = pipeline.proto!.components!.pcollections[id];  // TODO: redundant?
        this.pipeline = pipeline;
        this.id = id;
    }

    apply<OutputT extends PValue>(transform: PTransform<PCollection, OutputT> | ((PCollection) => OutputT)) {
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

    root(): Root {
        return new Root(this.pipeline);
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

    apply<OutputT extends PValue>(transform: PTransform<Root, OutputT> | ((Root) => OutputT)) {
        if (!(transform instanceof PTransform)) {
            transform = new PTransformFromCallable(transform, "" + transform);
        }
        return this.pipeline.apply2(transform, this, "");
    }
}

type PValue = void | Root | PCollection | PValue[] | { [key: string]: PValue };

function flattenPValue(PValue: PValue, prefix: string = ""): { [key: string]: PCollection } {
    const result: { [key: string]: PCollection } = {}
    if (PValue == null) {
        // pass
    } else if (PValue instanceof Root) {
        // pass
    } else if (PValue instanceof PCollection) {
        if (prefix) {
            result[prefix] = PValue
        } else {
            result.main = PValue;
        }
    } else {
        if (prefix) {
            prefix += ".";
        }
        if (PValue instanceof Array) {
            for (var i = 0; i < PValue.length; i++) {
                Object.assign(result, flattenPValue(PValue[i], prefix + i));
            }
        } else {
            for (const [key, value] of Object.entries(PValue)) {
                Object.assign(result, flattenPValue(value, prefix + key));
            }
        }
    }
    return result;
}

export class PTransform<InputT extends PValue, OutputT extends PValue> {
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

class PTransformFromCallable<InputT extends PValue, OutputT extends PValue> extends PTransform<InputT, OutputT> {
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
    *process(element: any): Generator<any, void, undefined> {
        throw new Error('Method process has not been implemented!');
    }

    startBundle() { }

    finishBundle() { }
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

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: PValue) {
        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': Impulse.urn,
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

        // For the ParDo output coder, we use a GeneralObjectCoder, which is a Javascript-specific
        // coder to encode the various types that exist in JS.
        return pipeline.createPCollectionInternal(new GeneralObjectCoder());
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
        let keyCoder: Coder<any>;
        let valueCoder: Coder<any>;
        const inputCoderProto = pipelineComponents.coders[inputPCollectionProto.coderId];
        if (inputCoderProto.componentCoderIds.length == 2) {
            keyCoder = pipeline.coders[inputCoderProto.componentCoderIds[0]];
            valueCoder = pipeline.coders[inputCoderProto.componentCoderIds[1]];
        }
        else {
            keyCoder = valueCoder = new GeneralObjectCoder();
        }
        const iterableValueCoder = new IterableCoder(valueCoder);
        const outputCoder = new KVCoder(keyCoder, iterableValueCoder);

        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': GroupByKey.urn,
            'payload': null!,
        });

        return pipeline.createPCollectionInternal(outputCoder);
    }
}


// TODO: P(...).apply()
export function flattenFunction(pcolls: PCollection[]): PCollection {
    return pcolls[0].pipeline.apply2(new Flatten(), pcolls, "Flatten");
}


export class Flatten extends PTransform<PCollection[], PCollection> {
    // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:flatten:v1";

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, inputs: PCollection[]) {
        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': Flatten.urn,
            'payload': null!,
        });

        // TODO: Input coder if they're all the same? UnionCoder?
        return pipeline.createPCollectionInternal(new GeneralObjectCoder());
    }
}


let fakeSerializeCounter = 0;
const fakeSerializeMap = new Map<string, any>();
export function fakeSeralize(obj) {
    fakeSerializeCounter += 1;
    const id = "s_" + fakeSerializeCounter;
    fakeSerializeMap.set(id, obj);
    return new TextEncoder().encode(id);
}
export function fakeDeserialize(s) {
    return fakeSerializeMap.get(new TextDecoder().decode(s));
}
