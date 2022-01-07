import * as runnerApi from './proto/beam_runner_api';
import * as fnApi from './proto/beam_fn_api';
import { Coder, CODER_REGISTRY } from './coders/coders'
import { BytesCoder, IterableCoder, KVCoder } from './coders/standard_coders';
import * as translations from './internal/translations'
import * as environments from './internal/environments'
import { GeneralObjectCoder } from './coders/js_coders';
import { JobState, JobState_Enum } from './proto/beam_job_api';

import * as datefns from 'date-fns'

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

export interface PipelineResult {
    waitUntilFinish(duration?: number): Promise<JobState_Enum>;
}

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
    async runPipeline(pipeline): Promise<PipelineResult> {
        console.dir(pipeline.proto, { depth: null });
        return {
            async waitUntilFinish(duration?: number): Promise<JobState_Enum> { return JobState_Enum.UNSPECIFIED; },
        };
    }
}

type Components = runnerApi.Components | fnApi.ProcessBundleDescriptor;

export class PipelineContext {
    components: Components;

    private coders: { [key: string]: Coder<any> } = {}

    constructor(components: Components) {
        this.components = components;
    }

    getCoder<T>(coderId: string): Coder<T> {
        if (this.coders[coderId] == undefined) {
            const coderProto = this.components.coders[coderId];
            const coderConstructor = CODER_REGISTRY.get(coderProto.spec!.urn);
            const components = (coderProto.componentCoderIds || []).map(c => new (CODER_REGISTRY.get(this.components.coders[c].spec!.urn))())
            if (coderProto.spec!.payload && coderProto.spec!.payload.length) {
                this.coders[coderId] = new coderConstructor(coderProto.spec!.payload, ...components);
            } else {
                this.coders[coderId] = new coderConstructor(...components);
            }
        }
        return this.coders[coderId];
    }

    getCoderId(coder: Coder<any>): string {
        const coderId = translations.registerPipelineCoder((coder as Coder<any>).toProto!(this), this.components!);
        this.coders[coderId] = coder;
        return coderId;
    }
}

/**
 * A Pipeline is the base object to start building a Beam DAG. It is the
 * first object that a user creates, and then they may start applying
 * transformations to it to build a DAG.
 */
export class Pipeline {
    context: PipelineContext;
    private proto: runnerApi.Pipeline;
    transformStack: string[] = [];
    private defaultEnvironment: string;

    constructor() {
        this.defaultEnvironment = 'jsEnvironment'
        this.proto = runnerApi.Pipeline.create({ 'components': runnerApi.Components.create({}) });
        this.proto.components!.environments[this.defaultEnvironment] = environments.defaultJsEnvironment();
        this.context = new PipelineContext(this.proto.components!);
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
            coderId = this.context.getCoderId(coder);
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

    getCoder<T>(coderId: string): Coder<T> {
        return this.context.getCoder(coderId);
    }

    getCoderId(coder: Coder<any>): string {
        return this.context.getCoderId(coder);
    }

    getProto(): runnerApi.Pipeline {
        return this.proto;
    }
}

export class PCollection<T> {
    type: string = "pcollection";
    id: string;
    proto: runnerApi.PCollection;
    pipeline: Pipeline;

    constructor(pipeline: Pipeline, id: string) {
        this.proto = pipeline.getProto().components!.pcollections[id];  // TODO: redundant?
        this.pipeline = pipeline;
        this.id = id;
    }

    apply<OutputT extends PValue>(transform: PTransform<PCollection<T>, OutputT> | ((PCollection) => OutputT)) {
        if (!(transform instanceof PTransform)) {
            transform = new PTransformFromCallable(transform, "" + transform);
        }
        return this.pipeline.apply2(transform, this, "");
    }

    map(fn: (T) => any): PCollection<any> {
        // TODO(robertwb): Should PTransforms have generics?
        return this.apply(new ParDo(new MapDoFn(fn))) as PCollection<any>;
    }

    flatMap(fn: (T) => Generator<any, void, void>): PCollection<any> {
        // TODO(robertwb): Should PTransforms have generics?
        return this.apply(new ParDo(new FlatMapDoFn(fn))) as PCollection<any>;
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

export type PValue = void | Root | PCollection<any> | PValue[] | { [key: string]: PValue };

function flattenPValue(PValue: PValue, prefix: string = ""): { [key: string]: PCollection<any> } {
    const result: { [key: string]: PCollection<any> } = {}
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

class PValueWrapper<T extends PValue> {
    constructor(private pvalue: T) { }
    apply<O extends PValue>(transform: PTransform<T, O>, root: Root | null = null) {
        let pipeline: Pipeline;
        if (root == null) {
            const flat = flattenPValue(this.pvalue);
            pipeline = Object.values(flat)[0].pipeline;
        } else {
            pipeline = root.pipeline;
        }
        return pipeline.apply2(transform, this.pvalue, "");
    }
}

export function P<T extends PValue>(pvalue: T) {
    return new PValueWrapper(pvalue);
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

export class DoFn<InputT, OutputT> {
    *process(element: InputT): Generator<OutputT> {
        throw new Error('Method process has not been implemented!');
    }

    startBundle() { }

    finishBundle() { }
}

export interface GenericCallable {
    (input: any): any
}

export class Impulse extends PTransform<Root, PCollection<Uint8Array>> {
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

export class ParDo<InputT, OutputT> extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
    private doFn: DoFn<InputT, OutputT>;
    // static urn: string = runnerApi.StandardPTransforms_Primitives.PAR_DO.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:pardo:v1";
    constructor(doFn: DoFn<InputT, OutputT>) {
        super("ParDo(" + doFn + ")");
        this.doFn = doFn;
    }

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: PCollection<InputT>) {
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

class MapDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    private fn: (InputT) => OutputT;
    constructor(fn: (InputT) => OutputT) {
        super();
        this.fn = fn;
    }
    *process(element: InputT) {
        yield this.fn(element);
    }
}

class FlatMapDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    private fn;
    constructor(fn: (InputT) => Generator<OutputT>) {
        super();
        this.fn = fn;
    }
    *process(element: InputT) {
        yield* this.fn(element);
    }
}

export type KV<K, V> = {
    key: K,
    value: V
}

// TODO(pabloem): Consider not exporting the GBK
export class GroupByKey<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {
    // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:group_by_key:v1";

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: PCollection<KV<K, V>>) {

        // TODO: Use context.
        const pipelineComponents: runnerApi.Components = pipeline.getProto().components!;
        const inputPCollectionProto = pipelineComponents.pcollections[input.id];

        // TODO: How to ensure the input is a KV coder?
        let keyCoder: Coder<any>;
        let valueCoder: Coder<any>;
        const inputCoderProto = pipelineComponents.coders[inputPCollectionProto.coderId];
        if (inputCoderProto.componentCoderIds.length == 2) {
            keyCoder = pipeline.getCoder(inputCoderProto.componentCoderIds[0]);
            valueCoder = pipeline.getCoder(inputCoderProto.componentCoderIds[1]);
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


export class Flatten extends PTransform<PCollection<any>[], PCollection<any>> {
    // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
    // TODO: use above line, not below line.
    static urn: string = "beam:transform:flatten:v1";

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, inputs: PCollection<any>[]) {
        transformProto.spec = runnerApi.FunctionSpec.create({
            'urn': Flatten.urn,
            'payload': null!,
        });

        // TODO: Input coder if they're all the same? UnionCoder?
        return pipeline.createPCollectionInternal(new GeneralObjectCoder());
    }
}

enum Timing {
    EARLY = "early",
    ON_TIME = "on_time",
    LATE = "late",
    UNKNOWN = "unknown"
}

export interface PaneInfo {
    timing: Timing,
    index: number, // TODO: should be a long
    nonSpeculativeIndex: number, // TODO should be a long
    isFirst: boolean,
    isLast: boolean
}

export interface BoundedWindow {
    maxTimestamp(): Date
}

export interface WindowedValue<T> {
    value: T;
    windows: Array<BoundedWindow>;
    pane: PaneInfo;
    timestamp: Date;
}

export class IntervalWindow implements BoundedWindow {
    constructor(public start: Date, public end: Date) { }
    maxTimestamp() { return datefns.sub(this.end, {seconds:0.001}) }
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
