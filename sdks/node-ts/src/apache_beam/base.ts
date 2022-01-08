import * as runnerApi from './proto/beam_runner_api';
import * as fnApi from './proto/beam_fn_api';
import { Coder, CODER_REGISTRY } from './coders/coders'
import { GlobalWindowCoder } from './coders/standard_coders'
import { BytesCoder, IterableCoder, KVCoder } from './coders/standard_coders';
import * as translations from './internal/translations'
import * as environments from './internal/environments'
import { GeneralObjectCoder } from './coders/js_coders';
import { JobState_Enum } from './proto/beam_job_api';
import equal from 'fast-deep-equal'

import * as datefns from 'date-fns'
import { PipelineOptions } from './options/pipeline_options';

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
    /**
     * Runs the transform.
     *
     * Resolves to an instance of PipelineResult when the pipeline completes.
     * Use runAsync() to execute the pipeline in the background.
     *
     * @param pipeline
     * @returns A PipelineResult
     */
    async run(pipeline: ((root: Root) => PValue<any>), options?: PipelineOptions): Promise<PipelineResult> {
        const p = new Pipeline();
        pipeline(new Root(p));
        const pipelineResult = await this.runPipeline(p, options);
        await pipelineResult.waitUntilFinish();
        return pipelineResult;
    }

    /**
     * runAsync() is the asynchronous version of run(), does not wait until
     * pipeline finishes. Use the returned PipelineResult to query job
     * status.
     */
    async runAsync(pipeline: ((root: Root) => PValue<any>), options?: PipelineOptions): Promise<PipelineResult> {
        const p = new Pipeline();
        pipeline(new Root(p));
        return this.runPipeline(p);
    }

    protected async runPipeline(pipeline: Pipeline, options?: PipelineOptions): Promise<PipelineResult> {
        throw new Error("Not implemented.");
    }
}

export class ProtoPrintingRunner extends Runner {
    async runPipeline(pipeline): Promise<PipelineResult> {
        console.dir(pipeline.proto, { depth: null });
        return {
            waitUntilFinish: (duration?) => Promise.reject('not implemented'),
        };
    }
}

type Components = runnerApi.Components | fnApi.ProcessBundleDescriptor;

export class PipelineContext {
    components: Components;
    counter: number = 0;

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

    getWindowingStrategy(id: string): runnerApi.WindowingStrategy {
        return this.components.windowingStrategies[id];
    }

    getWindowingStrategyId(windowing: runnerApi.WindowingStrategy): string {
        for (const [id, proto] of Object.entries(this.components.windowingStrategies)) {
            if (equal(proto, windowing)) {
                return id;
            }
        }
        const newId = "_windowing_" + (this.counter++);
        this.components.windowingStrategies[newId] = windowing;
        return newId;
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
    private globalWindowing: string;

    constructor() {
        this.defaultEnvironment = 'jsEnvironment';
        this.globalWindowing = 'globalWindowing';
        this.proto = runnerApi.Pipeline.create({ 'components': runnerApi.Components.create({}) });
        this.proto.components!.environments[this.defaultEnvironment] = environments.defaultJsEnvironment();
        this.context = new PipelineContext(this.proto.components!);
        this.proto.components!.windowingStrategies[this.globalWindowing] = {
            windowCoderId: this.context.getCoderId(new GlobalWindowCoder()),
            accumulationMode: runnerApi.AccumulationMode_Enum.DISCARDING,
            outputTime: runnerApi.OutputTime_Enum.END_OF_WINDOW,
            mergeStatus: runnerApi.MergeStatus_Enum.NEEDS_MERGE,
            closingBehavior: runnerApi.ClosingBehavior_Enum.EMIT_ALWAYS,
            onTimeBehavior: runnerApi.OnTimeBehavior_Enum.FIRE_ALWAYS,
            allowedLateness: BigInt(0),
            assignsToOneWindow: true,
            environmentId: this.defaultEnvironment,
        };
    }

    // TODO: Remove once test are fixed.
    apply<OutputT extends PValue<any>>(transform: PTransform<Root, OutputT>): OutputT {
        return new Root(this).apply(transform);
    }

    apply2<InputT extends PValue<any>, OutputT extends PValue<any>>(transform: PTransform<InputT, OutputT>, pvalue: InputT, name: string) {

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
            uniqueName: transformId + this.transformStack.map((id) => this_.proto?.components?.transforms![id].uniqueName).concat([name || transform.name]).join('/'),
            subtransforms: [],
            inputs: objectMap(flattenPValue(pvalue), (pc) => pc.id),
            outputs: {},
            environmentId: "",
            displayData: [],
            annotations: {},
        }
        this.proto.components!.transforms![transformId] = transformProto;
        this.transformStack.push(transformId);
        const result = transform.expandInternal(this, transformProto, pvalue); // TODO: try-catch
        this.transformStack.pop();
        transformProto.outputs = objectMap(flattenPValue(result), (pc) => pc.id);

        // Propagate any unset PCollection properties.
        const inputProtos = Object.values(transformProto.inputs).map((id) => this_.proto.components!.pcollections[id]);
        const inputBoundedness = new Set(inputProtos.map((proto) => proto.isBounded));
        const inputWindowings = new Set(inputProtos.map((proto) => proto.windowingStrategyId));

        function onlyValueOr<T>(valueSet: Set<T>, defaultValue: T) {
            if (valueSet.size == 0) {
                return defaultValue;
            } else if (valueSet.size == 1) {
                return valueSet.values().next().value;
            } else {
                throw new Error('Unable to deduce single value from ' + valueSet);
            }
        }

        for (const pcId of Object.values(transformProto.outputs)) {
            const pcProto = this.proto!.components!.pcollections[pcId];
            if (!pcProto.isBounded) {
                pcProto.isBounded = onlyValueOr(inputBoundedness, runnerApi.IsBounded_Enum.BOUNDED);
            }
            if (!pcProto.windowingStrategyId) {
                pcProto.windowingStrategyId = onlyValueOr(inputWindowings, this.globalWindowing);
            }
        }

        return result;
    }

    createPCollectionInternal(
        coder: Coder<any> | string,
        windowingStrategy: runnerApi.WindowingStrategy | undefined = undefined,
        isBounded: runnerApi.IsBounded_Enum | undefined = undefined) {
        const pcollId = pcollectionName();
        let coderId: string;
        let windowingStrategyId: string;
        if (typeof coder == "string") {
            coderId = coder;
        } else {
            coderId = this.context.getCoderId(coder);
        }
        if (windowingStrategy == undefined) {
            windowingStrategyId = undefined!;
        } else if (typeof windowingStrategy == "string") {
            windowingStrategyId = windowingStrategy;
        } else {
            windowingStrategyId = this.context.getWindowingStrategyId(windowingStrategy!);
        }
        this.proto!.components!.pcollections[pcollId] = {
            uniqueName: pcollId, // TODO: name according to producing transform?
            coderId: coderId,
            isBounded: isBounded!,
            windowingStrategyId: windowingStrategyId,
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

    apply<OutputT extends PValue<any>>(transform: PTransform<PCollection<T>, OutputT> | ((PCollection) => OutputT)) {
        if (!(transform instanceof PTransform)) {
            transform = new PTransformFromCallable(transform, "" + transform);
        }
        return this.pipeline.apply2(transform, this, "");
    }

    map<OutputT>(fn: (T) => OutputT): PCollection<OutputT> {
        // TODO(robertwb): Should PTransforms have generics?
        return this.apply(new ParDo(new MapDoFn(fn))) as PCollection<any>;
    }

    flatMap<OutputT>(fn: (T) => Generator<OutputT>): PCollection<OutputT> {
        return this.apply(new ParDo(new FlatMapDoFn(fn))) as PCollection<OutputT>;
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

    apply<OutputT extends PValue<any>>(transform: PTransform<Root, OutputT> | ((Root) => OutputT)) {
        if (!(transform instanceof PTransform)) {
            transform = new PTransformFromCallable(transform, "" + transform);
        }
        return this.pipeline.apply2(transform, this, "");
    }
}

export type PValue<T> =    void | Root | PCollection<T> | PValue<T>[] | { [key: string]: PValue<T> };

function flattenPValue<T>(PValue: PValue<T>, prefix: string = ""): { [key: string]: PCollection<T> } {
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

class PValueWrapper<T extends PValue<any>> {
    constructor(private pvalue: T) { }
    apply<O extends PValue<any>>(transform: PTransform<T, O>, root: Root | null = null) {
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

export function P<T extends PValue<any>>(pvalue: T) {
    return new PValueWrapper(pvalue);
}

export class PTransform<InputT extends PValue<any>, OutputT extends PValue<any>> {
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

class PTransformFromCallable<InputT extends PValue<any>, OutputT extends PValue<any>> extends PTransform<InputT, OutputT> {
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

interface CombineFn<I, A, O> {
    createAccumulator: () => A;
    addInput: (A, I) => A;
    mergeAccumulators: (accumulators: A[]) => A;
    extractOutput: (A) => O;
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

    expandInternal(pipeline: Pipeline, transformProto: runnerApi.PTransform, input: Root): PCollection<Uint8Array> {
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


export class Flatten<T> extends PTransform<PCollection<T>[], PCollection<T>> {
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

export type Instant = Long;

export interface BoundedWindow {
    maxTimestamp(): Instant
}

export interface WindowedValue<T> {
    value: T;
    windows: Array<BoundedWindow>;
    pane: PaneInfo;
    timestamp: Instant;
}

export class IntervalWindow implements BoundedWindow {
    constructor(public start: Instant, public end: Instant) { }
    maxTimestamp() { return this.end.sub(1) }
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
