import * as runnerApi from "../proto/beam_runner_api";
import * as urns from "../internal/urns";

import { GeneralObjectCoder } from "../coders/js_coders";
import { PCollection } from "../pvalue";
import { Pipeline, fakeSeralize } from "../base";
import { PTransform } from "./transform";
import { BoundedWindow, WindowedValue } from "../values";

export class DoFn<InputT, OutputT, ContextT = undefined> {
  *process(element: InputT, context: ContextT): Iterable<OutputT> | void {
    throw new Error("Method process has not been implemented!");
  }

  // TODO: Should these get context as well?
  startBundle() {}

  // TODO: Re-consider this API.
  *finishBundle(): Generator<WindowedValue<OutputT>> | void {}
}

// TODO: Do we need an AsyncDoFn (and async[Flat]Map) to be able to call async
// functions in the body of the fns. Or can they always be Async?
// (For PTransforms, it's a major usability issue, but maybe we can always
// await when calling user code.  OTOH, I don't know what the performance
// impact would be for creating promises for every element of every operation
// which is typically a very performance critical spot to optimize.)


export class ParDo<InputT, OutputT, ContextT = undefined> extends PTransform<
  PCollection<InputT>,
  PCollection<OutputT>
> {
  private doFn: DoFn<InputT, OutputT, ContextT>;
  private context: ContextT;
  // static urn: string = runnerApi.StandardPTransforms_Primitives.PAR_DO.urn;
  // TODO: use above line, not below line.
  static urn: string = "beam:transform:pardo:v1";
  // TODO: Can the arg be optional iff ContextT is undefined?
  constructor(
    doFn: DoFn<InputT, OutputT, ContextT>,
    context: ContextT = undefined!
  ) {
    super("ParDo(" + doFn + ")");
    this.doFn = doFn;
    this.context = context;
  }

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<InputT>
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SERIALIZED_JS_DOFN_INFO,
            payload: fakeSeralize({
              doFn: this.doFn,
              context: this.context,
            }),
          }),
        })
      ),
    });

    // For the ParDo output coder, we use a GeneralObjectCoder, which is a Javascript-specific
    // coder to encode the various types that exist in JS.
    return pipeline.createPCollectionInternal<OutputT>(
      new GeneralObjectCoder()
    );
  }
}

// TODO: Consider as top-level method.
// TODO: Naming.
// TODO: Allow default?  Technically splitter can be implemented/wrapped to produce such.
// TODO: Can we enforce splitter's output with the typing system to lie in targets?
// TODO: (Optionally?) delete the switched-on field.
// TODO: Consider doing
//     [{a: aValue}, {g: bValue}, ...] => a: [aValue, ...], b: [bValue, ...]
// instead of
//     [{key: 'a', aValueFields}, {key: 'b', bValueFields}, ...] =>
//          a: [{key: 'a', aValueFields}, ...], b: [{key: 'b', aValueFields}, ...],
export class Split<T> extends PTransform<
  PCollection<T>,
  { [key: string]: PCollection<T> }
> {
  private tags: string[];
  constructor(private splitter: (T) => string, ...tags: string[]) {
    super("Split(" + tags + ")");
    this.tags = tags;
  }
  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<T>
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SPLITTING_JS_DOFN_URN,
            payload: fakeSeralize({ splitter: this.splitter }),
          }),
        })
      ),
    });

    const this_ = this;
    return Object.fromEntries(
      this_.tags.map((tag) => [
        tag,
        pipeline.createPCollectionInternal<T>(
          pipeline.context.getPCollectionCoderId(input)
        ),
      ])
    );
  }
}

export interface ParamProvider {
  provide(paramName: string): any;
}

abstract class ParDoParam<T> {
  private parDoParamName: string;

  // Provided externally.
  private provider: ParamProvider | undefined;

  constructor(parDoParamName: string) {
    this.parDoParamName = parDoParamName;
  }

  // TODO: Name? get seems to be special.
  lookup(): T {
    if (this.provider == undefined) {
      throw new Error("Cannot be called outside of a DoFn's process method.");
    }

    return this.provider.provide(this.parDoParamName);
  }
}

export class WindowParam extends ParDoParam<BoundedWindow> {
  constructor() {
    super("window");
  }
}

// TODO: Add providers for timestamp, paneinfo, state, timers,
// restriction trackers, counters, etc.
// TODO: Put side inputs here too.