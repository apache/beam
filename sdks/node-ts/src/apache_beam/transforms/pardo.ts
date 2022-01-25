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
    // Extract side inputs.
    var context;
    const sideInputs = {};
    if (typeof this.context == "object") {
      context = Object.create(this.context as Object);
      for (const [name, value] of Object.entries(this.context)) {
        if (value instanceof SideInputParam) {
          const inputName = "side." + name;
          transformProto.inputs[inputName] = value.pcoll.getId();
          context[name] = copySideInputWithId(value, inputName);
          sideInputs[inputName] = {
            accessPattern: {
              urn: value.accessor.accessPattern,
              payload: new Uint8Array(),
            },
            // TODO: The viewFn is stored in the side input object. Unclear what benefit there is to putting it here.
            viewFn: { urn: "unused", payload: new Uint8Array() },
            // TODO: Possibly place this in the accessor.
            // Default should be mapping of endpoint to window according to the
            // windowFn of the side input PCollection.
            // TODO: In the short term, at least check that they agree.
            windowMappingFn: {
              urn: "beam:window_mapping_fn:identity:v1",
              value: new Uint8Array(),
            },
          };
        } else {
          context[name] = value;
        }
      }
    } else {
      context = this.context;
    }

    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SERIALIZED_JS_DOFN_INFO,
            payload: fakeSeralize({
              doFn: this.doFn,
              context: context,
            }),
          }),
          sideInputs: sideInputs,
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
  provide<T>(param: ParDoParam<T>): T;
}

export abstract class ParDoParam<T> {
  readonly parDoParamName: string;

  // Provided externally.
  private provider: ParamProvider | undefined;

  constructor(parDoParamName: string) {
    this.parDoParamName = parDoParamName;
  }

  // TODO: Name? "get" seems to be special.
  lookup(): T {
    if (this.provider == undefined) {
      throw new Error("Cannot be called outside of a DoFn's process method.");
    }

    return this.provider.provide(this);
  }
}

interface SideInputAccessor<PCollT, AccessorT, ValueT> {
  // This should be a value of runnerApi.StandardSideInputTypes, and specifies
  // the relationship between PCollT (the type fo the PCollection's elements)
  // and AccessorT (the type returned when fetching side inputs).
  accessPattern: string;
  // This transforms the runner type into the user's type.
  toValue: (AccessorT) => ValueT;
}

// TODO: Support side inputs that are composites of multiple more primitive
// side inputs.
export class SideInputParam<
  PCollT,
  AccessorT,
  ValueT
> extends ParDoParam<ValueT> {
  // Populated by user.
  pcoll: PCollection<any>;
  // Typically populated by subclass.
  accessor: SideInputAccessor<PCollT, AccessorT, ValueT>;

  constructor(
    pcoll: PCollection<PCollT>,
    accessor: SideInputAccessor<PCollT, AccessorT, ValueT>
  ) {
    super("sideInput");
    this.pcoll = pcoll;
    this.accessor = accessor;
  }

  // Internal.
  // TODO: Consistency between name and id.
  sideInputId: string;
}

function copySideInputWithId<PCollT, AccessorT, ValueT>(
  sideInput: SideInputParam<PCollT, AccessorT, ValueT>,
  id: string
): SideInputParam<PCollT, AccessorT, ValueT> {
  const copy = Object.create(sideInput);
  copy.sideInputId = id;
  delete copy.pcoll;
  return copy;
}

export class IterableSideInput<T> extends SideInputParam<
  T,
  Iterable<T>,
  Iterable<T>
> {
  constructor(pcoll: PCollection<T>) {
    super(pcoll, {
      accessPattern: "beam:side_input:iterable:v1",
      toValue: (iter: Iterable<T>) => iter,
    });
  }
}

export class SingletonSideInput<T> extends SideInputParam<T, Iterable<T>, T> {
  constructor(pcoll: PCollection<T>, defaultValue: T | undefined = undefined) {
    super(pcoll, {
      accessPattern: "beam:side_input:iterable:v1",
      toValue: (iter: Iterable<T>) => {
        const asArray = Array.from(iter);
        if (asArray.length == 0 && defaultValue != undefined) {
          return defaultValue;
        } else if (asArray.length == 1) {
          return asArray[0];
        } else {
          throw new Error("Expected a single element, got " + asArray.length);
        }
      },
    });
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
