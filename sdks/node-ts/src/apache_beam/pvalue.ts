import { Pipeline } from "./base";
import { PTransform, AsyncPTransform } from "./transforms/transform";
import { ParDo, DoFn } from "./transforms/pardo";

/**
 * The base object on which one can start building a Beam DAG.
 * Generally followed by a source-like transform such as a read or impulse.
 */
export class Root {
  pipeline: Pipeline;

  constructor(pipeline: Pipeline) {
    this.pipeline = pipeline;
  }

  apply<OutputT extends PValue<any>>(
    transform: PTransform<Root, OutputT> | ((Root) => OutputT)
  ) {
    if (!(transform instanceof PTransform)) {
      transform = new PTransformFromCallable(transform, "" + transform);
    }
    return this.pipeline.applyTransform(transform, this, "");
  }

  async asyncApply<OutputT extends PValue<any>>(
    transform: AsyncPTransform<Root, OutputT> | ((Root) => Promise<OutputT>)
  ) {
    if (!(transform instanceof AsyncPTransform)) {
      transform = new AsyncPTransformFromCallable(transform, "" + transform);
    }
    return await this.pipeline.asyncApplyTransform(transform, this, "");
  }
}

/**
 * A deferred, possibly distributed collection of elements.
 */
export class PCollection<T> {
  type: string = "pcollection";
  pipeline: Pipeline;
  private id: string;
  private computeId: () => string;

  constructor(pipeline: Pipeline, id: string | (() => string)) {
    this.pipeline = pipeline;
    if (typeof id == "string") {
      this.id = id;
    } else {
      this.computeId = id;
    }
  }

  getId(): string {
    if (this.id == undefined) {
      this.id = this.computeId();
    }
    return this.id;
  }

  apply<OutputT extends PValue<any>>(
    transform: PTransform<PCollection<T>, OutputT> | ((PCollection) => OutputT)
  ) {
    if (!(transform instanceof PTransform)) {
      transform = new PTransformFromCallable(transform, "" + transform);
    }
    return this.pipeline.applyTransform(transform, this, "");
  }

  asyncApply<OutputT extends PValue<any>>(
    transform:
      | AsyncPTransform<PCollection<T>, OutputT>
      | ((PCollection) => Promise<OutputT>)
  ) {
    if (!(transform instanceof AsyncPTransform)) {
      transform = new AsyncPTransformFromCallable(transform, "" + transform);
    }
    return this.pipeline.asyncApplyTransform(transform, this, "");
  }

  map<OutputT, ContextT>(
    fn:
      | (ContextT extends undefined ? (element: T) => OutputT : never)
      | ((element: T, context: ContextT) => OutputT),
    context: ContextT = undefined!
  ): PCollection<OutputT> {
    return this.apply(
      new ParDo<T, OutputT, ContextT>(
        new MapDoFn<T, OutputT, ContextT>(fn),
        context
      )
    );
  }

  flatMap<OutputT, ContextT>(
    fn:
      | (ContextT extends undefined ? (element: T) => Iterable<OutputT> : never)
      | ((element: T, context: ContextT) => Iterable<OutputT>),
    context: ContextT = undefined!
  ): PCollection<OutputT> {
    return this.apply(
      new ParDo<T, OutputT, ContextT>(
        new FlatMapDoFn<T, OutputT, ContextT>(fn),
        context
      )
    );
  }

  root(): Root {
    return new Root(this.pipeline);
  }
}

/**
 * The type of object that may be consumed or produced by a PTransform.
 */
export type PValue<T> =
  | void
  | Root
  | PCollection<T>
  | PValue<T>[]
  | { [key: string]: PValue<T> };

/**
 * Returns a PValue as a flat object with string keys and PCollection values.
 *
 * The full set of PCollections reachable by this PValue will be returned,
 * with keys corresponding roughly to the path taken to get there.
 */
export function flattenPValue<T>(
  pValue: PValue<T>,
  prefix: string = ""
): { [key: string]: PCollection<T> } {
  const result: { [key: string]: PCollection<any> } = {};
  if (pValue == null) {
    // pass
  } else if (pValue instanceof Root) {
    // pass
  } else if (pValue instanceof PCollection) {
    if (prefix) {
      result[prefix] = pValue;
    } else {
      result.main = pValue;
    }
  } else {
    if (prefix) {
      prefix += ".";
    }
    if (pValue instanceof Array) {
      for (var i = 0; i < pValue.length; i++) {
        Object.assign(result, flattenPValue(pValue[i], prefix + i));
      }
    } else {
      for (const [key, subValue] of Object.entries(pValue)) {
        Object.assign(result, flattenPValue(subValue, prefix + key));
      }
    }
  }
  return result;
}

/**
 * Wraps a PValue in a single object such that a transform can be applied to it.
 *
 * For example, Flatten takes a PCollection[] as input, but Array has no
 * apply(PTransform) method, so one writes
 *
 *    P([pcA, pcB, pcC]).apply(new Flatten())
 */
export function P<T extends PValue<any>>(pvalue: T) {
  return new PValueWrapper(pvalue);
}

class PValueWrapper<T extends PValue<any>> {
  constructor(private pvalue: T) {}

  apply<O extends PValue<any>>(
    transform: PTransform<T, O>,
    root: Root | null = null
  ) {
    return this.pipeline(root).applyTransform(transform, this.pvalue, "");
  }

  async asyncApply<O extends PValue<any>>(
    transform: AsyncPTransform<T, O>,
    root: Root | null = null
  ) {
    return await this.pipeline(root).asyncApplyTransform(
      transform,
      this.pvalue,
      ""
    );
  }

  private pipeline(root: Root | null = null) {
    if (root == null) {
      const flat = flattenPValue(this.pvalue);
      return Object.values(flat)[0].pipeline;
    } else {
      return root.pipeline;
    }
  }
}

class PTransformFromCallable<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> extends PTransform<InputT, OutputT> {
  expander: (InputT) => OutputT;

  constructor(expander: (InputT) => OutputT, name: string) {
    super(name);
    this.expander = expander;
  }

  expand(input: InputT) {
    return this.expander(input);
  }
}

class AsyncPTransformFromCallable<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> extends AsyncPTransform<InputT, OutputT> {
  expander: (InputT) => Promise<OutputT>;

  constructor(expander: (InputT) => Promise<OutputT>, name: string) {
    super(name);
    this.expander = expander;
  }

  async asyncExpand(input: InputT) {
    return this.expander(input);
  }
}

// TODO: If this is exported, should probably move out of this module.
class MapDoFn<InputT, OutputT, ContextT> extends DoFn<
  InputT,
  OutputT,
  ContextT
> {
  private fn: (element: InputT, context: ContextT) => OutputT;
  constructor(fn: (element: InputT, context: ContextT) => OutputT) {
    super();
    this.fn = fn;
  }
  *process(element: InputT, context: ContextT) {
    // While it's legal to call a function with extra arguments which will
    // be ignored, this can have surprising behavior (e.g. for map(console.log))
    yield context == undefined
      ? (this.fn as (InputT) => OutputT)(element)
      : this.fn(element, context);
  }
}

// TODO: If this is exported, should probably move out of this module.
class FlatMapDoFn<InputT, OutputT, ContextT> extends DoFn<
  InputT,
  OutputT,
  ContextT
> {
  private fn: (element: InputT, context: ContextT) => Iterable<OutputT>;
  constructor(fn: (element: InputT, context: ContextT) => Iterable<OutputT>) {
    super();
    this.fn = fn;
  }
  *process(element: InputT, context: ContextT) {
    yield* this.fn(element, context);
  }
}
