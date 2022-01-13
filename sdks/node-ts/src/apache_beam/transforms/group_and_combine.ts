import { KV } from "../values";
import { PTransform } from "./transform";
import { PCollection } from "../pvalue";
import { GroupByKey, CombinePerKey } from "./internal";
import { CountFn } from "./combiners";

// TODO: Consider groupBy as a top-level method on PCollections.
// TBD how to best express the combiners.
//     - Idea 1: We could allow these as extra arguments to groupBy
//     - Idea 2: We could return a special GroupedPCollection that has a nice,
//               chain-able combining() method. We'd want the intermediates to
//               still be usable, but lazy.

export interface CombineFn<I, A, O> {
  createAccumulator: () => A;
  addInput: (A, I) => A;
  mergeAccumulators: (accumulators: Iterable<A>) => A;
  extractOutput: (A) => O;
}

// TODO: When typing this as ((a: I, b: I) => I), types are not inferred well.
type Combiner<I> = CombineFn<I, any, any> | ((a: any, b: any) => any);

/**
 * A PTransform that takes a PCollection of elements, and returns a PCollection
 * of elements grouped by a field, multiple fields, an expression that is used
 * as the grouping key.
 *
 * @extends PTransform
 */
export class GroupBy<T, K> extends PTransform<
  PCollection<T>,
  PCollection<KV<K, Iterable<T>>>
> {
  keyFn: (element: T) => K;
  keyNames: string | string[];
  keyName: string;

  /**
   * Create a GroupBy transform.
   *
   * @param key: The name of the key in the JSON object, or a function that returns the key for a given element.
   */
  constructor(
    key: string | string[] | ((element: T) => K),
    keyName: string | undefined = undefined
  ) {
    super();
    [this.keyFn, this.keyNames] = extractFnAndName(key, keyName || "key");
    this.keyName = typeof this.keyNames == "string" ? this.keyNames : "key";
  }

  expand(input: PCollection<T>): PCollection<KV<K, Iterable<T>>> {
    const this_ = this;
    return input
      .map(function (x) {
        return { key: this_.keyFn(x), value: x };
      })
      .apply(new GroupByKey());
  }

  combining<I>(
    expr: string | ((element: T) => I),
    combiner: Combiner<I>,
    resultName: string
  ) {
    return new GroupByAndCombine(this.keyFn, this.keyNames, []).combining(
      expr,
      combiner,
      resultName
    );
  }
}

/**
 * Groups all elements of the input PCollection together.
 *
 * This is generally used with one or more combining specifications, as one
 * looses parallelization benefits in bringing all elements of a distributed
 * PCollection together on a single machine.
 */
export class GroupGlobally<T> extends PTransform<
  PCollection<T>,
  PCollection<Iterable<T>>
> {
  constructor() {
    super();
  }

  expand(input) {
    return input.apply(new GroupBy((_) => null)).map((kv) => kv[1]);
  }

  combining<I>(
    expr: string | ((element: T) => I),
    combiner: Combiner<I>,
    resultName: string
  ) {
    return new GroupByAndCombine((_) => null, undefined, []).combining(
      expr,
      combiner,
      resultName
    );
  }
}

class GroupByAndCombine<T, O> extends PTransform<
  PCollection<T>,
  PCollection<O>
> {
  keyFn: (element: T) => any;
  keyNames: string | string[] | undefined;
  combiners: CombineSpec<T, any, any>[];
  constructor(
    keyFn: (element: T) => any,
    keyNames: string | string[] | undefined,
    combiners: CombineSpec<T, any, any>[]
  ) {
    super();
    this.keyFn = keyFn;
    this.keyNames = keyNames;
    this.combiners = combiners;
  }

  // TODO: or name this combine?
  combining<I, O>(
    expr: string | ((element: T) => I),
    combiner: Combiner<I>,
    resultName: string // TODO: Optionally derive from expr and combineFn?
  ) {
    return new GroupByAndCombine(
      this.keyFn,
      this.keyNames,
      this.combiners.concat([
        {
          expr: extractFn(expr),
          combineFn: toCombineFn(combiner),
          resultName: resultName,
        },
      ])
    );
  }

  expand(input: PCollection<T>) {
    const this_ = this;
    return input
      .map(function (element) {
        return {
          key: this_.keyFn(element),
          value: this_.combiners.map((c) => c.expr(element)),
        };
      })
      .apply(
        new CombinePerKey(
          new MultiCombineFn(this_.combiners.map((c) => c.combineFn))
        )
      )
      .map(function (kv) {
        const result = {};
        if (this_.keyNames == undefined) {
          // Don't populate a key at all.
        } else if (typeof this_.keyNames == "string") {
          result[this_.keyNames] = kv.key;
        } else {
          for (let i = 0; i < this_.keyNames.length; i++) {
            result[this_.keyNames[i]] = kv.key[i];
          }
        }
        for (let i = 0; i < this_.combiners.length; i++) {
          result[this_.combiners[i].resultName] = kv.value[i];
        }
        return result;
      });
  }
}

// TODO: Does this carry its weight as a top-level built-in function?
// Cons: It's just a combine. Pros: It's kind of a non-obvious one.
// NOTE: The encoded form of the elements will be used for equality checking.
export class CountPerElement<T> extends PTransform<
  PCollection<T>,
  PCollection<{ element: T; count: number }>
> {
  expand(input) {
    return input.apply(
      new GroupBy((e) => e, "element").combining(
        (e) => e,
        new CountFn(),
        "count"
      )
    );
  }
}

export class CountGlobally<T> extends PTransform<
  PCollection<T>,
  PCollection<number>
> {
  expand(input) {
    return input
      .apply(new GroupGlobally().combining((e) => e, new CountFn(), "count"))
      .map((o) => o.count);
  }
}

function toCombineFn<I>(combiner: Combiner<I>): CombineFn<I, any, any> {
  if (typeof combiner == "function") {
    return new BinaryCombineFn<I>(combiner);
  } else {
    return combiner;
  }
}

interface CombineSpec<T, I, O> {
  expr: (T) => I;
  combineFn: CombineFn<I, any, O>;
  resultName: string;
}

class BinaryCombineFn<I> implements CombineFn<I, I | undefined, I> {
  constructor(private combiner: (a: I, b: I) => I) {}
  createAccumulator() {
    return undefined;
  }
  addInput(a, b) {
    if (a == undefined) {
      return b;
    } else {
      return this.combiner(a, b);
    }
  }
  mergeAccumulators(accs) {
    return accs.filter((a) => a != undefined).reduce(this.combiner, undefined);
  }
  extractOutput(a) {
    return a;
  }
}

class MultiCombineFn implements CombineFn<any[], any[], any[]> {
  batchSize: number = 100;
  // TODO: Is there a way to indicate type parameters match the above?
  constructor(private combineFns: CombineFn<any, any, any>[]) {}

  createAccumulator() {
    return this.combineFns.map((fn) => fn.createAccumulator());
  }

  addInput(accumulators: any[], inputs: any[]) {
    // TODO: zip?
    let result: any[] = [];
    for (let i = 0; i < this.combineFns.length; i++) {
      result.push(this.combineFns[i].addInput(accumulators[i], inputs[i]));
    }
    return result;
  }

  mergeAccumulators(accumulators: Iterable<any[]>) {
    const combineFns = this.combineFns;
    let batches = combineFns.map((fn) => [fn.createAccumulator()]);
    for (let acc of accumulators) {
      for (let i = 0; i < combineFns.length; i++) {
        batches[i].push(acc[i]);
        if (batches[i].length > this.batchSize) {
          batches[i] = [combineFns[i].mergeAccumulators(batches[i])];
        }
      }
    }
    for (let i = 0; i < combineFns.length; i++) {
      if (batches[i].length > 1) {
        batches[i] = [combineFns[i].mergeAccumulators(batches[i])];
      }
    }
    return batches.map((batch) => batch[0]);
  }

  extractOutput(accumulators: any[]) {
    // TODO: zip?
    let result: any[] = [];
    for (let i = 0; i < this.combineFns.length; i++) {
      result.push(this.combineFns[i].extractOutput(accumulators[i]));
    }
    return result;
  }
}

// TODO: Can I type T as "something that has this key" and/or, even better,
// ensure it has the correct type?
function extractFnAndName<T, K>(
  extractor: string | string[] | ((T) => K),
  defaultName: string
): [(T) => K, string | string[]] {
  if (typeof extractor == "string") {
    return [(element: T) => element[extractor], extractor];
  } else if (extractor instanceof Array) {
    return [
      (element: T) => extractor.map((field) => element[field]) as any,
      extractor,
    ];
  } else {
    return [extractor, defaultName];
  }
}

function extractFn<T, K>(extractor: string | string[] | ((T) => K)) {
  return extractFnAndName(extractor, undefined!)[0];
}
