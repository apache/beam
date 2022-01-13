import {
  PTransform,
  PCollection,
  Impulse,
  Root,
  CombineFn,
  DoFn,
} from "../base";
import * as translations from "../internal/translations";
import * as runnerApi from "../proto/beam_runner_api";
import { BytesCoder, KVCoder } from "../coders/standard_coders";

import { GroupByKey } from "../base";
import { GeneralObjectCoder } from "../coders/js_coders";
import { BoundedWindow, Instant, KV, PaneInfo } from "../values";
import { ParDo } from "..";
import { CombinePerKey } from "./combine";

/**
 * A Ptransform that represents a 'static' source with a list of elements passed at construction time. It
 * returns a PCollection that contains the elements in the input list.
 *
 * @extends PTransform
 */
export class Create<T> extends PTransform<Root, PCollection<T>> {
  elements: T[];

  /**
   * Construct a new Create PTransform.
   * @param elements - the list of elements in the PCollection
   */
  constructor(elements: T[]) {
    super("Create");
    this.elements = elements;
  }

  expand(root: Root) {
    const this_ = this;
    // TODO: Store encoded values and conditionally shuffle.
    return root.apply(new Impulse()).flatMap(function* (_) {
      yield* this_.elements;
    });
  }
}

/**
 * A PTransform that takes a PCollection of elements, and returns a PCollection of
 * elements grouped by a key.
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
  constructor(key: string | string[] | ((element: T) => K)) {
    super();
    [this.keyFn, this.keyNames] = extractFnAndName(key, "key");
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

class GroupByAndCombine<T, O> extends PTransform<
  PCollection<T>,
  PCollection<O>
> {
  keyFn: (element: T) => any;
  keyNames: string | string[];
  combiners: CombineSpec<T, any, any>[];
  constructor(
    keyFn: (element: T) => any,
    keyNames: string | string[],
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
        if (typeof this_.keyNames == "string") {
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

// TODO: When typing this as ((a: I, b: I) => I), types are not inferred well.
type Combiner<I> = CombineFn<I, any, any> | ((a: any, b: any) => any);

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

class KeyBy<InputT, KeyT> extends PTransform<
  PCollection<InputT>,
  PCollection<KV<KeyT, InputT>>
> {
  keyFn: (elm: InputT) => KeyT;
  constructor(keyFn: (elm: InputT) => KeyT) {
    super();
    this.keyFn = keyFn;
  }
  expand(input: PCollection<InputT>) {
    return input.map((elm) => ({ key: this.keyFn(elm), value: elm }));
  }
}

export function keyBy<InputT, KeyT>(keyFn: (elm: InputT) => KeyT) {
  return new KeyBy(keyFn);
}
