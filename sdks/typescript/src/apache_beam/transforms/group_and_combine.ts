/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { KV } from "../values";
import {
  PTransform,
  PTransformClass,
  withName,
  extractName,
} from "./transform";
import { flatten } from "./flatten";
import { PCollection } from "../pvalue";
import { P } from "../pvalue";
import { Coder } from "../coders/coders";
import * as internal from "./internal";
import { count } from "./combiners";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";

// TODO: (API) Consider groupBy as a top-level method on PCollections.
// TBD how to best express the combiners.
//     - Idea 1: We could allow these as extra arguments to groupBy
//     - Idea 2: We could return a special GroupedPCollection that has a nice,
//               chain-able combining() method. We'd want the intermediates to
//               still be usable, but lazy.

/**
 * An interface for performing possibly distributed, commutative, associative
 * combines (such as sum) to a set of values
 * (e.g. in `groupBy(...).combining(...)`.
 *
 * Several implementations (such as summation) are provided in the combiners
 * module.
 *
 * See also https://beam.apache.org/documentation/programming-guide/#transforms
 */
export interface CombineFn<I, A, O> {
  createAccumulator: () => A;
  addInput: (A, I) => A;
  mergeAccumulators: (accumulators: Iterable<A>) => A;
  extractOutput: (A) => O;
  accumulatorCoder?(inputCoder: Coder<I>): Coder<A>;
}

// TODO: (Typescript) When typing this as ((a: I, b: I) => I), types are not inferred well.
type Combiner<I> = CombineFn<I, any, any> | ((a: any, b: any) => any);

/**
 * A PTransformClass that takes a PCollection of elements, and returns a PCollection
 * of elements grouped by a field, multiple fields, an expression that is used
 * as the grouping key.
 *
 * @extends PTransformClass
 */
export class GroupBy<T, K> extends PTransformClass<
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
    keyName: string | undefined = undefined,
  ) {
    super();
    [this.keyFn, this.keyNames] = extractFnAndName(key, keyName || "key");
    // XXX: Actually use this.
    this.keyName = typeof this.keyNames === "string" ? this.keyNames : "key";
  }

  /** @internal */
  expand(input: PCollection<T>): PCollection<KV<K, Iterable<T>>> {
    const keyFn = this.keyFn;
    return input
      .map((x) => ({ key: keyFn(x), value: x }))
      .apply(internal.groupByKey());
  }

  combining<I>(
    expr: string | ((element: T) => I),
    combiner: Combiner<I>,
    resultName: string,
  ) {
    return withName(
      extractName(this),
      new GroupByAndCombine(this.keyFn, this.keyNames, []).combining(
        expr,
        combiner,
        resultName,
      ),
    );
  }
}

/**
 * Returns a PTransform that takes a PCollection of elements, and returns a
 * PCollection of elements grouped by a field, multiple fields, an expression
 * that is used as the grouping key.
 *
 * Various fields may be further aggregated with `CombineFns` by invoking
 * `groupBy(...).combining(...)`.
 */
export function groupBy<T, K>(
  key: string | string[] | ((element: T) => K),
  keyName: string | undefined = undefined,
): GroupBy<T, K> {
  return withName(
    `groupBy(${extractName(key)}`,
    new GroupBy<T, K>(key, keyName),
  );
}

/**
 * Groups all elements of the input PCollection together.
 *
 * This is generally used with one or more combining specifications, as one
 * loses parallelization benefits in bringing all elements of a distributed
 * PCollection together on a single machine.
 */
export class GroupGlobally<T> extends PTransformClass<
  PCollection<T>,
  PCollection<Iterable<T>>
> {
  constructor() {
    super();
  }

  /** @internal */
  expand(input) {
    return input.apply(new GroupBy((_) => null)).map((kv) => kv[1]);
  }

  combining<I>(
    expr: string | ((element: T) => I),
    combiner: Combiner<I>,
    resultName: string,
  ) {
    return withName(
      extractName(this),
      new GroupByAndCombine((_) => null, undefined, []).combining(
        expr,
        combiner,
        resultName,
      ),
    );
  }
}

/**
 * Returns a PTransform grouping all elements of the input PCollection together.
 *
 * This is generally used with one or more combining specifications, as one
 * loses parallelization benefits in bringing all elements of a distributed
 * PCollection together on a single machine.
 *
 * Various fields may be further aggregated with `CombineFns` by invoking
 * `groupGlobally(...).combining(...)`.
 */
export function groupGlobally<T>() {
  return new GroupGlobally<T>();
}

class GroupByAndCombine<T, O> extends PTransformClass<
  PCollection<T>,
  PCollection<O>
> {
  keyFn: (element: T) => any;
  keyNames: string | string[] | undefined;
  combiners: CombineSpec<T, any, any>[];
  constructor(
    keyFn: (element: T) => any,
    keyNames: string | string[] | undefined,
    combiners: CombineSpec<T, any, any>[],
  ) {
    super();
    this.keyFn = keyFn;
    this.keyNames = keyNames;
    this.combiners = combiners;
  }

  // TODO: (Naming) Name this combine?
  combining<I, O>(
    expr: string | ((element: T) => I),
    combiner: Combiner<I>,
    resultName: string, // TODO: (Unique names) Optionally derive from expr and combineFn?
  ) {
    return withName(
      extractName(this),
      new GroupByAndCombine(
        this.keyFn,
        this.keyNames,
        this.combiners.concat([
          {
            expr: extractFn(expr),
            combineFn: toCombineFn(combiner),
            resultName: resultName,
          },
        ]),
      ),
    );
  }

  expand(input: PCollection<T>) {
    const this_ = this;
    return input
      .map(function extractKeys(element) {
        return {
          key: this_.keyFn(element),
          value: this_.combiners.map((c) => c.expr(element)),
        };
      })
      .apply(
        internal.combinePerKey(
          multiCombineFn(this_.combiners.map((c) => c.combineFn)),
        ),
      )
      .map(function constructResult(kv) {
        const result = {};
        if (this_.keyNames === null || this_.keyNames === undefined) {
          // Don't populate a key at all.
        } else if (typeof this_.keyNames === "string") {
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

export function countPerElement<T>(): PTransform<
  PCollection<T>,
  PCollection<{ element: T; count: number }>
> {
  return withName(
    "countPerElement",
    groupBy((e) => e, "element").combining((e) => e, count, "count"),
  );
}

export function countGlobally<T>(): PTransform<
  PCollection<T>,
  PCollection<number>
> {
  return withName("countGlobally", (input) =>
    input
      .apply(new GroupGlobally().combining((e) => e, count, "count"))
      .map((o) => o.count),
  );
}

function toCombineFn<I>(combiner: Combiner<I>): CombineFn<I, any, any> {
  if (typeof combiner === "function") {
    return binaryCombineFn<I>(combiner);
  } else {
    return combiner;
  }
}

interface CombineSpec<T, I, O> {
  expr: (T) => I;
  combineFn: CombineFn<I, any, O>;
  resultName: string;
}

/**
 * Creates a CombineFn<I, ..., I> out of a binary operator (which must be
 * commutative and associative).
 */
export function binaryCombineFn<I>(
  combiner: (a: I, b: I) => I,
): CombineFn<I, I | undefined, I> {
  return {
    createAccumulator: () => undefined,
    addInput: (a, b) => (a === undefined ? b : combiner(a, b)),
    mergeAccumulators: (accs) =>
      (<I[]>([...accs].filter((a) => a !== null && a !== undefined))).reduce(combiner),
    extractOutput: (a) => a,
  };
}

// TODO: (Typescript) Is there a way to indicate type parameters match the above?
function multiCombineFn(
  combineFns: CombineFn<any, any, any>[],
  batchSize: number = 100,
): CombineFn<any[], any[], any[]> {
  return {
    createAccumulator: () => combineFns.map((fn) => fn.createAccumulator()),

    addInput: (accumulators: any[], inputs: any[]) => {
      // TODO: (Cleanup) Does javascript have a clean zip?
      let result: any[] = [];
      for (let i = 0; i < combineFns.length; i++) {
        result.push(combineFns[i].addInput(accumulators[i], inputs[i]));
      }
      return result;
    },

    mergeAccumulators: (accumulators: Iterable<any[]>) => {
      let batches = combineFns.map((fn) => [fn.createAccumulator()]);
      for (let acc of accumulators) {
        for (let i = 0; i < combineFns.length; i++) {
          batches[i].push(acc[i]);
          if (batches[i].length > batchSize) {
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
    },

    extractOutput: (accumulators: any[]) => {
      // TODO: (Cleanup) Does javascript have a clean zip?
      let result: any[] = [];
      for (let i = 0; i < combineFns.length; i++) {
        result.push(combineFns[i].extractOutput(accumulators[i]));
      }
      return result;
    },
  };
}

// TODO: Consider adding valueFn(s) rather than using the full value.
export function coGroupBy<T, K>(
  key: string | string[] | ((element: T) => K),
  keyName: string | undefined = undefined,
): PTransform<
  { [key: string]: PCollection<any> },
  PCollection<{ key: K; values: { [key: string]: Iterable<any> } }>
> {
  return withName(
    `coGroupBy(${extractName(key)})`,
    function coGroupBy(inputs: { [key: string]: PCollection<any> }) {
      const [keyFn, keyNames] = extractFnAndName(key, keyName || "key");
      keyName = typeof keyNames === "string" ? keyNames : "key";
      const tags = [...Object.keys(inputs)];
      const tagged = [...Object.entries(inputs)].map(([tag, pcoll]) =>
        pcoll.map(
          withName(`map[${tag}]`, (element) => ({
            key: keyFn(element),
            tag,
            element,
          })),
        ),
      );
      return P(tagged)
        .apply(flatten())
        .apply(groupBy("key"))
        .map(function groupValues({ key, value }) {
          const groupedValues: { [key: string]: any[] } = Object.fromEntries(
            tags.map((tag) => [tag, []]),
          );
          for (const { tag, element } of value) {
            groupedValues[tag].push(element);
          }
          return { key, values: groupedValues };
        });
    },
  );
}

// TODO: (Typescript) Can I type T as "something that has this key" and/or,
// even better, ensure it has the correct type?
// Should be possible to get rid of the cast somehow.
// function extractFnAndName<T, P extends keyof T, K = T[P]>(
//   extractor: P | P[] | ((element: T) => K),
//   defaultName: P
// ): [(element: T) => K, P | P[]] {
function extractFnAndName<T, K>(
  extractor: string | string[] | ((T) => K),
  defaultName: string,
): [(T) => K, string | string[]] {
  if (
    typeof extractor === "string" ||
    typeof extractor === "number" ||
    typeof extractor === "symbol"
  ) {
    return [(element: T) => element[extractor] as unknown as K, extractor];
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

requireForSerialization(`${packageName}/transforms/group_and_combine`, exports);
requireForSerialization(`${packageName}/transforms/group_and_combine`, {
  GroupByAndCombine: GroupByAndCombine,
});
