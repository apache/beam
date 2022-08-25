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

import * as beam from "../src/apache_beam";
import { createRunner } from "../src/apache_beam/runners/runner";
import * as testing from "../src/apache_beam/testing/assert";
import { KV } from "../src/apache_beam/values";

import * as combiners from "../src/apache_beam/transforms/combiners";
import {
  CombineFn,
  groupBy,
  groupGlobally,
  countPerElement,
  countGlobally,
} from "../src/apache_beam/transforms/group_and_combine";

describe("Apache Beam combiners", function () {
  it("runs wordcount with a countPerKey transform and asserts the result", async function () {
    await createRunner().run((root) => {
      const lines = root.apply(
        beam.create([
          "In the beginning God created the heaven and the earth.",
          "And the earth was without form, and void; and darkness was upon the face of the deep.",
          "And the Spirit of God moved upon the face of the waters.",
          "And God said, Let there be light: and there was light.",
        ])
      );

      lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function* splitWords(line: string) {
          yield* line.split(/[^a-z]+/);
        })
        .map((elm) => ({ key: elm, value: 1 }))
        .apply(groupBy("key").combining("value", combiners.sum, "value"))
        .apply(
          testing.assertDeepEqual([
            { key: "in", value: 1 },
            { key: "the", value: 9 },
            { key: "beginning", value: 1 },
            { key: "god", value: 3 },
            { key: "created", value: 1 },
            { key: "heaven", value: 1 },
            { key: "and", value: 7 },
            { key: "earth", value: 2 },
            { key: "", value: 4 },
            { key: "was", value: 3 },
            { key: "without", value: 1 },
            { key: "form", value: 1 },
            { key: "void", value: 1 },
            { key: "darkness", value: 1 },
            { key: "upon", value: 2 },
            { key: "face", value: 2 },
            { key: "of", value: 3 },
            { key: "deep", value: 1 },
            { key: "spirit", value: 1 },
            { key: "moved", value: 1 },
            { key: "waters", value: 1 },
            { key: "said", value: 1 },
            { key: "let", value: 1 },
            { key: "there", value: 2 },
            { key: "be", value: 1 },
            { key: "light", value: 2 },
          ])
        );
    });
  });

  it("runs wordcount with a countGlobally transform and asserts the result", async function () {
    await createRunner().run((root) => {
      const lines = root.apply(
        beam.create(["And God said, Let there be light: and there was light"])
      );

      lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function* splitWords(line: string) {
          yield* line.split(/[^a-z]+/);
        })
        .apply(countGlobally())
        .apply(testing.assertDeepEqual([11]));
    });
  });

  it("runs an example where a custom combine function is passed", async function () {
    type StdDevAcc = {
      count: number;
      sum: number;
      sumOfSquares: number;
    };

    function unstableStdDevCombineFn(): CombineFn<number, StdDevAcc, number> {
      // NOTE: This Standard Deviation algorithm is **unstable**, so it is not recommended
      //    for an actual production-level pipeline.
      return {
        createAccumulator: function () {
          return { count: 0, sum: 0, sumOfSquares: 0 };
        },
        addInput: function (acc: StdDevAcc, inp: number) {
          return {
            count: acc.count + 1,
            sum: acc.sum + inp,
            sumOfSquares: acc.sumOfSquares + inp * inp,
          };
        },
        mergeAccumulators: function (accumulators: StdDevAcc[]) {
          return accumulators.reduce((previous, current) => ({
            count: previous.count + current.count,
            sum: previous.sum + current.sum,
            sumOfSquares: previous.sumOfSquares + current.sumOfSquares,
          }));
        },
        extractOutput: function (acc: StdDevAcc) {
          const mean = acc.sum / acc.count;
          return acc.sumOfSquares / acc.count - mean * mean;
        },
      };
    }

    await createRunner().run((root) => {
      const lines = root.apply(
        beam.create([
          "In the beginning God created the heaven and the earth.",
          "And the earth was without form, and void; and darkness was upon the face of the deep.",
          "And the Spirit of God moved upon the face of the waters.",
          "And God said, Let there be light: and there was light.",
        ])
      );

      lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function* splitWords(line: string) {
          yield* line.split(/[^a-z]+/);
        })
        .map((word) => word.length)
        .apply(
          groupGlobally()
            .combining((c) => c, combiners.mean, "mean")
            .combining((c) => c, unstableStdDevCombineFn(), "stdDev")
        )
        .apply(
          testing.assertDeepEqual([
            { mean: 3.611111111111111, stdDev: 3.2746913580246897 },
          ])
        );
    });
  });

  it("test GroupBy with combining", async function () {
    await createRunner().run((root) => {
      const inputs = root.apply(
        beam.create([
          { k: "k1", a: 1, b: 100 },
          { k: "k1", a: 2, b: 200 },
          { k: "k2", a: 9, b: 1000 },
        ])
      );

      inputs
        .apply(
          groupBy("k")
            .combining("a", combiners.max, "aMax")
            .combining("a", combiners.sum, "aSum")
            .combining("b", combiners.mean, "mean")
        )
        .apply(
          testing.assertDeepEqual([
            { k: "k1", aMax: 2, aSum: 3, mean: 150 },
            { k: "k2", aMax: 9, aSum: 9, mean: 1000 },
          ])
        );
    });
  });

  it("test GroupBy list with combining", async function () {
    await createRunner().run((root) => {
      const inputs = root.apply(
        beam.create([
          { a: 1, b: 10, c: 100 },
          { a: 2, b: 10, c: 100 },
          { a: 1, b: 10, c: 400 },
        ])
      );

      inputs
        .apply(groupBy(["a", "b"]).combining("c", combiners.sum, "sum"))
        .apply(
          testing.assertDeepEqual([
            { a: 1, b: 10, sum: 500 },
            { a: 2, b: 10, sum: 100 },
          ])
        );

      inputs
        .apply(groupBy(["b", "c"]).combining("a", combiners.sum, "sum"))
        .apply(
          testing.assertDeepEqual([
            { b: 10, c: 100, sum: 3 },
            { b: 10, c: 400, sum: 1 },
          ])
        );
    });
  });

  it("test GroupBy expr with combining", async function () {
    await createRunner().run((root) => {
      const inputs = root.apply(
        beam.create([
          { a: 1, b: 10 },
          { a: 0, b: 20 },
          { a: -1, b: 30 },
        ])
      );

      inputs
        .apply(
          groupBy((element: any) => element.a * element.a).combining(
            "b",
            combiners.sum,
            "sum"
          )
        )
        .apply(
          testing.assertDeepEqual([
            { key: 1, sum: 40 },
            { key: 0, sum: 20 },
          ])
        );
    });
  });

  it("test GroupBy with binary combinefn", async function () {
    await createRunner().run((root) => {
      const inputs = root.apply(
        beam.create([
          { key: 0, value: 10 },
          { key: 1, value: 20 },
          { key: 0, value: 30 },
        ])
      );

      inputs
        .apply(
          groupBy("key")
            .combining("value", (x, y) => x + y, "sum")
            .combining("value", (x, y) => Math.max(x, y), "max")
        )
        .apply(
          testing.assertDeepEqual([
            { key: 0, sum: 40, max: 30 },
            { key: 1, sum: 20, max: 20 },
          ])
        );
    });
  });
});
