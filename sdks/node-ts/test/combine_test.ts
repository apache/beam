import * as beam from "../src/apache_beam";
import { DirectRunner } from "../src/apache_beam/runners/direct_runner";
import * as testing from "../src/apache_beam/testing/assert";
import { KV } from "../src/apache_beam/values";

import { NodeRunner } from "../src/apache_beam/runners/node_runner/runner";
import { RemoteJobServiceClient } from "../src/apache_beam/runners/node_runner/client";
import { SumFn, MeanFn, MaxFn } from "../src/apache_beam/transforms/combine";
import {
  CombineFn,
  GroupBy,
  GroupGlobally,
  CountPerElement,
  CountGlobally,
} from "../src/apache_beam/transforms/group_and_combine";

describe("Apache Beam combiners", function () {
  it("runs wordcount with a countPerKey transform and asserts the result", async function () {
    //         await new NodeRunner(new RemoteJobServiceClient('localhost:3333')).run(
    await new DirectRunner().run((root) => {
      const lines = root.apply(
        new beam.Create([
          "In the beginning God created the heaven and the earth.",
          "And the earth was without form, and void; and darkness was upon the face of the deep.",
          "And the Spirit of God moved upon the face of the waters.",
          "And God said, Let there be light: and there was light.",
        ])
      );

      lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function* (line: string) {
          yield* line.split(/[^a-z]+/);
        })
        .map((elm) => ({ key: elm, value: 1 }))
        .apply(new GroupBy("key").combining("value", new SumFn(), "value"))
        .apply(
          new testing.AssertDeepEqual([
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
    await new DirectRunner().run((root) => {
      const lines = root.apply(
        new beam.Create([
          "And God said, Let there be light: and there was light",
        ])
      );

      lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function* (line: string) {
          yield* line.split(/[^a-z]+/);
        })
        .apply(new CountGlobally())
        .apply(new testing.AssertDeepEqual([11]));
    });
  });

  it("runs an example where a custom combine function is passed", async function () {
    type StdDevAcc = {
      count: number;
      sum: number;
      sumOfSquares: number;
    };

    class UnstableStdDevCombineFn
      implements CombineFn<number, StdDevAcc, number>
    {
      // NOTE: This Standard Deviation algorithm is **unstable**, so it is not recommended
      //    for an actual production-level pipeline.
      createAccumulator() {
        return { count: 0, sum: 0, sumOfSquares: 0 };
      }
      addInput(acc: StdDevAcc, inp: number) {
        return {
          count: acc.count + 1,
          sum: acc.sum + inp,
          sumOfSquares: acc.sumOfSquares + inp * inp,
        };
      }
      mergeAccumulators(accumulators: StdDevAcc[]) {
        return accumulators.reduce((previous, current) => ({
          count: previous.count + current.count,
          sum: previous.sum + current.sum,
          sumOfSquares: previous.sumOfSquares + current.sumOfSquares,
        }));
      }
      extractOutput(acc: StdDevAcc) {
        const mean = acc.sum / acc.count;
        return acc.sumOfSquares / acc.count - mean * mean;
      }
    }
    await new DirectRunner().run((root) => {
      const lines = root.apply(
        new beam.Create([
          "In the beginning God created the heaven and the earth.",
          "And the earth was without form, and void; and darkness was upon the face of the deep.",
          "And the Spirit of God moved upon the face of the waters.",
          "And God said, Let there be light: and there was light.",
        ])
      );

      lines
        .map((s: string) => s.toLowerCase())
        .flatMap(function* (line: string) {
          yield* line.split(/[^a-z]+/);
        })
        .map((word) => word.length)
        .apply(
          new GroupGlobally()
            .combining((c) => c, new MeanFn(), "mean")
            .combining((c) => c, new UnstableStdDevCombineFn(), "stdDev")
        )
        .apply(
          new testing.AssertDeepEqual([
            { mean: 3.611111111111111, stdDev: 3.2746913580246897 },
          ])
        );
    });
  });

  it("test GroupBy with combining", async function () {
    await new DirectRunner().run((root) => {
      const inputs = root.apply(
        new beam.Create([
          { k: "k1", a: 1, b: 100 },
          { k: "k1", a: 2, b: 200 },
          { k: "k2", a: 9, b: 1000 },
        ])
      );

      inputs
        .apply(
          new GroupBy("k")
            .combining("a", new MaxFn(), "aMax")
            .combining("a", new SumFn(), "aSum")
            .combining("b", new MeanFn(), "mean")
        )
        .apply(
          new testing.AssertDeepEqual([
            { k: "k1", aMax: 2, aSum: 3, mean: 150 },
            { k: "k2", aMax: 9, aSum: 9, mean: 1000 },
          ])
        );
    });
  });

  it("test GroupBy list with combining", async function () {
    await new DirectRunner().run((root) => {
      const inputs = root.apply(
        new beam.Create([
          { a: 1, b: 10, c: 100 },
          { a: 2, b: 10, c: 100 },
          { a: 1, b: 10, c: 400 },
        ])
      );

      inputs
        .apply(new GroupBy(["a", "b"]).combining("c", new SumFn(), "sum"))
        .apply(
          new testing.AssertDeepEqual([
            { a: 1, b: 10, sum: 500 },
            { a: 2, b: 10, sum: 100 },
          ])
        );

      inputs
        .apply(new GroupBy(["b", "c"]).combining("a", new SumFn(), "sum"))
        .apply(
          new testing.AssertDeepEqual([
            { b: 10, c: 100, sum: 3 },
            { b: 10, c: 400, sum: 1 },
          ])
        );
    });
  });

  it("test GroupBy expr with combining", async function () {
    await new DirectRunner().run((root) => {
      const inputs = root.apply(
        new beam.Create([
          { a: 1, b: 10 },
          { a: 0, b: 20 },
          { a: -1, b: 30 },
        ])
      );

      inputs
        .apply(
          new GroupBy((element: any) => element.a * element.a).combining(
            "b",
            new SumFn(),
            "sum"
          )
        )
        .apply(
          new testing.AssertDeepEqual([
            { key: 1, sum: 40 },
            { key: 0, sum: 20 },
          ])
        );
    });
  });

  it("test GroupBy with binary combinefn", async function () {
    await new DirectRunner().run((root) => {
      const inputs = root.apply(
        new beam.Create([
          { key: 0, value: 10 },
          { key: 1, value: 20 },
          { key: 0, value: 30 },
        ])
      );

      inputs
        .apply(
          new GroupBy("key")
            .combining("value", (x, y) => x + y, "sum")
            .combining("value", (x, y) => Math.max(x, y), "max")
        )
        .apply(
          new testing.AssertDeepEqual([
            { key: 0, sum: 40, max: 30 },
            { key: 1, sum: 20, max: 20 },
          ])
        );
    });
  });
});
