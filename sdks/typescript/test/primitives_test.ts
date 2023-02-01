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

import Long from "long";

import * as beam from "../src/apache_beam";
import * as assert from "assert";
import {
  BytesCoder,
  IterableCoder,
  KVCoder,
} from "../src/apache_beam/coders/standard_coders";
import * as combiners from "../src/apache_beam/transforms/combiners";
import { GeneralObjectCoder } from "../src/apache_beam/coders/js_coders";

import { directRunner } from "../src/apache_beam/runners/direct_runner";
import { loopbackRunner } from "../src/apache_beam/runners/runner";
import { Pipeline } from "../src/apache_beam/internal/pipeline";
import { GlobalWindow } from "../src/apache_beam/values";
import { PaneInfoCoder } from "../src/apache_beam/coders/standard_coders";
import * as testing from "../src/apache_beam/testing/assert";
import * as windowings from "../src/apache_beam/transforms/windowings";
import * as pardo from "../src/apache_beam/transforms/pardo";
import { withName } from "../src/apache_beam/transforms";
import * as service from "../src/apache_beam/utils/service";
import { MultiPipelineRunner } from "../src/apache_beam/testing/multi_pipeline_runner";

let subprocessCache;
before(async function () {
  this.timeout(30000);
  subprocessCache = service.SubprocessService.createCache();
  if (process.env.BEAM_SERVICE_OVERRIDES) {
    // Start it up here so we don't timeout any individual test.
    await loopbackRunner().run(function pipeline(root) {
      root.apply(beam.impulse());
    });
  }
});

after(() => subprocessCache.stopAll());

function sortValues(kvs) {
  return { key: kvs.key, value: [...kvs.value].sort() };
}

export function suite(runner: beam.Runner = directRunner()) {
  describe("testing.assertDeepEqual", function () {
    // The tests below won't catch failures if this doesn't fail.
    it("fails on bad assert", async function () {
      // TODO: There's probably a more idiomatic way to test failures.
      var seenError = false;
      try {
        await runner.run((root) => {
          const pcolls = root
            .apply(beam.create([1, 2, 3]))
            .apply(testing.assertDeepEqual([1, 2]));
        });
      } catch (Error) {
        seenError = true;
      }
      assert.equal(true, seenError);
    });
  });

  describe("runs basic transforms", function () {
    it("runs a map", async function () {
      await runner.run((root) => {
        const pcolls = root
          .apply(beam.create([1, 2, 3]))
          .map((x) => x * x)
          .apply(testing.assertDeepEqual([1, 4, 9]));
      });
    });

    it("runs a flatmap", async function () {
      await runner.run((root) => {
        const pcolls = root
          .apply(beam.create(["a b", "c"]))
          .flatMap((s) => s.split(/ +/))
          .apply(testing.assertDeepEqual(["a", "b", "c"]));
      });
    });

    it("runs a Splitter", async function () {
      await runner.run((root) => {
        const pcolls = root
          .apply(beam.create([{ a: 1 }, { b: 10 }, { a: 2, b: 20 }]))
          .apply(beam.split(["a", "b"], { exclusive: false }));
        pcolls.a.apply(testing.assertDeepEqual([1, 2]));
        pcolls.b.apply(testing.assertDeepEqual([10, 20]));
      });
    });

    it("runs a map with context", async function () {
      await runner.run((root) => {
        root
          .apply(beam.create([1, 2, 3]))
          .map((a: number, b: number) => a + b, 100)
          .apply(testing.assertDeepEqual([101, 102, 103]));
      });
    });

    it("runs a map with counters", async function () {
      const result = await runner.run((root) => {
        root
          .apply(beam.create([1, 2, 3]))
          .map(
            withName(
              "mapWithCounter",
              pardo.withContext(
                (x: number, context) => {
                  context.myCounter.increment(x);
                  context.myDist.update(x);
                  return x * x;
                },
                {
                  myCounter: pardo.counter("myCounter"),
                  myDist: pardo.distribution("myDist"),
                }
              )
            )
          )
          .apply(testing.assertDeepEqual([1, 4, 9]));
      });
      assert.deepEqual((await result.counters()).myCounter, 1 + 2 + 3);
      assert.deepEqual((await result.distributions()).myDist, {
        count: 3,
        sum: 6,
        min: 1,
        max: 3,
      });
    });

    it("runs a map with singleton side input", async function () {
      await runner.run((root) => {
        const input = root.apply(beam.create([1, 2, 1]));
        const sideInput = root.apply(
          beam.withName("createSide", beam.create([4]))
        );
        input
          .map((e, context) => e / context.side.lookup(), {
            side: pardo.singletonSideInput(sideInput),
          })
          .apply(testing.assertDeepEqual([0.25, 0.5, 0.25]));
      });
    });

    it("runs a map with a side input sharing input root", async function () {
      await runner.run((root) => {
        const input = root.apply(beam.create([1, 2, 1]));
        // TODO: Can this type be inferred?
        const sideInput: beam.PCollection<{ sum: number }> = input.apply(
          beam.groupGlobally().combining((e) => e, combiners.sum, "sum")
        );
        input
          .map((e, context) => e / context.side.lookup().sum, {
            side: pardo.singletonSideInput(sideInput),
          })
          .apply(testing.assertDeepEqual([0.25, 0.5, 0.25]));
      });
    });

    it("runs a map with window-sensitive context", async function () {
      await runner.run((root) => {
        root
          .apply(beam.create([1, 2, 3, 4, 5, 10, 11, 12]))
          .apply(beam.assignTimestamps((t) => Long.fromValue(t * 1000)))
          .apply(beam.windowInto(windowings.fixedWindows(10)))
          .apply(beam.groupBy((e: number) => ""))
          .map(
            withName(
              "MapWithContext",
              pardo.withContext(
                // This is the function to apply.
                (kv, context) => {
                  return {
                    key: kv.key,
                    value: [...kv.value].sort(),
                    window_start_ms: context.window.lookup().start.low,
                    a: context.other,
                  };
                },
                // This is the context to pass as the second argument.
                // At each element, window.get() will return the associated window.
                { window: pardo.windowParam(), other: "A" }
              )
            )
          )
          .apply(
            testing.assertDeepEqual([
              { key: "", value: [1, 2, 3, 4, 5], window_start_ms: 0, a: "A" },
              { key: "", value: [10, 11, 12], window_start_ms: 10000, a: "A" },
            ])
          );
      });
    });

    it("runs a WindowInto", async function () {
      await runner.run((root) => {
        root
          .apply(beam.create(["apple", "apricot", "banana"]))
          .apply(beam.windowInto(windowings.globalWindows()))
          .apply(beam.groupBy((e: string) => e[0]))
          .map(sortValues)
          .apply(
            testing.assertDeepEqual([
              { key: "a", value: ["apple", "apricot"] },
              { key: "b", value: ["banana"] },
            ])
          );
      });
    });

    it("runs a WindowInto IntervalWindow", async function () {
      await runner.run((root) => {
        root
          .apply(beam.create([1, 2, 3, 4, 5, 10, 11, 12]))
          .apply(beam.assignTimestamps((t) => Long.fromValue(t * 1000)))
          .apply(beam.windowInto(windowings.fixedWindows(10)))
          .apply(beam.groupBy((e: number) => ""))
          .map(sortValues)
          .apply(
            testing.assertDeepEqual([
              { key: "", value: [1, 2, 3, 4, 5] },
              { key: "", value: [10, 11, 12] },
            ])
          );
      });
    });
  });

  describe("applies basic transforms", function () {
    // TODO: test output with direct runner.
    it("runs a basic Impulse expansion", function () {
      var p = new Pipeline();
      var res = new beam.Root(p).apply(beam.impulse());

      assert.deepEqual(p.context.getPCollectionCoder(res), new BytesCoder());
    });
    it("runs a ParDo expansion", function () {
      var p = new Pipeline();
      var res = new beam.Root(p)
        .apply(beam.impulse())
        .map(function (v: any) {
          return v * 2;
        })
        .map(function (v: number) {
          return v * 4;
        });

      assert.deepEqual(
        p.context.getPCollectionCoder(res),
        new GeneralObjectCoder()
      );
    });
    // why doesn't map need types here?
    it("runs a GroupBy expansion", function () {
      var p = new Pipeline();
      var res = new beam.Root(p)
        .apply(beam.impulse())
        .map(function createElement(v) {
          return { name: "pablo", lastName: "wat" };
        })
        .apply(beam.groupBy("lastName"));

      assert.deepEqual(
        p.context.getPCollectionCoder(res),
        new KVCoder(
          new GeneralObjectCoder(),
          new IterableCoder(new GeneralObjectCoder())
        )
      );
    });
  });
}

describe("primitives module", function () {
  describe("direct runner", suite.bind(this));

  if (process.env.BEAM_SERVICE_OVERRIDES) {
    describe("portable runner @ulr", () => {
      suite.bind(this)(loopbackRunner());
    });
  } else {
    it("Portable tests not run because BEAM_SERVICE_OVERRIDES not set.", function () {
      this.skip();
    });
  }

  describe("multi-pipeline @dataflow", async () => {
    if (process.env.GCP_PROJECT_ID) {
      if (!process.env.BEAM_SERVICE_OVERRIDES) {
        throw new Error("Please specify BEAM_SERVICE_OVERRIDES env var.");
      }
      if (!process.env.GCP_TESTING_BUCKET) {
        throw new Error("Please specify GCP_REGION env var.");
      }
      if (!process.env.GCP_REGION) {
        throw new Error("Please specify GCP_TESTING_BUCKET env var.");
      }
      const runner = new MultiPipelineRunner(
        require("../src/apache_beam/runners/dataflow").dataflowRunner({
          project: process.env.GCP_PROJECT_ID,
          tempLocation: process.env.GCP_TESTING_BUCKET,
          region: process.env.GCP_REGION,
        })
      );

      beforeEach(function () {
        if (
          this.test!.title.includes("fails") ||
          this.test!.title.includes("counter")
        ) {
          this.skip();
        }
        runner.setNextTestName(this.test!.title.match(/([^"]+)"$/)![1]);
      });

      after(async function () {
        this.timeout(10 * 60 * 1000 /* 10 min */);
        console.log(await runner.reallyRunPipelines());
      });

      suite.bind(this)(runner);
    } else {
      it("Dataflow tests not run because GCP_PROJECT_ID not set.", function () {
        this.skip();
      });
    }
  });
});

describe("liquid sharding @ulr", function () {
  if (process.env.BEAM_SERVICE_OVERRIDES) {
    it("handles splitting", async function () {
      // This pipeline will process a number of "slow" elements, asking the
      // ULR to split the "toSplit" bundle at various points. We then verify
      // that the bundle was indeed split and all expected elements made it
      // through.
      const runner = loopbackRunner({
        directTestSplits: JSON.stringify({
          toSplit: {
            // Split 0.1, 0.15, etc. seconds into processing the bundle.
            timings: [0.1, 0.15, 0.2, 0.25],
            fractions: [0.5, 0.5, 0.5, 0.5],
          },
        }),
      });
      const numElements = 20;
      const elements = [...Array(numElements).keys()].map((x) => "" + x);
      await runner.run((root) => {
        const pcolls = root
          .apply(beam.create(elements))
          .map((x) => {
            // Synchronous sleep.
            Atomics.wait(
              new Int32Array(new SharedArrayBuffer(4)),
              0,
              0,
              // Processing all elements spread out over 1000 ms, plus overhead.
              1000 / numElements
            );
            return x;
          })
          .apply(
            withName(
              "toSplit",
              beam.parDo({
                process: (element) => [element],
                finishBundle: () => [
                  {
                    value: "endOfBundle",
                    windows: [new GlobalWindow()],
                    pane: PaneInfoCoder.ONE_AND_ONLY_FIRING,
                    timestamp: new GlobalWindow().maxTimestamp(),
                  },
                ],
              })
            )
          )
          .apply(
            // More than one "endOfBundle" output because we split.
            // (We in fact should have split this first bundle multiple times,
            // but all the remainders get processed in a (single) second bundle.
            // Importantly, however, we verify that regardless of where the
            // splits occurred (if you print them, they're in an interesting
            // order), all elements are accounted for.)
            testing.assertDeepEqual(
              elements.concat(["endOfBundle", "endOfBundle"])
            )
          );
      });
      // The pipeline should run fast, but give plenty of time for the service
      // to start up in case the test machine is loaded.
    }).timeout(30000);
  }
});
