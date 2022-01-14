import Long from "long";

import * as beam from "../src/apache_beam";
import * as assert from "assert";
import {
  BytesCoder,
  IterableCoder,
  KVCoder,
} from "../src/apache_beam/coders/standard_coders";
import { GroupBy } from "../src/apache_beam/transforms/group_and_combine";
import { GeneralObjectCoder } from "../src/apache_beam/coders/js_coders";

import { DirectRunner } from "../src/apache_beam/runners/direct_runner";
import * as testing from "../src/apache_beam/testing/assert";
import * as windowings from "../src/apache_beam/transforms/windowings";
import * as pardo from "../src/apache_beam/transforms/pardo";

describe("primitives module", function () {
  describe("applies basic transforms", function () {
    // TODO: test output with direct runner.
    it("runs a basic Impulse expansion", function () {
      var p = new beam.Pipeline();
      var res = new beam.Root(p).apply(new beam.Impulse());

      assert.equal(res.type, "pcollection");
      assert.deepEqual(p.context.getPCollectionCoder(res), new BytesCoder());
    });
    it("runs a ParDo expansion", function () {
      var p = new beam.Pipeline();
      var res = new beam.Root(p)
        .apply(new beam.Impulse())
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
      assert.equal(res.type, "pcollection");
    });
    // why doesn't map need types here?
    it("runs a GroupBy expansion", function () {
      var p = new beam.Pipeline();
      var res = new beam.Root(p)
        .apply(new beam.Impulse())
        .map(function (v) {
          return { name: "pablo", lastName: "wat" };
        })
        .apply(new GroupBy("lastName"));

      assert.deepEqual(
        p.context.getPCollectionCoder(res),
        new KVCoder(
          new GeneralObjectCoder(),
          new IterableCoder(new GeneralObjectCoder())
        )
      );
    });
  });
  describe("runs a basic transforms", function () {
    it("runs a Splitter", async function () {
      await new DirectRunner().run((root) => {
        const pcolls = root
          .apply(new beam.Create(["apple", "apricot", "banana"]))
          .apply(new beam.Split((e) => e[0], "a", "b"));
        pcolls.a.apply(new testing.AssertDeepEqual(["apple", "apricot"]));
        pcolls.b.apply(new testing.AssertDeepEqual(["banana"]));
      });
    });
    it("runs a map with context", async function () {
      await new DirectRunner().run((root) => {
        root
          .apply(new beam.Create([1, 2, 3]))
          .map((a: number, b: number) => a + b, 100)
          .apply(new testing.AssertDeepEqual([101, 102, 103]));
      });
    });
    it("runs a map with window-sensitive context", async function () {
      await new DirectRunner().run((root) => {
        root
          .apply(new beam.Create([1, 2, 3, 4, 5, 10, 11, 12]))
          .apply(new beam.AssignTimestamps((t) => Long.fromValue(t * 1000)))
          .apply(new beam.WindowInto(new windowings.FixedWindows(10)))
          .apply(new beam.GroupBy((e: number) => ""))
          .map(
            // This is the function to apply.
            (kv, context) => {
              return {
                key: kv.key,
                value: kv.value,
                window_start_ms: context.window.lookup().start.low,
                a: context.other,
              };
            },
            // This is the context to pass as the second argument.
            // At each element, window.get() will return the associated window.
            { window: new pardo.WindowParam(), other: "A" }
          )
          .apply(
            new testing.AssertDeepEqual([
              { key: "", value: [1, 2, 3, 4, 5], window_start_ms: 0, a: "A" },
              { key: "", value: [10, 11, 12], window_start_ms: 10000, a: "A" },
            ])
          );
      });
    });
    it("runs a WindowInto", async function () {
      await new DirectRunner().run((root) => {
        root
          .apply(new beam.Create(["apple", "apricot", "banana"]))
          .apply(new beam.WindowInto(new windowings.GlobalWindows()))
          .apply(new beam.GroupBy((e: string) => e[0]))
          .apply(
            new testing.AssertDeepEqual([
              { key: "a", value: ["apple", "apricot"] },
              { key: "b", value: ["banana"] },
            ])
          );
      });
    });
    it("runs a WindowInto IntervalWindow", async function () {
      await new DirectRunner().run((root) => {
        root
          .apply(new beam.Create([1, 2, 3, 4, 5, 10, 11, 12]))
          .apply(new beam.AssignTimestamps((t) => Long.fromValue(t * 1000)))
          .apply(new beam.WindowInto(new windowings.FixedWindows(10)))
          .apply(new beam.GroupBy((e: number) => ""))
          .apply(
            new testing.AssertDeepEqual([
              { key: "", value: [1, 2, 3, 4, 5] },
              { key: "", value: [10, 11, 12] },
            ])
          );
      });
    });
  });
});
