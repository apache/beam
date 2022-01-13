import Long from "long";

import * as beam from "../src/apache_beam";
import * as assert from "assert";
import {
  BytesCoder,
  IterableCoder,
  KVCoder,
} from "../src/apache_beam/coders/standard_coders";
import { GroupBy } from "../src/apache_beam/transforms/core";
import { GeneralObjectCoder } from "../src/apache_beam/coders/js_coders";

import { DirectRunner } from "../src/apache_beam/runners/direct_runner";
import * as testing from "../src/apache_beam/testing/assert";
import * as windowing from "../src/apache_beam/transforms/windowing";

describe("primitives module", function () {
  describe("applies basic transforms", function () {
    // TODO: test output with direct runner.
    it("runs a basic Impulse expansion", function () {
      var p = new beam.Pipeline();
      var res = p.apply(new beam.Impulse());

      assert.equal(res.type, "pcollection");
      assert.deepEqual(p.context.getPCollectionCoder(res), new BytesCoder());
    });
    it("runs a ParDo expansion", function () {
      var p = new beam.Pipeline();
      var res = p
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
      var res = p
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
    it("runs a WindowInto", async function () {
      await new DirectRunner().run((root) => {
        root
          .apply(new beam.Create(["apple", "apricot", "banana"]))
          .apply(new beam.WindowInto(new windowing.GlobalWindows()))
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
          .apply(new beam.WindowInto(new windowing.FixedWindows(10)))
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
