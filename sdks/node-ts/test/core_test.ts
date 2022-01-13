import * as beam from "../src/apache_beam";
import * as assert from "assert";
import { BytesCoder } from "../src/apache_beam/coders/standard_coders";
// TODO(pabloem): Fix installation.

describe("core module", function () {
  describe("runs a basic impulse expansion", function () {
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
        });
    });
    it("runs a GroupBy expansion", function () {});
  });
});
