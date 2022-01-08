import * as beam from '../src/apache_beam';
import * as assert from 'assert';
import { BytesCoder, IterableCoder, KVCoder } from '../src/apache_beam/coders/standard_coders';
import { GroupBy } from '../src/apache_beam/transforms/core'
import { GeneralObjectCoder } from '../src/apache_beam/coders/js_coders';

import { DirectRunner } from '../src/apache_beam/runners/direct_runner'
import * as testing from '../src/apache_beam/testing/assert';


describe("primitives module", function() {
    describe("runs a basic impulse expansion", function() {
        // TODO: test output with direct runner.
        it("runs a basic Impulse expansion", function() {
            var p = new beam.Pipeline();
            var res = p.apply(new beam.Impulse());

            assert.equal(res.type, "pcollection");
            assert.deepEqual(p.getCoder(res.proto.coderId), new BytesCoder());
        });
        it("runs a ParDo expansion", function() {
            var p = new beam.Pipeline();
            var res = p.apply(new beam.Impulse())
                .map<number,number>(function(v: number) { return v * 2; })
                .map<number,number>(function(v: number) { return v * 4; });

            const coder = p.getCoder(res.proto.coderId);
            assert.deepEqual(coder, new GeneralObjectCoder());
            assert.equal(res.type, "pcollection");
        });
        // why doesn't map need types here?
        it("runs a GroupBy expansion", function() {
            var p = new beam.Pipeline();
            var res = p.apply(new beam.Impulse())
                .map(function(v) { return { "name": "pablo", "lastName": "wat" }; })
                .apply(new GroupBy("lastName"));

            const coder = p.getCoder(res.proto.coderId);
            assert.deepEqual(coder, new KVCoder(new GeneralObjectCoder(), new IterableCoder(new GeneralObjectCoder())));
        });
        it("runs a Splitter", async function() {
        await new DirectRunner().run(
            (root) => {
                const pcolls = root
                    .apply(new beam.Create(['apple', 'apricot', 'banana']))
                    .apply(new beam.Split(e => e[0], 'a', 'b'));
                pcolls.a.apply(new testing.AssertDeepEqual(['apple', 'apricot']));
                pcolls.b.apply(new testing.AssertDeepEqual(['banana']));
            })
        });
    });
});
