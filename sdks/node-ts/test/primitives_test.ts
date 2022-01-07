import * as beam from '../src/apache_beam';
import * as assert from 'assert';
import { BytesCoder, IterableCoder, KVCoder } from '../src/apache_beam/coders/standard_coders';
import {GroupBy} from '../src/apache_beam/transforms/core'
import { GeneralObjectCoder } from '../src/apache_beam/coders/js_coders';

describe("primitives module", function() {
    describe("runs a basic impulse expansion", function() {
        it("runs a basic Impulse expansion", function() {
            var p = new beam.Pipeline();
            var res = p.apply(new beam.Impulse());

            assert.equal(res.type, "pcollection");
            assert.deepEqual(p.getCoder(res.proto.coderId), new BytesCoder());
        });
        it("runs a ParDo expansion", function() {
            var p = new beam.Pipeline();
            var res = p.apply(new beam.Impulse())
            .map(function(v) {return v*2;})
            .map(function(v) {return v*4;});

            const coder = p.getCoder(res.proto.coderId);
            assert.deepEqual(coder, new GeneralObjectCoder());
            assert.equal(res.type, "pcollection");
        });
        it("runs a GroupBy expansion", function() {
            var p = new beam.Pipeline();
            var res = p.apply(new beam.Impulse())
            .map(function(v) {return {"name": "pablo", "lastName": "wat"};})
            .apply(new GroupBy("lastName"));

            const coder = p.getCoder(res.proto.coderId);
            assert.deepEqual(coder, new KVCoder(new GeneralObjectCoder(), new IterableCoder(new GeneralObjectCoder())));
        });
    });
});
