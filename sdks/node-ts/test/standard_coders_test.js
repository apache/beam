const coders = require("../dist/apache_beam/coders/standard_coders");

const assert = require('assert');
const yaml = require('js-yaml');
const fs   = require('fs');
const util = require('util');

const STANDARD_CODERS_FILE = '../../model/fn-execution/src/main/resources/org/apache/beam/model/fnexecution/v1/standard_coders.yaml';

const CODER_REGISTRY = coders.CODER_REGISTRY;


// TODO(pabloem): Empty this list.
const UNSUPPORTED_CODERS = [
    "beam:coder:bool:v1",
    "beam:coder:string_utf8:v1",
    "beam:coder:varint:v1",
    "beam:coder:kv:v1",
    "beam:coder:interval_window:v1",
    "beam:coder:iterable:v1",
    "beam:coder:timer:v1",
    "beam:coder:global_window:v1",
    "beam:coder:windowed_value:v1",
    "beam:coder:param_windowed_value:v1",
    "beam:coder:double:v1",
    "beam:coder:row:v1",
    "beam:coder:sharded_key:v1",
    "beam:coder:custom_window:v1"
];

describe("standard Beam coders on Javascript", function() {
    const docs = yaml.loadAll(fs.readFileSync(STANDARD_CODERS_FILE, 'utf8'));
    docs.forEach(doc => {
        const urn = doc.coder.urn;
        if (UNSUPPORTED_CODERS.includes(urn)) {
            return;
        }
        const nested = doc.nested;
        const spec = doc;

        const coder = CODER_REGISTRY.get(urn);
        describeCoder(coder, urn, nested, spec);
    });
});

function describeCoder(coder, urn, nested, spec) {
    describe(util.format("coder %s(%s) nested %s encodes properly", coder, urn, nested), function() {
        let examples = 0;
        for (let expected in spec.examples) {
            value = spec.examples[expected];
            examples += 1;
            coderCase(coder, value, expected, examples);
        }
    })
}

function coderCase(coder, obj, expectedEncoded, exampleCount) {
    it(util.format("example %d", exampleCount), function() {
        const encoded = coder.encode(obj);
        const decoded = coder.decode(expectedEncoded);
        assert.equal(expectedEncoded, encoded);
        assert.equal(obj, decoded);
    });
}