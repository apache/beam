import { PTransform, PCollection } from "../src/apache_beam/proto/beam_runner_api";
import { ProcessBundleDescriptor } from "../src/apache_beam/proto/beam_fn_api";

import * as worker from '../src/apache_beam/worker/worker';
import * as operators from '../src/apache_beam/worker/operators';
import { WindowedValue } from '../src/apache_beam/worker/operators';

const assert = require('assert');


////////// Define some mock operators for use in our tests. //////////

const CREATE_URN = "create";
const RECORDING_URN = "recording";
const PARTITION_URN = "partition";


class Create implements operators.IOperator {
    data: any;
    receivers: operators.Receiver[];

    constructor(transformId: string, transform: PTransform, context: operators.OperatorContext) {
        this.data = JSON.parse(new TextDecoder().decode(transform.spec!.payload!));
        this.receivers = Object.values(transform.outputs).map(context.getReceiver);
    }

    process(wvalue: WindowedValue) { }

    startBundle() {
        const this_ = this;
        this_.data.forEach(function(datum) {
            this_.receivers.map((receiver) => receiver.receive({ value: datum }));
        })
    }

    finishBundle() { }
}

operators.registerOperator(CREATE_URN, Create);


class Recording implements operators.IOperator {
    static log: string[]
    transformId: string;
    receivers: operators.Receiver[];

    static reset() {
        Recording.log = [];
    }

    constructor(transformId: string, transform: PTransform, context: operators.OperatorContext) {
        this.transformId = transformId;
        this.receivers = Object.values(transform.outputs).map(context.getReceiver);
    }

    startBundle() {
        Recording.log.push(this.transformId + ".startBundle()");
    }

    process(wvalue: WindowedValue) {
        Recording.log.push(this.transformId + ".process(" + wvalue.value + ")")
        this.receivers.map((receiver) => receiver.receive(wvalue));
    }

    finishBundle() {
        Recording.log.push(this.transformId + ".finishBundle()");
    }
}

operators.registerOperator(RECORDING_URN, Recording);


class Partition implements operators.IOperator {
    big: operators.Receiver;
    all: operators.Receiver;

    constructor(transformId: string, transform: PTransform, context: operators.OperatorContext) {
        this.big = context.getReceiver(transform.outputs.big);
        this.all = context.getReceiver(transform.outputs.all);
    }

    process(wvalue: WindowedValue) {
        this.all.receive(wvalue);
        if (wvalue.value.length > 3) {
            this.big.receive(wvalue);
        }
    }

    startBundle() { }
    finishBundle() { }
}

operators.registerOperator(PARTITION_URN, Partition);


////////// Helper Functions. //////////

function makePTransform(urn: string, inputs: { [key: string]: string; }, outputs: { [key: string]: string; }, payload: any = null): PTransform {
    return {
        spec: {
            urn: urn,
            payload: new TextEncoder().encode(JSON.stringify(payload)),
        },
        inputs: inputs,
        outputs: outputs,
        uniqueName: "",
        environmentId: "",
        displayData: [],
        annotations: {},
        subtransforms: [],
    };
}


////////// The actual tests. //////////

describe("worker module", function() {
    it("operator construction", async function() {
        const descriptor: ProcessBundleDescriptor = {
            id: "",
            transforms: {
                // Note the inverted order should still be resolved correctly.
                "y": makePTransform(RECORDING_URN, { input: "pc1" }, { out: "pc2" }),
                "z": makePTransform(RECORDING_URN, { input: "pc2" }, {}),
                "x": makePTransform(CREATE_URN, {}, { out: "pc1" }, ["a", "b", "c"]),
            },
            pcollections: {},
            windowingStrategies: {},
            coders: {},
            environments: {},
        };
        Recording.reset();
        const processor = new worker.BundleProcessor(descriptor, null!);
        await processor.process("bundle_id", 0);
        assert.deepEqual(
            Recording.log,
            [
                'z.startBundle()',
                'y.startBundle()',
                'y.process(a)',
                'z.process(a)',
                'y.process(b)',
                'z.process(b)',
                'y.process(c)',
                'z.process(c)',
                'y.finishBundle()',
                'z.finishBundle()'
            ]);
    });

    it("forking operator construction", async function() {
        const descriptor: ProcessBundleDescriptor = {
            id: "",
            transforms: {
                // Note the inverted order should still be resolved correctly.
                "all": makePTransform(RECORDING_URN, { input: "pcAll" }, {}),
                "big": makePTransform(RECORDING_URN, { input: "pcBig" }, {}),
                "part": makePTransform(PARTITION_URN, { input: "pc1" }, { all: "pcAll", big: "pcBig" }),
                "create": makePTransform(CREATE_URN, {}, { out: "pc1" }, ["a", "b", "ccccc"]),
            },
            pcollections: {},
            windowingStrategies: {},
            coders: {},
            environments: {},
        };
        Recording.reset();
        const processor = new worker.BundleProcessor(descriptor, null!);
        await processor.process("bundle_id", 0);
        assert.deepEqual(
            Recording.log,
            [
                "all.startBundle()",
                "big.startBundle()",
                "all.process(a)",
                "all.process(b)",
                "all.process(ccccc)",
                "big.process(ccccc)",
                "big.finishBundle()",
                "all.finishBundle()",
            ]);
    });
});
