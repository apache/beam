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

import { PaneInfoCoder } from "../src/apache_beam/coders/standard_coders";

import {
  PTransform,
  PCollection,
} from "../src/apache_beam/proto/beam_runner_api";
import { ProcessBundleDescriptor } from "../src/apache_beam/proto/beam_fn_api";

import * as worker from "../src/apache_beam/worker/worker";
import * as operators from "../src/apache_beam/worker/operators";
import {
  Window,
  GlobalWindow,
  Instant,
  PaneInfo,
  WindowedValue,
} from "../src/apache_beam/values";

const assert = require("assert");

////////// Define some mock operators for use in our tests. //////////

const CREATE_URN = "create";
const RECORDING_URN = "recording";
const PARTITION_URN = "partition";

class Create implements operators.IOperator {
  data: any;
  receivers: operators.Receiver[];

  constructor(
    public transformId: string,
    transform: PTransform,
    context: operators.OperatorContext
  ) {
    this.data = JSON.parse(new TextDecoder().decode(transform.spec!.payload!));
    this.receivers = Object.values(transform.outputs).map(context.getReceiver);
  }

  process(wvalue: WindowedValue<any>) {
    return operators.NonPromise;
  }

  async startBundle() {
    const this_ = this;
    this_.data.forEach(function (datum) {
      this_.receivers.map((receiver) =>
        receiver.receive({
          value: datum,
          windows: [new GlobalWindow()],
          pane: PaneInfoCoder.ONE_AND_ONLY_FIRING,
          timestamp: Long.fromValue("-9223372036854775"),
        })
      );
    });
  }

  async finishBundle() {}
}

operators.registerOperator(CREATE_URN, Create);

class Recording implements operators.IOperator {
  static log: string[];
  transformId: string;
  receivers: operators.Receiver[];

  static reset() {
    Recording.log = [];
  }

  constructor(
    transformId: string,
    transform: PTransform,
    context: operators.OperatorContext
  ) {
    this.transformId = transformId;
    this.receivers = Object.values(transform.outputs).map(context.getReceiver);
  }

  async startBundle() {
    Recording.log.push(this.transformId + ".startBundle()");
  }

  process(wvalue: WindowedValue<any>) {
    Recording.log.push(this.transformId + ".process(" + wvalue.value + ")");
    const results = this.receivers.map((receiver) => receiver.receive(wvalue));
    if (!results.every((r) => r === operators.NonPromise)) {
      throw new Error("Unexpected non-promise: " + results);
    }
    return operators.NonPromise;
  }

  async finishBundle() {
    Recording.log.push(this.transformId + ".finishBundle()");
  }
}

operators.registerOperator(RECORDING_URN, Recording);
worker.primitiveSinks.push(RECORDING_URN);

class Partition implements operators.IOperator {
  big: operators.Receiver;
  all: operators.Receiver;

  constructor(
    public transformId: string,
    transform: PTransform,
    context: operators.OperatorContext
  ) {
    this.big = context.getReceiver(transform.outputs.big);
    this.all = context.getReceiver(transform.outputs.all);
  }

  process(wvalue: WindowedValue<any>) {
    const a = this.all.receive(wvalue);
    if (a !== operators.NonPromise) throw new Error("Unexpected promise: " + a);
    if (wvalue.value.length > 3) {
      const b = this.big.receive(wvalue);
      if (b !== operators.NonPromise)
        throw new Error("Unexpected promise: " + b);
    }
    return operators.NonPromise;
  }

  async startBundle() {}
  async finishBundle() {}
}

operators.registerOperator(PARTITION_URN, Partition);

////////// Helper Functions. //////////

function makePTransform(
  urn: string,
  inputs: { [key: string]: string },
  outputs: { [key: string]: string },
  payload: any = null
): PTransform {
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

describe("worker module", function () {
  it("operator construction", async function () {
    const descriptor: ProcessBundleDescriptor = {
      id: "",
      transforms: {
        // Note the inverted order should still be resolved correctly.
        y: makePTransform(RECORDING_URN, { input: "pc1" }, { out: "pc2" }),
        z: makePTransform(RECORDING_URN, { input: "pc2" }, {}),
        x: makePTransform(CREATE_URN, {}, { out: "pc1" }, ["a", "b", "c"]),
      },
      pcollections: {},
      windowingStrategies: {},
      coders: {},
      environments: {},
    };
    Recording.reset();
    const processor = new worker.BundleProcessor(descriptor, null!, null!, [
      CREATE_URN,
    ]);
    await processor.process("bundle_id");
    assert.deepEqual(Recording.log, [
      "z.startBundle()",
      "y.startBundle()",
      "y.process(a)",
      "z.process(a)",
      "y.process(b)",
      "z.process(b)",
      "y.process(c)",
      "z.process(c)",
      "y.finishBundle()",
      "z.finishBundle()",
    ]);
  });

  it("forking operator construction", async function () {
    const descriptor: ProcessBundleDescriptor = {
      id: "",
      transforms: {
        // Note the inverted order should still be resolved correctly.
        all: makePTransform(RECORDING_URN, { input: "pcAll" }, {}),
        big: makePTransform(RECORDING_URN, { input: "pcBig" }, {}),
        part: makePTransform(
          PARTITION_URN,
          { input: "pc1" },
          { all: "pcAll", big: "pcBig" }
        ),
        create: makePTransform(CREATE_URN, {}, { out: "pc1" }, [
          "a",
          "b",
          "ccccc",
        ]),
      },
      pcollections: {},
      windowingStrategies: {},
      coders: {},
      environments: {},
    };
    Recording.reset();
    const processor = new worker.BundleProcessor(descriptor, null!, null!, [
      CREATE_URN,
    ]);
    await processor.process("bundle_id");
    assert.deepEqual(Recording.log, [
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
