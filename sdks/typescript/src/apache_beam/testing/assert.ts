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

import * as beam from "../../apache_beam";
import { GlobalWindows } from "../../apache_beam/transforms/windowings";

import * as assert from "assert";

// TODO: (Naming)
export class AssertDeepEqual extends beam.PTransform<
  beam.PCollection<any>,
  void
> {
  expected: any[];

  constructor(expected: any[]) {
    super("AssertDeepEqual");
    this.expected = expected;
  }

  expand(pcoll: beam.PCollection<any>) {
    const expected = this.expected;
    pcoll.apply(
      new Assert("Assert", (actual) => {
        // Is there a less explicit way to do this?
        const actualArray: any[] = [];
        for (const a of actual) {
          actualArray.push(a);
        }
        expected.sort();
        actualArray.sort();
        assert.deepEqual(actualArray, expected);
      })
    );
  }
}

export class Assert extends beam.PTransform<beam.PCollection<any>, void> {
  check: (actual: any[]) => void;

  constructor(name: string, check: (actual: any[]) => void) {
    super(name);
    this.check = check;
  }

  expand(pcoll: beam.PCollection<any>) {
    const check = this.check;
    // We provide some value here to ensure there is at least one element
    // so the DoFn gets invoked.
    const singleton = pcoll
      .root()
      .apply(new beam.Impulse())
      .map((_) => ({ tag: "expected" }));
    // CoGBK.
    const tagged = pcoll
      .map((e) => ({ tag: "actual", value: e }))
      .apply(new beam.WindowInto(new GlobalWindows()));
    beam
      .P([singleton, tagged])
      .apply(new beam.Flatten())
      .map((e) => ({ key: 0, value: e }))
      .apply(new beam.GroupByKey())
      .map(
        beam.withName("extractActual", (kv) => {
          // Javascript list comprehension?
          const actual: any[] = [];
          for (const o of kv.value) {
            if (o.tag == "actual") {
              actual.push(o.value);
            }
          }
          check(actual);
        })
      );
  }
}
