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

/**
 * Various transforms useful for asserting the expected contents of
 * PCollections, primarily for for testing.
 *
 * @packageDocumentation
 */

import * as beam from "../index";
import { globalWindows } from "../transforms/windowings";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";
import * as assert from "assert";

// TODO(serialization): See if we can avoid this.
function callAssertDeepEqual(a, b) {
  return assert.deepEqual(a, b);
}

// TODO: (Naming)
/**
 * A PTransform that will fail the pipeline if the input PCollection does not
 * contain exactly the given elements (in any order).  Useful for writing test,
 * e.g.
 *
 *```js
 * pcoll.apply(assertDeepEqual(1, 2, 3));
 *```
 */
export function assertDeepEqual<T>(
  expected: T[],
): beam.PTransform<beam.PCollection<T>, void> {
  return beam.withName(
    `assertDeepEqual(${JSON.stringify(expected).substring(0, 100)})`,
    function assertDeepEqual(pcoll: beam.PCollection<T>) {
      pcoll.apply(
        assertContentsSatisfies((actual: T[]) => {
          const actualArray: T[] = [...actual];
          expected.sort((a, b) =>
            JSON.stringify(a) < JSON.stringify(b) ? -1 : 1,
          );
          actualArray.sort((a, b) =>
            JSON.stringify(a) < JSON.stringify(b) ? -1 : 1,
          );
          callAssertDeepEqual(actualArray, expected);
        }),
      );
    },
  );
}

/**
 * A PTransform that will fail the pipeline if the given callback fails when
 * called with the input PCollection's elements.
 *
 * Note that the callback must not be sensitive to ordering, as the ordering
 * of the provided elements is not well determined.
 */
export function assertContentsSatisfies<T>(
  check: (actual: T[]) => void,
): beam.PTransform<beam.PCollection<T>, void> {
  function expand(pcoll: beam.PCollection<T>) {
    // We provide some value here to ensure there is at least one element
    // so the DoFn gets invoked.
    const singleton = pcoll
      .root()
      .apply(beam.impulse())
      .map((_) => ({ tag: "expected" }));
    // CoGBK.
    const tagged = pcoll
      .map((e) => ({ tag: "actual", value: e }))
      .apply(beam.windowInto(globalWindows()));
    beam
      .P([singleton, tagged])
      .apply(beam.flatten())
      .apply(beam.groupBy((e) => 0))
      .map(
        beam.withName("extractActual", (kv) => {
          const actual: any[] =
            kv.value?.filter((o) => o.tag === "actual").map((o) => o.value) ||
            [];
          check(actual);
        }),
      );
  }

  return beam.withName(
    `assertContentsSatisfies(${beam.extractName(check)})`,
    expand,
  );
}

requireForSerialization(`${packageName}/testing/assert`, exports);
requireForSerialization(`${packageName}/testing/assert`, {
  callAssertDeepEqual,
});
requireForSerialization("assert");
