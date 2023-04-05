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

import * as beam from "../src/apache_beam";
import { directRunner } from "../src/apache_beam/runners/direct_runner";
import * as testing from "../src/apache_beam/testing/assert";

import { PortableRunner } from "../src/apache_beam/runners/portable_runner/runner";

function wordCount(
  lines: beam.PCollection<string>
): beam.PCollection<beam.KV<string, number>> {
  return lines
    .map((s: string) => s.toLowerCase())
    .flatMap(function* splitWords(line: string) {
      yield* line.split(/[^a-z]+/);
    })
    .apply(beam.countPerElement());
}

describe("wordcount", function () {
  it("wordcount", async function () {
    await directRunner().run((root) => {
      const lines = root.apply(
        beam.create([
          "In the beginning God created the heaven and the earth.",
          "And the earth was without form, and void; and darkness was upon the face of the deep.",
          "And the Spirit of God moved upon the face of the waters.",
          "And God said, Let there be light: and there was light.",
        ])
      );

      lines.apply(wordCount).map(console.log);
    });
  });

  it("wordcount assert", async function () {
    await directRunner().run((root) => {
      const lines = root.apply(
        beam.create(["And God said, Let there be light: and there was light"])
      );

      lines.apply(wordCount).apply(
        testing.assertDeepEqual([
          { element: "and", count: 2 },
          { element: "god", count: 1 },
          { element: "said", count: 1 },
          { element: "let", count: 1 },
          { element: "there", count: 2 },
          { element: "be", count: 1 },
          { element: "light", count: 2 },
          { element: "was", count: 1 },
        ])
      );
    });
  });
});
