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

// Run directly with
//
//    node dist/src/apache_beam/examples/wordcount.js
//
// A different runner can be chosen via a --runner argument, e.g.
//
//    node dist/src/apache_beam/examples/wordcount.js --runner=flink
//
// To run on Dataflow, pass the required arguments:
//
//    node dist/src/apache_beam/examples/wordcount.js --runner=dataflow --project=PROJECT_ID --tempLocation=gs://BUCKET/DIR' --region=us-central1

// TODO: Should this be in a top-level examples dir, rather than under apache_beam?

import * as yargs from "yargs";

import * as beam from "../../apache_beam";
import { createRunner } from "../runners/runner";
import { countPerElement } from "../transforms/group_and_combine";

function wordCount(lines: beam.PCollection<string>): beam.PCollection<any> {
  return lines
    .map((s: string) => s.toLowerCase())
    .flatMap(function* (line: string) {
      yield* line.split(/[^a-z]+/);
    })
    .apply(countPerElement());
}

async function main() {
  await createRunner(yargs.argv).run((root) => {
    const lines = root.apply(
      beam.create([
        "In the beginning God created the heaven and the earth.",
        "And the earth was without form, and void; and darkness was upon the face of the deep.",
        "And the Spirit of God moved upon the face of the waters.",
        "And God said, Let there be light: and there was light.",
      ]),
    );

    lines.apply(wordCount).map(console.log);
  });
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
