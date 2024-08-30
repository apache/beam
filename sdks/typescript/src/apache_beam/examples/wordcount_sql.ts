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

// TODO: Should this be in a top-level examples dir, rather than under apache_beam.

import * as beam from "../../apache_beam";
import * as external from "../../apache_beam/transforms/external";
import * as textio from "../io/textio";

import { PortableRunner } from "../runners/portable_runner/runner";

import { sqlTransform } from "../transforms/sql";

async function main() {
  // python apache_beam/runners/portability/local_job_service_main.py --port 3333
  await new PortableRunner({
    environmentType: "LOOPBACK",
    jobEndpoint: "localhost:3333",
  }).run(async (root) => {
    const lines = root.apply(beam.create(["a", "b", "c", "c"]));

    const filtered = await lines
      .map((w) => ({ word: w }))
      .apply(beam.withRowCoder({ word: "str" }))
      .applyAsync(
        sqlTransform(
          "SELECT word, count(*) as c from PCOLLECTION group by word",
        ),
      );

    filtered.map(console.log);
  });
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
