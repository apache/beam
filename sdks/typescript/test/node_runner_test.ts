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

import fs from "fs";
import assert from "assert";

import * as runnerApiProto from "../src/apache_beam/proto/beam_runner_api";
import { PortableRunner } from "../src/apache_beam/runners/portable_runner/runner";
import { JobState_Enum } from "../src/apache_beam/proto/beam_job_api";

const JOB_SERVICE_HOST = process.env["JOB_SERVICE_HOST"];
const JSON_PROTO_PATH = __dirname + "/../../test/testdata/pipeline.json";

describe("node runner", () => {
  it("runs", async function () {
    if (!JOB_SERVICE_HOST) {
      this.skip();
    }

    const pipelineJson = fs.readFileSync(JSON_PROTO_PATH, "utf-8");

    const runner = new PortableRunner(JOB_SERVICE_HOST);
    const pipelineResult = await runner.runPipelineWithProto(
      runnerApiProto.Pipeline.fromJsonString(pipelineJson)
    );

    const event = await pipelineResult.waitUntilFinish(60000);
    assert(event == JobState_Enum.DONE);
  });
});
