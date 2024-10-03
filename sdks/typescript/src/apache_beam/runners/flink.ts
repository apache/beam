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

const fs = require("fs");
const os = require("os");
const path = require("path");

import { Pipeline } from "../proto/beam_runner_api";
import { PipelineResult, Runner } from "./runner";
import { PortableRunner } from "./portable_runner/runner";
import { JavaJarService } from "../utils/service";

const MAGIC_HOST_NAMES = ["[local]", "[auto]"];

// These should stay in sync with gradle.properties.
const PUBLISHED_FLINK_VERSIONS = ["1.15", "1.16", "1.17", "1.18", "1.19"];

const defaultOptions = {
  flinkMaster: "[local]",
  flinkVersion: PUBLISHED_FLINK_VERSIONS[PUBLISHED_FLINK_VERSIONS.length - 1],
};

export function flinkRunner(runnerOptions: Object = {}): Runner {
  return new (class extends Runner {
    async runPipeline(
      pipeline: Pipeline,
      options: Object = {},
    ): Promise<PipelineResult> {
      const allOptions = {
        ...defaultOptions,
        ...runnerOptions,
        ...options,
      } as any;
      if (
        !allOptions.environmentType &&
        MAGIC_HOST_NAMES.includes(allOptions.flinkMaster)
      ) {
        allOptions.environmentType = "LOOPBACK";
      }
      if (!allOptions.artifactsDir) {
        allOptions.artifactsDir = fs.mkdtempSync(
          path.join(os.tmpdir(), "flinkArtifactsDir"),
        );
      }

      const jobServerJar =
        allOptions.flinkJobServerJar ||
        (await JavaJarService.cachedJar(
          await JavaJarService.gradleToJar(
            `runners:flink:${allOptions.flinkVersion}:job-server:shadowJar`,
          ),
        ));
      const jobServer = new JavaJarService(jobServerJar, [
        "--flink-master",
        allOptions.flinkMaster,
        "--artifacts-dir",
        allOptions.artifactsDir,
        "--job-port",
        "{{PORT}}",
        "--artifact-port",
        "0",
        "--expansion-port",
        "0",
      ]);

      return new PortableRunner(allOptions, jobServer).runPipeline(pipeline);
    }
  })();
}
