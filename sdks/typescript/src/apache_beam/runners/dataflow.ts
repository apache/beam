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

import { Pipeline } from "../proto/beam_runner_api";
import { PipelineResult, Runner } from "./runner";
import { PortableRunner } from "./portable_runner/runner";
import { PythonService } from "../utils/service";

export function dataflowRunner(runnerOptions: {
  project: string;
  tempLocation: string;
  region: string;
  [others: string]: any;
}): Runner {
  return new (class extends Runner {
    async runPipeline(
      pipeline: Pipeline,
      options: Object = {},
    ): Promise<PipelineResult> {
      var augmentedOptions = { experiments: [] as string[], ...options };
      augmentedOptions.experiments.push("use_runner_v2");
      augmentedOptions.experiments.push("use_portable_job_submission");
      augmentedOptions.experiments.push("use_sibling_sdk_workers");
      const service = PythonService.forModule(
        "apache_beam.runners.dataflow.dataflow_job_service",
        ["--port", "{{PORT}}"],
      );
      const result = new PortableRunner(
        runnerOptions as any,
        service,
      ).runPipeline(pipeline, augmentedOptions);
      result.then((res) => {
        res.waitUntilFinish().then((_state) => {
          service.stop();
        });
      });
      return result;
    }
  })();
}
