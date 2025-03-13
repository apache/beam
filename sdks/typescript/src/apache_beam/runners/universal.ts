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

export function universalRunner(runnerOptions: {
  [others: string]: any;
}): Runner {
  return new (class extends Runner {
    async runPipeline(
      pipeline: Pipeline,
      options: Object = {},
    ): Promise<PipelineResult> {
      return new PortableRunner(
        runnerOptions as any,
        PythonService.forModule(
          "apache_beam.runners.portability.local_job_service_main",
          ["--port", "{{PORT}}"],
        ),
      ).runPipeline(pipeline, { directEmbedDockerPython: true, ...options });
    }
  })();
}
