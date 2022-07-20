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

import { JobState_Enum } from "../proto/beam_job_api";
import { Pipeline } from "../internal/pipeline";
import { Root, PValue } from "../pvalue";
import { PipelineOptions } from "../options/pipeline_options";

export interface PipelineResult {
  waitUntilFinish(duration?: number): Promise<JobState_Enum>;
}

export function createRunner(options: any = {}): Runner {
  let runnerConstructor: (any) => Runner;
  if (options.runner === undefined || options.runner === "default") {
    runnerConstructor = defaultRunner;
  } else if (options.runner === "direct") {
    runnerConstructor = require("./direct_runner").directRunner;
  } else if (options.runner === "universal") {
    runnerConstructor = require("./universal").universalRunner;
  } else if (options.runner === "flink") {
    runnerConstructor = require("./flink").flinkRunner;
  } else if (options.runner === "dataflow") {
    runnerConstructor = require("./dataflow").dataflowRunner;
  } else {
    throw new Error("Unknown runner: " + options.runner);
  }
  return runnerConstructor(options);
}

/**
 * A Runner is the object that takes a pipeline definition and actually
 * executes, e.g. locally or on a distributed system.
 */
export abstract class Runner {
  /**
   * Runs the transform.
   *
   * Resolves to an instance of PipelineResult when the pipeline completes.
   * Use runAsync() to execute the pipeline in the background.
   *
   * @param pipeline
   * @returns A PipelineResult
   */
  async run(
    pipeline: (root: Root) => PValue<any> | Promise<PValue<any>>,
    options?: PipelineOptions
  ): Promise<PipelineResult> {
    const p = new Pipeline();
    await pipeline(new Root(p));
    const pipelineResult = await this.runPipeline(p, options);
    const finalState = await pipelineResult.waitUntilFinish();
    if (finalState != JobState_Enum.DONE) {
      // TODO: Grab the last/most severe error message?
      throw new Error("Job finished in state " + JobState_Enum[finalState]);
    }
    return pipelineResult;
  }

  /**
   * runAsync() is the asynchronous version of run(), does not wait until
   * pipeline finishes. Use the returned PipelineResult to query job
   * status.
   */
  async runAsync(
    pipeline: (root: Root) => PValue<any> | Promise<PValue<any>>,
    options?: PipelineOptions
  ): Promise<PipelineResult> {
    const p = new Pipeline();
    await pipeline(new Root(p));
    return this.runPipeline(p);
  }

  abstract runPipeline(
    pipeline: Pipeline,
    options?: PipelineOptions
  ): Promise<PipelineResult>;
}

export function defaultRunner(defaultOptions: Object): Runner {
  return new (class extends Runner {
    async runPipeline(
      pipeline: Pipeline,
      options: Object = {}
    ): Promise<PipelineResult> {
      const directRunner =
        require("./direct_runner").directRunner(defaultOptions);
      if (directRunner.unsupportedFeatures(pipeline, options).length === 0) {
        return directRunner.runPipeline(pipeline, options);
      } else {
        return require("./universal")
          .universalRunner({ environmentType: "LOOPBACK", ...defaultOptions })
          .runPipeline(pipeline, options);
      }
    }
  })();
}
