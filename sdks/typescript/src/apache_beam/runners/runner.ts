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
import * as runnerApi from "../proto/beam_runner_api";
import { MonitoringInfo } from "../proto/metrics";
import { Pipeline } from "../internal/pipeline";
import { Root, PValue } from "../pvalue";
import { PipelineOptions } from "../options/pipeline_options";
import * as metrics from "../worker/metrics";

export class PipelineResult {
  waitUntilFinish(duration?: number): Promise<JobState_Enum> {
    throw new Error("NotImplemented");
  }

  async rawMetrics(): Promise<MonitoringInfo[]> {
    throw new Error("NotImplemented");
  }

  // TODO: Support filtering, slicing.
  async counters(): Promise<{ [key: string]: number }> {
    return Object.fromEntries(
      metrics.aggregateMetrics(
        await this.rawMetrics(),
        "beam:metric:user:sum_int64:v1",
      ),
    );
  }

  async distributions(): Promise<{ [key: string]: number }> {
    return Object.fromEntries(
      metrics.aggregateMetrics(
        await this.rawMetrics(),
        "beam:metric:user:distribution_int64:v1",
      ),
    );
  }
}

/**
 * Creates a `Runner` object with the given set of options.
 *
 * The exact type of runner to be created can be specified by the special
 * `runner` option, e.g. `createRunner({runner: "direct"})` would create
 * a direct runner.  If no runner option is specified, the "default" runner
 * is used, which runs what pipelines it can on the direct runner, and
 * otherwise falls back to the universal runner (e.g. if cross-language
 * transforms, non-trivial windowing, etc. are used).
 */
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
 * executes, e.g. locally or on a distributed system, by invoking its
 * `run` or `runAsync` method.
 *
 * Runners are generally created using the `createRunner` method in this
 * same module.
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
    options?: PipelineOptions,
  ): Promise<PipelineResult> {
    const pipelineResult = await this.runAsync(pipeline, options);
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
    options?: PipelineOptions,
  ): Promise<PipelineResult> {
    const p = new Pipeline();
    await pipeline(new Root(p));
    return this.runPipeline(p.getProto());
  }

  abstract runPipeline(
    pipeline: runnerApi.Pipeline,
    options?: PipelineOptions,
  ): Promise<PipelineResult>;
}

export function defaultRunner(defaultOptions: Object): Runner {
  return new (class extends Runner {
    async runPipeline(
      pipeline: runnerApi.Pipeline,
      options: Object = {},
    ): Promise<PipelineResult> {
      const directRunner =
        require("./direct_runner").directRunner(defaultOptions);
      if (directRunner.unsupportedFeatures(pipeline, options).length === 0) {
        return directRunner.runPipeline(pipeline, options);
      } else {
        return loopbackRunner(defaultOptions).runPipeline(pipeline, options);
      }
    }
  })();
}

export function loopbackRunner(defaultOptions: Object = {}): Runner {
  return require("./universal").universalRunner({
    environmentType: "LOOPBACK",
    ...defaultOptions,
  });
}
