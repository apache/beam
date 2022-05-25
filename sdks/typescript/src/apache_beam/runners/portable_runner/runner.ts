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

import { ChannelCredentials } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { Struct } from "../../proto/google/protobuf/struct";

import * as runnerApiProto from "../../proto/beam_runner_api";
import { PrepareJobRequest } from "../../proto/beam_job_api";
import { JobServiceClient } from "../../proto/beam_job_api.client";
import { ArtifactStagingServiceClient } from "../../proto/beam_artifact_api.client";

import { Pipeline } from "../../internal/pipeline";
import { PipelineResult, Runner } from "../runner";
import { PipelineOptions } from "../../options/pipeline_options";
import { JobState_Enum, JobStateEvent } from "../../proto/beam_job_api";

import { ExternalWorkerPool } from "../../worker/external_worker_service";
import * as environments from "../../internal/environments";
import * as artifacts from "../artifacts";
import { Service as JobService } from "../../utils/service";

const TERMINAL_STATES = [
  JobState_Enum.DONE,
  JobState_Enum.FAILED,
  JobState_Enum.CANCELLED,
  JobState_Enum.UPDATED,
  JobState_Enum.DRAINED,
];

type completionCallback = (terminalState: JobStateEvent) => Promise<unknown>;

class PortableRunnerPipelineResult implements PipelineResult {
  jobId: string;
  runner: PortableRunner;
  completionCallbacks: completionCallback[];
  terminalState?: JobStateEvent;

  constructor(
    runner: PortableRunner,
    jobId: string,
    completionCallbacks: completionCallback[]
  ) {
    this.runner = runner;
    this.jobId = jobId;
    this.completionCallbacks = completionCallbacks;
  }

  static isTerminal(state: JobState_Enum) {
    return TERMINAL_STATES.includes(state);
  }

  async getState() {
    if (this.terminalState) {
      return this.terminalState;
    }
    const state = await this.runner.getJobState(this.jobId);
    if (PortableRunnerPipelineResult.isTerminal(state.state)) {
      this.terminalState = state;
      for (const callback of this.completionCallbacks) {
        await callback(state);
      }
    }
    return state;
  }

  /**
   * Waits until the pipeline finishes and returns the final status.
   * @param duration timeout in milliseconds.
   */
  async waitUntilFinish(duration?: number) {
    let { state } = await this.getState();
    const start = Date.now();
    let pollMillis = 10;
    while (!PortableRunnerPipelineResult.isTerminal(state)) {
      const now = Date.now();
      if (duration !== undefined && now - start > duration) {
        return state;
      }

      pollMillis = Math.min(10000, pollMillis * 1.2);
      await new Promise((r) => setTimeout(r, pollMillis));
      state = (await this.getState()).state;
    }
    return state;
  }
}

export class PortableRunner extends Runner {
  client?: JobServiceClient;
  defaultOptions: any;

  constructor(
    options: string | { jobEndpoint: string; [others: string]: any },
    private jobService: JobService | undefined = undefined
  ) {
    super();
    if (typeof options == "string") {
      this.defaultOptions = { jobEndpoint: options };
    } else if (options) {
      this.defaultOptions = options;
    }
  }

  async getClient(): Promise<JobServiceClient> {
    if (!this.client) {
      if (this.jobService) {
        this.defaultOptions.jobEndpoint = await this.jobService.start();
      }
      this.client = new JobServiceClient(
        new GrpcTransport({
          host: this.defaultOptions?.jobEndpoint,
          channelCredentials: ChannelCredentials.createInsecure(),
        })
      );
    }
    return this.client;
  }

  async getJobState(jobId: string) {
    const call = (await this.getClient()).getState({ jobId });
    return await call.response;
  }

  async runPipeline(
    pipeline: Pipeline,
    options?: PipelineOptions
  ): Promise<PipelineResult> {
    return this.runPipelineWithProto(pipeline.getProto(), options);
  }

  async runPipelineWithProto(
    pipeline: runnerApiProto.Pipeline,
    options?: PipelineOptions
  ) {
    if (!options) {
      options = this.defaultOptions;
    } else {
      options = { ...this.defaultOptions, ...options };
    }

    const completionCallbacks: completionCallback[] = [];

    if (this.jobService) {
      const jobService = this.jobService;
      completionCallbacks.push(() => jobService.stop());
    }

    let loopbackAddress: string | undefined = undefined;
    if ((options as any)?.environmentType == "LOOPBACK") {
      const workers = new ExternalWorkerPool();
      loopbackAddress = await workers.start();
      completionCallbacks.push(() => workers.stop());
    }

    // Replace the default environment according to the pipeline options.
    pipeline = runnerApiProto.Pipeline.clone(pipeline);
    for (const [envId, env] of Object.entries(
      pipeline.components!.environments
    )) {
      if (env.urn == environments.TYPESCRIPT_DEFAULT_ENVIRONMENT_URN) {
        if (loopbackAddress) {
          pipeline.components!.environments[envId] =
            environments.asExternalEnvironment(env, loopbackAddress);
        } else {
          pipeline.components!.environments[envId] =
            environments.asDockerEnvironment(
              env,
              (options as any)?.sdkContainerImage ||
                "gcr.io/apache-beam-testing/beam_typescript_sdk:dev"
            );
        }
      }
    }

    // Inform the runner that we'd like to execute this pipeline.
    console.debug("Preparing job.");
    let message: PrepareJobRequest = {
      pipeline,
      jobName: (options as any)?.jobName || "",
    };
    if (options) {
      const camel_to_snake = (option) =>
        option.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
      message.pipelineOptions = Struct.fromJson(
        Object.fromEntries(
          Object.entries(options).map(([k, v]) => [
            `beam:option:${camel_to_snake(k)}:v1`,
            v,
          ])
        )
      );
    }
    const client = await this.getClient();
    const prepareResponse = await client.prepare(message).response;

    // Allow the runner to fetch any artifacts it can't interpret.
    if (prepareResponse.artifactStagingEndpoint) {
      console.debug("Staging artifacts");
      await artifacts.offerArtifacts(
        new ArtifactStagingServiceClient(
          new GrpcTransport({
            host: prepareResponse.artifactStagingEndpoint.url,
            channelCredentials: ChannelCredentials.createInsecure(),
          })
        ),
        prepareResponse.stagingSessionToken
      );
    }

    // Actually kick off the job.
    console.debug("Running job.");
    const runCall = client.run({
      preparationId: prepareResponse.preparationId,
      retrievalToken: "",
    });
    const { jobId } = await runCall.response;

    // Return a handle the user can use to monitor the job as it's running.
    // If desired, the user can use this handle to await job completion, but
    // this function returns as soon as the job is successfully started, not
    // once the job has completed.
    return new PortableRunnerPipelineResult(this, jobId, completionCallbacks);
  }
}
