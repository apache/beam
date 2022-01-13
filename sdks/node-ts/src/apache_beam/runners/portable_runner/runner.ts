import { ChannelCredentials } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";

import { PrepareJobRequest } from "../../proto/beam_job_api";
// TODO: standardize on .client or .grpc-client.
import { JobServiceClient } from "../../proto/beam_job_api.client";

import { Pipeline } from "../../base";
import { PipelineResult, Runner } from "../runner";
import { PipelineOptions } from "../../options/pipeline_options";
import * as runnerApiProto from "../../proto/beam_runner_api";
import { JobState_Enum } from "../../proto/beam_job_api";

import { ExternalWorkerPool } from "../../worker/external_worker_service";
import * as environments from "../../internal/environments";

const TERMINAL_STATES = [
  JobState_Enum.DONE,
  JobState_Enum.FAILED,
  JobState_Enum.CANCELLED,
  JobState_Enum.UPDATED,
  JobState_Enum.DRAINED,
];

class PortableRunnerPipelineResult implements PipelineResult {
  jobId: string;
  runner: PortableRunner;
  workers?: ExternalWorkerPool;

  constructor(
    runner: PortableRunner,
    jobId: string,
    workers: ExternalWorkerPool | undefined = undefined
  ) {
    this.runner = runner;
    this.jobId = jobId;
    this.workers = workers;
  }

  static isTerminal(state: JobState_Enum) {
    return TERMINAL_STATES.includes(state);
  }

  async getState() {
    const state = await this.runner.getJobState(this.jobId);
    if (
      this.workers != undefined &&
      PortableRunnerPipelineResult.isTerminal(state.state)
    ) {
      this.workers.stop();
      this.workers = undefined;
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
    while (!PortableRunnerPipelineResult.isTerminal(state)) {
      const now = Date.now();
      if (duration !== undefined && now - start > duration) {
        return state;
      }

      state = (await this.getState()).state;
    }
    return state;
  }
}

export class PortableRunner extends Runner {
  client: JobServiceClient;

  constructor(host: string) {
    super();
    this.client = new JobServiceClient(
      new GrpcTransport({
        host,
        channelCredentials: ChannelCredentials.createInsecure(),
      })
    );
  }

  async getJobState(jobId: string) {
    const call = this.client.getState({ jobId });
    return await call.response;
  }

  async runPipeline(
    pipeline: Pipeline,
    options?: PipelineOptions
  ): Promise<PipelineResult> {
    return this.runPipelineWithProto(pipeline.getProto(), "", options);
  }

  async runPipelineWithProto(
    pipeline: runnerApiProto.Pipeline,
    jobName: string,
    options?: PipelineOptions
  ) {
    // Replace the default environment according to the pipeline options.
    // TODO: Choose a free port.
    const externalWorkerServiceAddress = "localhost:5555";
    const workers = new ExternalWorkerPool(externalWorkerServiceAddress);
    workers.start();

    pipeline = runnerApiProto.Pipeline.clone(pipeline);
    for (const [envId, env] of Object.entries(
      pipeline.components!.environments
    )) {
      if (env.urn == environments.PYTHON_DEFAULT_ENVIRONMENT_URN) {
        pipeline.components!.environments[envId] =
          environments.asExternalEnvironment(env, externalWorkerServiceAddress);
      }
    }

    let message: PrepareJobRequest = { pipeline, jobName };
    if (options) {
      message.pipelineOptions = options;
    }
    const prepareCall = this.client.prepare(message);
    const { preparationId } = await prepareCall.response;

    const runCall = this.client.run({ preparationId, retrievalToken: "" });
    const { jobId } = await runCall.response;
    return new PortableRunnerPipelineResult(this, jobId, workers);
  }

  async runPipelineWithJsonValueProto(
    json: string,
    jobName: string,
    options?: PipelineOptions
  ) {
    return this.runPipelineWithProto(
      runnerApiProto.Pipeline.fromJsonString(json),
      jobName,
      options
    );
  }

  async runPipelineWithJsonStringProto(
    json: string,
    jobName: string,
    options?: PipelineOptions
  ) {
    return this.runPipelineWithProto(
      runnerApiProto.Pipeline.fromJsonString(json),
      jobName,
      options
    );
  }
}
