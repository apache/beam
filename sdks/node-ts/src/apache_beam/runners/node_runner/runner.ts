import { RemoteJobServiceClient } from "./client";
import { Pipeline, PipelineResult, Runner } from "../../base";
import { PipelineOptions } from "../../options/pipeline_options";
import * as runnerApiProto from '../../proto/beam_runner_api';
import { JobState_Enum } from "../../proto/beam_job_api";

import { ExternalWorkerPool } from "../../worker/external_worker_service"
import * as environments from '../../internal/environments';

const TERMINAL_STATES = [
    JobState_Enum.DONE,
    JobState_Enum.FAILED,
    JobState_Enum.CANCELLED,
    JobState_Enum.UPDATED,
    JobState_Enum.DRAINED,
]

class NodeRunnerPipelineResult implements PipelineResult {
    jobId: string;
    runner: NodeRunner;
    workers?: ExternalWorkerPool;

    constructor(runner: NodeRunner, jobId: string, workers: ExternalWorkerPool | undefined = undefined) {
        this.runner = runner;
        this.jobId = jobId;
        this.workers = workers;
    }

    static isTerminal(state: JobState_Enum) {
        return TERMINAL_STATES.includes(state);
    }

    async getState() {
        const state = await this.runner.getJobState(this.jobId);
        if (this.workers != undefined && NodeRunnerPipelineResult.isTerminal(state.state)) {
            this.workers.stop()
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
        while (!NodeRunnerPipelineResult.isTerminal(state)) {
            const now = Date.now();
            if (duration !== undefined && now - start > duration) {
                return state;
            }

            state = (await this.getState()).state;
        }
        return state;
    }
}

export class NodeRunner extends Runner {
    client: RemoteJobServiceClient;

    constructor(client: RemoteJobServiceClient) {
        super();
        this.client = client;
    }

    async getJobState(jobId: string) {
        return this.client.getState(jobId);
    }

    async runPipeline(pipeline: Pipeline, options?: PipelineOptions): Promise<PipelineResult> {
        return this.runPipelineWithProto(pipeline.getProto(), '', options);
    }

    async runPipelineWithProto(
        pipeline: runnerApiProto.Pipeline,
        jobName: string,
        options?: PipelineOptions) {

        // Replace the default environment according to the pipeline options.
        const externalWorkerServiceAddress = 'localhost:5555'
        const workers = new ExternalWorkerPool(externalWorkerServiceAddress)
        workers.start()

        pipeline = runnerApiProto.Pipeline.clone(pipeline);
        for (const [envId, env] of Object.entries(pipeline.components!.environments)) {
            if (env.urn == environments.PYTHON_DEFAULT_ENVIRONMENT_URN) {
                pipeline.components!.environments[envId] = environments.asExternalEnvironment(env, externalWorkerServiceAddress);
            }
        }

        const { preparationId } = await this.client.prepare(pipeline, jobName, options);
        const { jobId } = await this.client.run(preparationId);
        return new NodeRunnerPipelineResult(this, jobId, workers);
    }

    async runPipelineWithJsonValueProto(json: string, jobName: string, options?: PipelineOptions) {
        return this.runPipelineWithProto(
            runnerApiProto.Pipeline.fromJsonString(json),
            jobName,
            options,
        );
    }

    async runPipelineWithJsonStringProto(json: string, jobName: string, options?: PipelineOptions) {
        return this.runPipelineWithProto(
            runnerApiProto.Pipeline.fromJsonString(json),
            jobName,
            options,
        );
    }
}
