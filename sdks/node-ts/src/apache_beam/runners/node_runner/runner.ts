import { RemoteJobServiceClient } from "./client";
import { Pipeline, PipelineResult, Runner } from "../../base";
import { PipelineOptions } from "../../options/pipeline_options";
import * as runnerApiProto from '../../proto/beam_runner_api';
import { JobState_Enum } from "../../proto/beam_job_api";

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

    constructor(runner: NodeRunner, jobId: string) {
        this.runner = runner;
        this.jobId = jobId;
    }

    static isTerminal(state: JobState_Enum) {
        return TERMINAL_STATES.includes(state);
    }

    async getState() {
        return await this.runner.getJobState(this.jobId);
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

    async runPipeline(pipeline: Pipeline): Promise<PipelineResult> {
        throw new Error('runPipeline not implemented.')
    }

    async runPipelineWithProto(
        pipeline: runnerApiProto.Pipeline,
        jobName: string,
        options?: PipelineOptions) {
        const { preparationId } = await this.client.prepare(pipeline, jobName, options);
        const { jobId } = await this.client.run(preparationId);
        return new NodeRunnerPipelineResult(this, jobId);
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
