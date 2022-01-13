import { JobState_Enum } from "../proto/beam_job_api";
import { Root, PValue, Pipeline } from "../base";
import { PipelineOptions } from "../options/pipeline_options";

export interface PipelineResult {
  waitUntilFinish(duration?: number): Promise<JobState_Enum>;
}

/**
 * A Runner is the object that takes a pipeline definition and actually
 * executes, e.g. locally or on a distributed system.
 */
export class Runner {
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
    await pipelineResult.waitUntilFinish();
    return pipelineResult;
  }

  /**
   * runAsync() is the asynchronous version of run(), does not wait until
   * pipeline finishes. Use the returned PipelineResult to query job
   * status.
   */
  async runAsync(
    pipeline: (root: Root) => PValue<any>,
    options?: PipelineOptions
  ): Promise<PipelineResult> {
    const p = new Pipeline();
    pipeline(new Root(p));
    return this.runPipeline(p);
  }

  protected async runPipeline(
    pipeline: Pipeline,
    options?: PipelineOptions
  ): Promise<PipelineResult> {
    throw new Error("Not implemented.");
  }
}
