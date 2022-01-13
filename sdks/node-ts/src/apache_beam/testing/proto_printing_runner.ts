import { Runner, PipelineResult } from "../runners/runner";

/**
 * A runner that simply prints the pipeline definition and returns.
 */
export class ProtoPrintingRunner extends Runner {
  async runPipeline(pipeline): Promise<PipelineResult> {
    console.dir(pipeline.proto, { depth: null });
    return {
      waitUntilFinish: (duration?) => Promise.reject("not implemented"),
    };
  }
}
