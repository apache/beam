import * as runnerApi from "../proto/beam_runner_api";
import { PValue } from "../pvalue";
import { Pipeline } from "../base";

export class AsyncPTransform<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> {
  name: string;

  constructor(name: string | null = null) {
    this.name = name || typeof this;
  }

  async asyncExpand(input: InputT): Promise<OutputT> {
    throw new Error("Method expand has not been implemented.");
  }

  async asyncExpandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: InputT
  ): Promise<OutputT> {
    return this.asyncExpand(input);
  }
}

export class PTransform<
  InputT extends PValue<any>,
  OutputT extends PValue<any>
> extends AsyncPTransform<InputT, OutputT> {
  expand(input: InputT): OutputT {
    throw new Error("Method expand has not been implemented.");
  }

  async asyncExpand(input: InputT): Promise<OutputT> {
    return this.expand(input);
  }

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: InputT
  ): OutputT {
    return this.expand(input);
  }

  async asyncExpandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: InputT
  ): Promise<OutputT> {
    return this.expandInternal(pipeline, transformProto, input);
  }
}
