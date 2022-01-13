import * as runnerApi from "../proto/beam_runner_api";
import * as urns from "../internal/urns";

import { GeneralObjectCoder } from "../coders/js_coders";
import { PCollection } from "../pvalue";
import { Pipeline, fakeSeralize } from "../base";
import { PTransform } from "./transform";
import { WindowedValue } from "../values";

export class DoFn<InputT, OutputT> {
  *process(element: InputT): Generator<OutputT> | void {
    throw new Error("Method process has not been implemented!");
  }

  startBundle() {}

  *finishBundle(): Generator<WindowedValue<OutputT>> | void {}
}

export class ParDo<InputT, OutputT> extends PTransform<
  PCollection<InputT>,
  PCollection<OutputT>
> {
  private doFn: DoFn<InputT, OutputT>;
  // static urn: string = runnerApi.StandardPTransforms_Primitives.PAR_DO.urn;
  // TODO: use above line, not below line.
  static urn: string = "beam:transform:pardo:v1";
  constructor(doFn: DoFn<InputT, OutputT>) {
    super("ParDo(" + doFn + ")");
    this.doFn = doFn;
  }

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<InputT>
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SERIALIZED_JS_DOFN_INFO,
            payload: fakeSeralize(this.doFn),
          }),
        })
      ),
    });

    // For the ParDo output coder, we use a GeneralObjectCoder, which is a Javascript-specific
    // coder to encode the various types that exist in JS.
    return pipeline.createPCollectionInternal<OutputT>(
      new GeneralObjectCoder()
    );
  }
}

// TODO: Consider as top-level method.
// TODO: Naming.
// TODO: Allow default?  Technically splitter can be implemented/wrapped to produce such.
// TODO: Can we enforce splitter's output with the typing system to lie in targets?
export class Split<T> extends PTransform<
  PCollection<T>,
  { [key: string]: PCollection<T> }
> {
  private tags: string[];
  constructor(private splitter: (T) => string, ...tags: string[]) {
    super("Split(" + tags + ")");
    this.tags = tags;
  }
  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    input: PCollection<T>
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SPLITTING_JS_DOFN_URN,
            payload: fakeSeralize({ splitter: this.splitter }),
          }),
        })
      ),
    });

    const this_ = this;
    return Object.fromEntries(
      this_.tags.map((tag) => [
        tag,
        pipeline.createPCollectionInternal<T>(
          pipeline.context.getPCollectionCoderId(input)
        ),
      ])
    );
  }
}
