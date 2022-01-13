import * as runnerApi from "../proto/beam_runner_api";
import { PTransform } from "./transform";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { GeneralObjectCoder } from "../coders/js_coders";

export class Flatten<T> extends PTransform<PCollection<T>[], PCollection<T>> {
  // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
  // TODO: use above line, not below line.
  static urn: string = "beam:transform:flatten:v1";

  expandInternal(
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
    inputs: PCollection<any>[]
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: Flatten.urn,
      payload: null!,
    });

    // TODO: Input coder if they're all the same? UnionCoder?
    return pipeline.createPCollectionInternal<T>(new GeneralObjectCoder());
  }
}
