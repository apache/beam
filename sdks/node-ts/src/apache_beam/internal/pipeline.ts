import equal from "fast-deep-equal";

import * as runnerApi from "../proto/beam_runner_api";
import { PTransform, AsyncPTransform } from "../transforms/transform";
import { GlobalWindows } from "../transforms/windowings";
import { PValue, PCollection, flattenPValue } from "../pvalue";
import { PipelineContext } from "../base";
import { WindowInto } from "../base";
import * as environments from "./environments";
import { Coder } from "../coders/coders";

/**
 * A Pipeline holds the Beam DAG and auxiliary information needed to construct
 * Beam Pipelines.
 */
export class Pipeline {
  context: PipelineContext;
  transformStack: string[] = [];
  defaultEnvironment: string;

  private proto: runnerApi.Pipeline;
  private globalWindowing: string;

  constructor() {
    this.defaultEnvironment = "jsEnvironment";
    this.globalWindowing = "globalWindowing";
    this.proto = runnerApi.Pipeline.create({
      components: runnerApi.Components.create({}),
    });
    this.proto.components!.environments[this.defaultEnvironment] =
      environments.defaultJsEnvironment();
    this.context = new PipelineContext(this.proto.components!);
    this.proto.components!.windowingStrategies[this.globalWindowing] =
      WindowInto.createWindowingStrategy(this, new GlobalWindows());
  }

  preApplyTransform<InputT extends PValue<any>, OutputT extends PValue<any>>(
    transform: AsyncPTransform<InputT, OutputT>,
    input: InputT,
    name: string
  ) {
    const this_ = this;
    const transformId = this.context.createUniqueName("transform");
    if (this.transformStack.length) {
      this.proto!.components!.transforms![
        this.transformStack[this.transformStack.length - 1]
      ].subtransforms.push(transformId);
    } else {
      this.proto.rootTransformIds.push(transformId);
    }
    const transformProto: runnerApi.PTransform = {
      uniqueName:
        transformId +
        this.transformStack
          .map((id) => this_.proto?.components?.transforms![id].uniqueName)
          .concat([name || transform.name])
          .join("/"),
      subtransforms: [],
      inputs: objectMap(flattenPValue(input), (pc) => pc.getId()),
      outputs: {},
      environmentId: this.defaultEnvironment,
      displayData: [],
      annotations: {},
    };
    this.proto.components!.transforms![transformId] = transformProto;
    return { id: transformId, proto: transformProto };
  }

  applyTransform<InputT extends PValue<any>, OutputT extends PValue<any>>(
    transform: PTransform<InputT, OutputT>,
    input: InputT,
    name: string
  ) {
    const { id: transformId, proto: transformProto } = this.preApplyTransform(
      transform,
      input,
      name
    );
    let result: OutputT;
    try {
      this.transformStack.push(transformId);
      result = transform.expandInternal(this, transformProto, input);
    } finally {
      this.transformStack.pop();
    }
    return this.postApplyTransform(transform, transformProto, result);
  }

  async asyncApplyTransform<
    InputT extends PValue<any>,
    OutputT extends PValue<any>
  >(transform: AsyncPTransform<InputT, OutputT>, input: InputT, name: string) {
    const { id: transformId, proto: transformProto } = this.preApplyTransform(
      transform,
      input,
      name
    );
    let result: OutputT;
    try {
      this.transformStack.push(transformId);
      result = await transform.asyncExpandInternal(this, transformProto, input);
    } finally {
      this.transformStack.pop();
    }
    return this.postApplyTransform(transform, transformProto, result);
  }

  postApplyTransform<InputT extends PValue<any>, OutputT extends PValue<any>>(
    transform: AsyncPTransform<InputT, OutputT>,
    transformProto: runnerApi.PTransform,
    result: OutputT
  ) {
    transformProto.outputs = objectMap(flattenPValue(result), (pc) =>
      pc.getId()
    );

    // Propagate any unset PCollection properties.
    const this_ = this;
    const inputProtos = Object.values(transformProto.inputs).map(
      (id) => this_.proto.components!.pcollections[id]
    );
    const inputBoundedness = new Set(
      inputProtos.map((proto) => proto.isBounded)
    );
    const inputWindowings = new Set(
      inputProtos.map((proto) => proto.windowingStrategyId)
    );

    for (const pcId of Object.values(transformProto.outputs)) {
      const pcProto = this.proto!.components!.pcollections[pcId];
      if (!pcProto.isBounded) {
        pcProto.isBounded = onlyValueOr(
          inputBoundedness,
          runnerApi.IsBounded_Enum.BOUNDED
        );
      }
      // TODO: Handle the case of equivalent strategies.
      if (!pcProto.windowingStrategyId) {
        pcProto.windowingStrategyId = onlyValueOr(
          inputWindowings,
          this.globalWindowing,
          (a, b) => {
            return equal(
              this_.proto.components!.windowingStrategies[a],
              this_.proto.components!.windowingStrategies[b]
            );
          }
        );
      }
    }

    return result;
  }

  createPCollectionInternal<OutputT>(
    coder: Coder<OutputT> | string,
    windowingStrategy:
      | runnerApi.WindowingStrategy
      | string
      | undefined = undefined,
    isBounded: runnerApi.IsBounded_Enum | undefined = undefined
  ): PCollection<OutputT> {
    return new PCollection<OutputT>(
      this,
      this.createPCollectionIdInternal(coder, windowingStrategy, isBounded)
    );
  }

  createPCollectionIdInternal<OutputT>(
    coder: Coder<OutputT> | string,
    windowingStrategy:
      | runnerApi.WindowingStrategy
      | string
      | undefined = undefined,
    isBounded: runnerApi.IsBounded_Enum | undefined = undefined
  ): string {
    const pcollId = this.context.createUniqueName("pc");
    let coderId: string;
    let windowingStrategyId: string;
    if (typeof coder == "string") {
      coderId = coder;
    } else {
      coderId = this.context.getCoderId(coder);
    }
    if (windowingStrategy == undefined) {
      windowingStrategyId = undefined!;
    } else if (typeof windowingStrategy == "string") {
      windowingStrategyId = windowingStrategy;
    } else {
      windowingStrategyId = this.context.getWindowingStrategyId(
        windowingStrategy!
      );
    }
    this.proto!.components!.pcollections[pcollId] = {
      uniqueName: pcollId, // TODO: name according to producing transform?
      coderId: coderId,
      isBounded: isBounded!,
      windowingStrategyId: windowingStrategyId,
      displayData: [],
    };
    return pcollId;
  }

  getCoder<T>(coderId: string): Coder<T> {
    return this.context.getCoder(coderId);
  }

  getCoderId(coder: Coder<any>): string {
    return this.context.getCoderId(coder);
  }

  getProto(): runnerApi.Pipeline {
    return this.proto;
  }
}

function objectMap(obj, func) {
  return Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, func(v)]));
}

function onlyValueOr<T>(
  valueSet: Set<T>,
  defaultValue: T,
  comparator: (a: T, b: T) => boolean = (a, b) => false
) {
  if (valueSet.size == 0) {
    return defaultValue;
  } else if (valueSet.size == 1) {
    return valueSet.values().next().value;
  } else {
    const candidate = valueSet.values().next().value;
    for (const other of valueSet) {
      if (!comparator(candidate, other)) {
        throw new Error("Unable to deduce single value from " + valueSet);
      }
    }
    return candidate;
  }
}
