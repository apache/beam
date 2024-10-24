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

import equal from "fast-deep-equal";

import * as runnerApi from "../proto/beam_runner_api";
import * as fnApi from "../proto/beam_fn_api";
import {
  PTransformClass,
  AsyncPTransformClass,
  extractName,
} from "../transforms/transform";
import { globalWindows } from "../transforms/windowings";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";
import * as pvalue from "../pvalue";
import { createWindowingStrategyProto } from "../transforms/window";
import * as environments from "./environments";
import { Coder, globalRegistry as globalCoderRegistry } from "../coders/coders";

type Components = runnerApi.Components | fnApi.ProcessBundleDescriptor;

// TODO: Cleanup. Where to put this.
export class PipelineContext {
  counter: number = 0;

  private coders: { [key: string]: Coder<any> } = {};

  constructor(
    public components: Components,
    private componentPrefix: string,
  ) {}

  getCoder<T>(coderId: string): Coder<T> {
    const this_ = this;
    if (this.coders[coderId] === null || this.coders[coderId] === undefined) {
      const coderProto = this.components.coders[coderId];
      const components: Coder<unknown>[] = (
        coderProto.componentCoderIds || []
      ).map(this_.getCoder.bind(this_));
      this.coders[coderId] = globalCoderRegistry().getCoder(
        coderProto.spec!.urn,
        coderProto.spec!.payload,
        ...components,
      );
    }
    return this.coders[coderId];
  }

  getCoderId(coder: Coder<any>): string {
    return this.getOrAssign(
      this.components.coders,
      coder.toProto(this),
      "coder",
    );
  }

  getPCollectionCoder<T>(pcoll: pvalue.PCollection<T>): Coder<T> {
    return this.getCoder(this.getPCollectionCoderId(pcoll));
  }

  getPCollectionCoderId<T>(pcoll: pvalue.PCollection<T>): string {
    const pcollId = typeof pcoll === "string" ? pcoll : pcoll.getId();
    return this.components!.pcollections[pcollId].coderId;
  }

  getWindowingStrategy(id: string): runnerApi.WindowingStrategy {
    return this.components.windowingStrategies[id];
  }

  getWindowingStrategyId(windowing: runnerApi.WindowingStrategy): string {
    return this.getOrAssign(
      this.components.windowingStrategies,
      windowing,
      "windowing",
    );
  }

  private getOrAssign<T>(
    existing: { [key: string]: T },
    obj: T,
    prefix: string,
  ) {
    for (const [id, other] of Object.entries(existing)) {
      if (equal(other, obj)) {
        return id;
      }
    }
    const newId = this.createUniqueName(prefix);
    existing[newId] = obj;
    return newId;
  }

  createUniqueName(prefix: string): string {
    return this.componentPrefix + prefix + "_" + this.counter++;
  }
}

/**
 * A Pipeline holds the Beam DAG and auxiliary information needed to construct
 * Beam Pipelines.
 */
export class Pipeline {
  context: PipelineContext;
  transformStack: string[] = [];
  defaultEnvironment: string;
  usedStageNames: Set<string> = new Set();

  private proto: runnerApi.Pipeline;
  private globalWindowing: string;

  constructor(componentPrefix: string = "") {
    this.defaultEnvironment = componentPrefix + "jsEnvironment";
    this.globalWindowing = componentPrefix + "globalWindowing";
    this.proto = runnerApi.Pipeline.create({
      components: runnerApi.Components.create({}),
    });
    this.proto.components!.environments[this.defaultEnvironment] =
      environments.defaultJsEnvironment();
    this.context = new PipelineContext(this.proto.components!, componentPrefix);
    this.proto.components!.windowingStrategies[this.globalWindowing] =
      createWindowingStrategyProto(this, globalWindows());
  }

  preApplyTransform<
    InputT extends pvalue.PValue<any>,
    OutputT extends pvalue.PValue<any>,
  >(transform: AsyncPTransformClass<InputT, OutputT>, input: InputT) {
    const this_ = this;
    const transformId = this.context.createUniqueName("transform");
    let parent: runnerApi.PTransform | undefined = undefined;
    if (this.transformStack.length) {
      parent =
        this.proto!.components!.transforms![
          this.transformStack[this.transformStack.length - 1]
        ];
      parent.subtransforms.push(transformId);
    } else {
      this.proto.rootTransformIds.push(transformId);
    }
    const uniqueName =
      (parent ? parent.uniqueName + "/" : "") + extractName(transform);
    if (this.usedStageNames.has(uniqueName)) {
      throw new Error(
        `Duplicate stage name: "${uniqueName}". ` +
          "Use beam.withName(...) to give your transform a unique name.",
      );
    }
    this.usedStageNames.add(uniqueName);
    const transformProto: runnerApi.PTransform = {
      uniqueName,
      subtransforms: [],
      inputs: objectMap(pvalue.flattenPValue(input), (pc) => pc.getId()),
      outputs: {},
      environmentId: this.defaultEnvironment,
      displayData: [],
      annotations: {},
    };
    this.proto.components!.transforms![transformId] = transformProto;
    return { id: transformId, proto: transformProto };
  }

  applyTransform<
    InputT extends pvalue.PValue<any>,
    OutputT extends pvalue.PValue<any>,
  >(transform: PTransformClass<InputT, OutputT>, input: InputT) {
    const { id: transformId, proto: transformProto } = this.preApplyTransform(
      transform,
      input,
    );
    let result: OutputT;
    try {
      this.transformStack.push(transformId);
      result = transform.expandInternal(input, this, transformProto);
    } finally {
      this.transformStack.pop();
    }
    return this.postApplyTransform(transform, transformProto, result);
  }

  async applyAsyncTransform<
    InputT extends pvalue.PValue<any>,
    OutputT extends pvalue.PValue<any>,
  >(transform: AsyncPTransformClass<InputT, OutputT>, input: InputT) {
    const { id: transformId, proto: transformProto } = this.preApplyTransform(
      transform,
      input,
    );
    let result: OutputT;
    try {
      this.transformStack.push(transformId);
      result = await transform.expandInternalAsync(input, this, transformProto);
    } finally {
      this.transformStack.pop();
    }
    return this.postApplyTransform(transform, transformProto, result);
  }

  postApplyTransform<
    InputT extends pvalue.PValue<any>,
    OutputT extends pvalue.PValue<any>,
  >(
    transform: AsyncPTransformClass<InputT, OutputT>,
    transformProto: runnerApi.PTransform,
    result: OutputT,
  ) {
    transformProto.outputs = objectMap(pvalue.flattenPValue(result), (pc) =>
      pc.getId(),
    );

    // Propagate any unset pvalue.PCollection properties.
    const this_ = this;
    const inputProtos = Object.values(transformProto.inputs).map(
      (id) => this_.proto.components!.pcollections[id],
    );
    const inputBoundedness = new Set(
      inputProtos.map((proto) => proto.isBounded),
    );
    const inputWindowings = new Set(
      inputProtos.map((proto) => proto.windowingStrategyId),
    );

    for (const pcId of Object.values(transformProto.outputs)) {
      const pcProto = this.proto!.components!.pcollections[pcId];
      if (!pcProto.isBounded) {
        pcProto.isBounded = onlyValueOr(
          inputBoundedness,
          runnerApi.IsBounded_Enum.BOUNDED,
        );
      }
      // TODO: (Cleanup) Handle the case of equivalent strategies.
      if (!pcProto.windowingStrategyId) {
        pcProto.windowingStrategyId = onlyValueOr(
          inputWindowings,
          this.globalWindowing,
          (a, b) => {
            return equal(
              this_.proto.components!.windowingStrategies[a],
              this_.proto.components!.windowingStrategies[b],
            );
          },
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
    isBounded: runnerApi.IsBounded_Enum | undefined = undefined,
  ): pvalue.PCollection<OutputT> {
    return new pvalue.PCollection<OutputT>(
      this,
      this.createPCollectionIdInternal(coder, windowingStrategy, isBounded),
    );
  }

  createPCollectionIdInternal<OutputT>(
    coder: Coder<OutputT> | string,
    windowingStrategy:
      | runnerApi.WindowingStrategy
      | string
      | undefined = undefined,
    isBounded: runnerApi.IsBounded_Enum | undefined = undefined,
  ): string {
    const pcollId = this.context.createUniqueName("pc");
    let coderId: string;
    let windowingStrategyId: string;
    if (typeof coder === "string") {
      coderId = coder;
    } else {
      coderId = this.context.getCoderId(coder);
    }
    if (windowingStrategy === null || windowingStrategy === undefined) {
      windowingStrategyId = undefined!;
    } else if (typeof windowingStrategy === "string") {
      windowingStrategyId = windowingStrategy;
    } else {
      windowingStrategyId = this.context.getWindowingStrategyId(
        windowingStrategy!,
      );
    }
    this.proto!.components!.pcollections[pcollId] = {
      uniqueName: pcollId, // TODO: (Named Transforms) name according to producing transform?
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
  comparator: (a: T, b: T) => boolean = (a, b) => false,
) {
  if (valueSet.size === 0) {
    return defaultValue;
  } else if (valueSet.size === 1) {
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

requireForSerialization(`${packageName}/internal/pipeline`, exports);
