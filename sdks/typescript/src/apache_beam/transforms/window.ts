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

import * as runnerApi from "../proto/beam_runner_api";
import * as urns from "../internal/urns";

import { PTransform } from "./transform";
import { Coder } from "../coders/coders";
import { Window } from "../values";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { ParDo } from "./pardo";
import { serializeFn } from "../internal/serialize";

export interface WindowFn<W extends Window> {
  assignWindows: (Instant) => W[];
  windowCoder: () => Coder<W>;
  toProto: () => runnerApi.FunctionSpec;
  isMerging: () => boolean;
  assignsToOneWindow: () => boolean;
}

export class WindowInto<T, W extends Window> extends PTransform<
  PCollection<T>,
  PCollection<T>
> {
  static createWindowingStrategy(
    pipeline: Pipeline,
    windowFn: WindowFn<any>,
    windowingStrategyBase: runnerApi.WindowingStrategy | undefined = undefined
  ): runnerApi.WindowingStrategy {
    let result: runnerApi.WindowingStrategy;
    if (windowingStrategyBase == undefined) {
      result = {
        windowFn: undefined!,
        windowCoderId: undefined!,
        mergeStatus: undefined!,
        assignsToOneWindow: undefined!,
        trigger: { trigger: { oneofKind: "default", default: {} } },
        accumulationMode: runnerApi.AccumulationMode_Enum.DISCARDING,
        outputTime: runnerApi.OutputTime_Enum.END_OF_WINDOW,
        closingBehavior: runnerApi.ClosingBehavior_Enum.EMIT_ALWAYS,
        onTimeBehavior: runnerApi.OnTimeBehavior_Enum.FIRE_ALWAYS,
        allowedLateness: BigInt(0),
        environmentId: pipeline.defaultEnvironment,
      };
    } else {
      result = runnerApi.WindowingStrategy.clone(windowingStrategyBase);
    }
    result.windowFn = windowFn.toProto();
    result.windowCoderId = pipeline.context.getCoderId(windowFn.windowCoder());
    result.mergeStatus = windowFn.isMerging()
      ? runnerApi.MergeStatus_Enum.NEEDS_MERGE
      : runnerApi.MergeStatus_Enum.NON_MERGING;
    result.assignsToOneWindow = windowFn.assignsToOneWindow();
    return result;
  }

  constructor(
    private windowFn: WindowFn<W>,
    private windowingStrategyBase:
      | runnerApi.WindowingStrategy
      | undefined = undefined
  ) {
    super("WindowInto(" + windowFn + ", " + windowingStrategyBase + ")");
  }

  expandInternal(
    input: PCollection<T>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.JS_WINDOW_INTO_DOFN_URN,
            payload: serializeFn({ windowFn: this.windowFn }),
          }),
        })
      ),
    });

    const inputCoder = pipeline.context.getPCollectionCoderId(input);
    return pipeline.createPCollectionInternal<T>(
      inputCoder,
      WindowInto.createWindowingStrategy(
        pipeline,
        this.windowFn,
        this.windowingStrategyBase
      )
    );
  }
}

// TODO: (Cleanup) Add restrictions on moving backwards?
export class AssignTimestamps<T> extends PTransform<
  PCollection<T>,
  PCollection<T>
> {
  constructor(private func: (T, Instant) => typeof Instant) {
    super();
  }

  expandInternal(
    input: PCollection<T>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.JS_ASSIGN_TIMESTAMPS_DOFN_URN,
            payload: serializeFn({ func: this.func }),
          }),
        })
      ),
    });

    return pipeline.createPCollectionInternal<T>(
      pipeline.context.getPCollectionCoderId(input)
    );
  }
}
