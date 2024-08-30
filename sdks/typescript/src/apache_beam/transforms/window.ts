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

import { PTransform, withName, extractName } from "./transform";
import { Coder } from "../coders/coders";
import { Window } from "../values";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { parDo } from "./pardo";
import { serializeFn } from "../internal/serialize";

export interface WindowFn<W extends Window> {
  assignWindows: (Instant) => W[];
  windowCoder: () => Coder<W>;
  toProto: () => runnerApi.FunctionSpec;
  isMerging: () => boolean;
  assignsToOneWindow: () => boolean;
  beamName?: string;
}

/** @internal */
export function createWindowingStrategyProto(
  pipeline: Pipeline,
  windowFn: WindowFn<any>,
  windowingStrategyBase: runnerApi.WindowingStrategy | undefined = undefined,
): runnerApi.WindowingStrategy {
  let result: runnerApi.WindowingStrategy;
  if (windowingStrategyBase === null || windowingStrategyBase === undefined) {
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

/**
 * returns a PTransform that takes an input PCollection and returns an output
 * PCollection with the same elements but a new windowing according to the
 * provided WindowFn.
 *
 * See https://beam.apache.org/documentation/programming-guide/#windowing
 */
export function windowInto<T, W extends Window>(
  windowFn: WindowFn<W>,
  windowingStrategyBase: runnerApi.WindowingStrategy | undefined = undefined,
): PTransform<PCollection<T>, PCollection<T>> {
  return withName(
    `WindowInto(${extractName(windowFn)}, ${windowingStrategyBase})`,
    (
      input: PCollection<T>,
      pipeline: Pipeline,
      transformProto: runnerApi.PTransform,
    ) => {
      transformProto.spec = runnerApi.FunctionSpec.create({
        urn: parDo.urn,
        payload: runnerApi.ParDoPayload.toBinary(
          runnerApi.ParDoPayload.create({
            doFn: runnerApi.FunctionSpec.create({
              urn: urns.JS_WINDOW_INTO_DOFN_URN,
              payload: serializeFn({ windowFn: windowFn }),
            }),
          }),
        ),
      });

      const inputCoder = pipeline.context.getPCollectionCoderId(input);
      return pipeline.createPCollectionInternal<T>(
        inputCoder,
        createWindowingStrategyProto(pipeline, windowFn, windowingStrategyBase),
      );
    },
  );
}

// TODO: (Cleanup) Add restrictions on moving backwards?
export function assignTimestamps<T>(
  timestampFn: (T, Instant) => typeof Instant,
): PTransform<PCollection<T>, PCollection<T>> {
  return withName(
    `assignTimestamp(${extractName(timestampFn)})`,
    (
      input: PCollection<T>,
      pipeline: Pipeline,
      transformProto: runnerApi.PTransform,
    ) => {
      transformProto.spec = runnerApi.FunctionSpec.create({
        urn: parDo.urn,
        payload: runnerApi.ParDoPayload.toBinary(
          runnerApi.ParDoPayload.create({
            doFn: runnerApi.FunctionSpec.create({
              urn: urns.JS_ASSIGN_TIMESTAMPS_DOFN_URN,
              payload: serializeFn({ func: timestampFn }),
            }),
          }),
        ),
      });

      return pipeline.createPCollectionInternal<T>(
        pipeline.context.getPCollectionCoderId(input),
      );
    },
  );
}
