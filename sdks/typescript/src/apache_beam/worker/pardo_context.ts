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

import * as protobufjs from "protobufjs";

import * as runnerApi from "../proto/beam_runner_api";
import * as fnApi from "../proto/beam_fn_api";
import { MetricsContainer, MetricSpec } from "./metrics";
import { StateProvider } from "./state";

import * as urns from "../internal/urns";
import { Coder, Context as CoderContext } from "../coders/coders";
import { GlobalWindowCoder } from "../coders/required_coders";
import {
  Window,
  Instant,
  PaneInfo,
  GlobalWindow,
  WindowedValue,
} from "../values";
import {
  DoFn,
  ParDoParam,
  ParamProvider,
  SideInputParam,
} from "../transforms/pardo";
import * as operators from "./operators";

/**
 * @fileoverview This is where we handle the magic of populating the context
 * properties for Maps and ParDos. It is rather javascript-specific, both
 * in the way the contexts are manipulated and in the finagling we have to
 * because there is no blocking way to interact with the runner to look up
 * things like side inputs.
 */

export class ParamProviderImpl implements ParamProvider {
  wvalue: WindowedValue<unknown> | undefined = undefined;
  prefetchCallbacks: ((window: Window) => operators.ProcessResult)[];
  sideInputValues: Map<string, unknown> = new Map();

  constructor(
    private transformId: string,
    private sideInputInfo: Map<string, SideInputInfo>,
    private getStateProvider: () => StateProvider,
    private metricsContainer: MetricsContainer,
  ) {}

  // Avoid modifying the original object, as that could have surprising results
  // if they are widely shared.
  augmentContext(context: any) {
    this.prefetchCallbacks = [];
    if (typeof context !== "object") {
      return context;
    }

    const result = Object.create(context);
    for (const [name, value] of Object.entries(context)) {
      // Is this the best way to check post serialization?
      if (
        typeof value === "object" &&
        value !== null &&
        value["parDoParamName"] !== undefined
      ) {
        result[name] = Object.create(value);
        result[name].provider = this;
        if ((value as ParDoParam).parDoParamName === "sideInput") {
          this.prefetchCallbacks.push(
            this.prefetchSideInput(
              value as SideInputParam<unknown, unknown, unknown>,
            ),
          );
        }
      }
    }
    return result;
  }

  prefetchSideInput(
    param: SideInputParam<unknown, unknown, unknown>,
  ): (window: Window) => operators.ProcessResult {
    const this_ = this;
    const stateProvider = this.getStateProvider();
    const { windowCoder, elementCoder, windowMappingFn } =
      this.sideInputInfo.get(param.sideInputId)!;
    const isGlobal = windowCoder instanceof GlobalWindowCoder;
    const decode = (encodedElements: Uint8Array) => {
      return param.accessor.toValue(
        (function* () {
          const reader = new protobufjs.Reader(encodedElements);
          while (reader.pos < reader.len) {
            yield elementCoder.decode(reader, CoderContext.needsDelimiters);
          }
        })(),
      );
    };
    return (window: Window) => {
      if (isGlobal && this_.sideInputValues.has(param.sideInputId)) {
        return operators.NonPromise;
      }
      const stateKey = createStateKey(
        this_.transformId,
        param.accessor.accessPattern,
        param.sideInputId,
        window,
        windowCoder,
      );
      const lookupResult = stateProvider.getState(stateKey, decode);
      if (lookupResult.type === "value") {
        this_.sideInputValues.set(param.sideInputId, lookupResult.value);
        return operators.NonPromise;
      } else {
        return lookupResult.promise.then((value) => {
          this_.sideInputValues.set(param.sideInputId, value);
        });
      }
    };
  }

  setCurrentValue(
    wvalue: WindowedValue<unknown> | undefined,
  ): operators.ProcessResult {
    this.wvalue = wvalue;
    if (wvalue === null || wvalue === undefined) {
      return operators.NonPromise;
    }
    // We have to prefetch all the side inputs.
    // TODO: (API) Let the user's process() await them.
    if (this.prefetchCallbacks.length === 0) {
      return operators.NonPromise;
    } else {
      const result = new operators.ProcessResultBuilder();
      for (const cb of this.prefetchCallbacks) {
        result.add(cb(wvalue!.windows[0]));
      }
      return result.build();
    }
  }

  lookup(param) {
    if (this.wvalue === null || this.wvalue === undefined) {
      throw new Error(
        param.parDoParamName + " not defined outside of a process() call.",
      );
    }

    switch (param.parDoParamName) {
      case "window":
        // If we're here and there was more than one window, we have exploded.
        return this.wvalue.windows[0];

      case "timestamp":
        return this.wvalue.timestamp;

      case "paneinfo":
        return this.wvalue.pane;

      case "sideInput":
        return this.sideInputValues.get(param.sideInputId) as any;

      default:
        throw new Error("Unknown context parameter: " + param.parDoParamName);
    }
  }

  update(param, value) {
    switch (param.parDoParamName) {
      case "metric":
        this.metricsContainer
          .getMetric(this.transformId, undefined, param as MetricSpec)
          .update(value);
        break;

      default:
        throw new Error("Unknown context parameter: " + param.parDoParamName);
    }
  }
}

export interface SideInputInfo {
  elementCoder: Coder<unknown>;
  windowCoder: Coder<Window>;
  windowMappingFn: (window: Window) => Window;
}

export function createSideInputInfo(
  transformProto: runnerApi.PTransform,
  spec: runnerApi.ParDoPayload,
  operatorContext: operators.OperatorContext,
): Map<string, SideInputInfo> {
  const globalWindow = new GlobalWindow();
  const sideInputInfo: Map<string, SideInputInfo> = new Map();
  for (const [sideInputId, sideInput] of Object.entries(spec.sideInputs)) {
    let windowMappingFn: (window: Window) => Window;
    switch (sideInput.windowMappingFn!.urn) {
      case urns.GLOBAL_WINDOW_MAPPING_FN_URN:
        windowMappingFn = (window) => globalWindow;
        break;
      case urns.IDENTITY_WINDOW_MAPPING_FN_URN:
        windowMappingFn = (window) => window;
        break;
      default:
        throw new Error(
          "Unsupported window mapping fn: " + sideInput.windowMappingFn!.urn,
        );
    }
    const sidePColl =
      operatorContext.descriptor.pcollections[
        transformProto.inputs[sideInputId]
      ];
    const windowingStrategy =
      operatorContext.pipelineContext.getWindowingStrategy(
        sidePColl.windowingStrategyId,
      );
    sideInputInfo.set(sideInputId, {
      elementCoder: operatorContext.pipelineContext.getCoder(sidePColl.coderId),
      windowCoder: operatorContext.pipelineContext.getCoder(
        windowingStrategy.windowCoderId,
      ),
      windowMappingFn: windowMappingFn,
    });
  }
  return sideInputInfo;
}

export function createStateKey(
  transformId: string,
  accessPattern: string,
  sideInputId: string,
  window: Window,
  windowCoder: Coder<Window>,
): fnApi.StateKey {
  const writer = new protobufjs.Writer();
  windowCoder.encode(window, writer, CoderContext.needsDelimiters);
  const encodedWindow = writer.finish();

  switch (accessPattern) {
    case "beam:side_input:iterable:v1":
      return {
        type: {
          oneofKind: "iterableSideInput",
          iterableSideInput: {
            transformId: transformId,
            sideInputId: sideInputId,
            window: encodedWindow,
          },
        },
      };

    default:
      throw new Error("Unimplemented access pattern: " + accessPattern);
  }
}
