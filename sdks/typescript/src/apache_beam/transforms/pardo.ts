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

import { GeneralObjectCoder } from "../coders/js_coders";
import { PCollection } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { serializeFn } from "../internal/serialize";
import {
  PTransform,
  PTransformClass,
  withName,
  extractName,
} from "./transform";
import { PaneInfo, Instant, Window, WindowedValue } from "../values";

export interface DoFn<InputT, OutputT, ContextT = undefined> {
  beamName?: string;

  process: (element: InputT, context: ContextT) => Iterable<OutputT> | void;

  startBundle?: (context: ContextT) => void;

  // TODO: (API) Re-consider this API.
  finishBundle?: (context: ContextT) => Iterable<WindowedValue<OutputT>> | void;
}

// TODO: (API) Do we need an AsyncDoFn (and async[Flat]Map) to be able to call
// async functions in the body of the fns. Or can they always be Async?
// The latter seems to have perf issues.
// (For PTransformClasss, it's a major usability issue, but maybe we can always
// await when calling user code.  OTOH, I don't know what the performance
// impact would be for creating promises for every element of every operation
// which is typically a very performance critical spot to optimize.)

// TODO: (Typescript) Can the context arg be optional iff ContextT is undefined?
export function parDo<
  InputT,
  OutputT,
  ContextT extends Object | undefined = undefined
>(
  doFn: DoFn<InputT, OutputT, ContextT>,
  context: ContextT = undefined!
): PTransform<PCollection<InputT>, PCollection<OutputT>> {
  function expandInternal(
    input: PCollection<InputT>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    // Extract and populate side inputs from the context.
    const sideInputs = {};
    var contextCopy;
    if (typeof context === "object") {
      contextCopy = Object.create(context as Object) as any;
      const components = pipeline.context.components;
      for (const [name, value] of Object.entries(context)) {
        if (value instanceof SideInputParam) {
          const inputName = "side." + name;
          transformProto.inputs[inputName] = value.pcoll.getId();
          contextCopy[name] = copySideInputWithId(value, inputName);
          const mainWindowingStrategyId =
            components.pcollections[input.getId()].windowingStrategyId;
          const sideWindowingStrategyId =
            components.pcollections[transformProto.inputs[inputName]]
              .windowingStrategyId;
          const sideWindowingStrategy =
            components.windowingStrategies[sideWindowingStrategyId];
          const isGlobalSide =
            sideWindowingStrategy.windowFn!.urn ==
            "beam:window_fn:global_windows:v1";
          sideInputs[inputName] = {
            accessPattern: {
              urn: value.accessor.accessPattern,
              payload: new Uint8Array(),
            },
            // TODO: (Cleanup) The viewFn is stored in the side input object.
            // Unclear what benefit there is to putting it here.
            viewFn: { urn: "unused", payload: new Uint8Array() },
            // TODO: (Extension) Possibly place this in the accessor.
            windowMappingFn: {
              urn: isGlobalSide
                ? urns.GLOBAL_WINDOW_MAPPING_FN_URN
                : mainWindowingStrategyId === sideWindowingStrategyId
                ? urns.IDENTITY_WINDOW_MAPPING_FN_URN
                : urns.ASSIGN_MAX_TIMESTAMP_WINDOW_MAPPING_FN_URN,
              value: new Uint8Array(),
            },
          };
        } else {
          contextCopy[name] = value;
        }
      }
    } else {
      contextCopy = context;
    }

    // Now finally construct the proto.
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: parDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SERIALIZED_JS_DOFN_INFO,
            payload: serializeFn({
              doFn: doFn,
              context: contextCopy,
            }),
          }),
          sideInputs: sideInputs,
        })
      ),
    });

    // For the ParDo output coder, we use a GeneralObjectCoder, which is a Javascript-specific
    // coder to encode the various types that exist in JS.
    // TODO: (Types) Should there be a way to specify, or better yet infer, the coder to use?
    return pipeline.createPCollectionInternal<OutputT>(
      new GeneralObjectCoder()
    );
  }

  return withName(`parDo(${extractName(doFn)})`, expandInternal);
}

// TODO: (Cleanup) use runnerApi.StandardPTransformClasss_Primitives.PAR_DO.urn.
parDo.urn = "beam:transform:pardo:v1";

export type SplitOptions = {
  knownTags?: string[];
  unknownTagBehavior?: "error" | "ignore" | "rename" | undefined;
  unknownTagName?: string;
  exclusive?: boolean;
};

/**
 * Splits a single PCollection of objects, with keys k, into an object of
 * PCollections, with the same keys k, where each PCollection consists of the
 * values associated with that key. That is,
 *
 * PCollection<{a: T, b: U, ...}> maps to {a: PCollection<T>, b: PCollection<U>, ...}
 */
// TODO: (API) Consider as top-level method.
// TODO: Naming.
export function split<T extends { [key: string]: unknown }>(
  tags: string[],
  options: SplitOptions = {}
): PTransform<PCollection<T>, { [P in keyof T]: PCollection<T[P]> }> {
  function expandInternal(
    input: PCollection<T>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    if (options.exclusive === undefined) {
      options.exclusive = true;
    }
    if (options.unknownTagBehavior === undefined) {
      options.unknownTagBehavior = "error";
    }
    if (
      options.unknownTagBehavior === "rename" &&
      !tags.includes(options.unknownTagName!)
    ) {
      tags.push(options.unknownTagName!);
    }
    if (options.knownTags === undefined) {
      options.knownTags = tags;
    }

    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: parDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SPLITTING_JS_DOFN_URN,
            payload: serializeFn(options),
          }),
        })
      ),
    });

    return Object.fromEntries(
      tags.map((tag) => [
        tag,
        pipeline.createPCollectionInternal<T[typeof tag]>(
          pipeline.context.getPCollectionCoderId(input)
        ),
      ])
    ) as { [P in keyof T]: PCollection<T[P]> };
  }

  return withName(`Split(${tags})`, expandInternal);
}

export function partition<T>(
  partitionFn: (element: T, numPartitions: number) => number,
  numPartitions: number
): PTransform<PCollection<T>, PCollection<T>[]> {
  return function partition(input: PCollection<T>) {
    const indices = Array.from({ length: numPartitions }, (v, i) =>
      i.toString()
    );
    const splits = input
      .map((x) => {
        const part = partitionFn(x, numPartitions);
        return { ["" + part]: x };
      })
      .apply(split(indices));
    return indices.map((ix) => splits[ix]);
  };
}

/*
 * This is the root class of special parameters that can be provided in the
 * context of a map or DoFn.process method.  At runtime, one can invoke the
 * special `lookup` method to retrieve the relevant value associated with the
 * currently-being-processed element.
 */
export class ParDoParam<T> {
  // Provided externally.
  private provider: ParamProvider | undefined;

  constructor(readonly parDoParamName: string) {}

  // TODO: Nameing "get" seems to be special.
  lookup(): T {
    if (this.provider === undefined) {
      throw new Error("Cannot be called outside of a DoFn's process method.");
    }

    return this.provider.provide(this);
  }
}

/**
 * This is the magic class that wires up the ParDoParams to their values
 * at runtime.
 */
export interface ParamProvider {
  provide<T>(param: ParDoParam<T>): T;
}

export function windowParam(): ParDoParam<Window> {
  return new ParDoParam<Window>("window");
}

export function timestampParam(): ParDoParam<Instant> {
  return new ParDoParam<Instant>("timestamp");
}

// TODO: Naming. Should this be PaneParam?
export function paneInfoParam(): ParDoParam<PaneInfo> {
  return new ParDoParam<PaneInfo>("paneinfo");
}

interface SideInputAccessor<PCollT, AccessorT, ValueT> {
  // This should be a value of runnerApi.StandardSideInputTypes, and specifies
  // the relationship between PCollT (the type fo the PCollection's elements)
  // and AccessorT (the type returned when fetching side inputs).
  accessPattern: string;
  // This transforms the runner type into the user's type.
  toValue: (AccessorT) => ValueT;
}

// TODO: (Extension) Support side inputs that are composites of multiple more
// primitive side inputs.
export class SideInputParam<
  PCollT,
  AccessorT,
  ValueT
> extends ParDoParam<ValueT> {
  // Populated by user.
  pcoll: PCollection<PCollT>;
  // Typically populated by subclass.
  accessor: SideInputAccessor<PCollT, AccessorT, ValueT>;

  constructor(
    pcoll: PCollection<PCollT>,
    accessor: SideInputAccessor<PCollT, AccessorT, ValueT>
  ) {
    super("sideInput");
    this.pcoll = pcoll;
    this.accessor = accessor;
  }

  // Internal. Should match the id of the side input in the proto.
  // TODO: (Cleanup) Rename to tag for consistency?
  sideInputId: string;
}

function copySideInputWithId<PCollT, AccessorT, ValueT>(
  sideInput: SideInputParam<PCollT, AccessorT, ValueT>,
  id: string
): SideInputParam<PCollT, AccessorT, ValueT> {
  const copy = Object.create(sideInput);
  copy.sideInputId = id;
  delete copy.pcoll;
  return copy;
}

export function iterableSideInput<T>(
  pcoll: PCollection<T>
): SideInputParam<T, Iterable<T>, Iterable<T>> {
  return new SideInputParam<T, Iterable<T>, Iterable<T>>(pcoll, {
    accessPattern: "beam:side_input:iterable:v1",
    toValue: (iter: Iterable<T>) => iter,
  });
}

export function singletonSideInput<T>(
  pcoll: PCollection<T>,
  defaultValue: T | undefined = undefined
): SideInputParam<T, Iterable<T>, T> {
  return new SideInputParam<T, Iterable<T>, T>(pcoll, {
    accessPattern: "beam:side_input:iterable:v1",
    toValue: (iter: Iterable<T>) => {
      const asArray = Array.from(iter);
      if (
        asArray.length === 0 &&
        defaultValue !== null &&
        defaultValue !== undefined
      ) {
        return defaultValue;
      } else if (asArray.length === 1) {
        return asArray[0];
      } else {
        throw new Error("Expected a single element, got " + asArray.length);
      }
    },
  });
}

// TODO: (Extension) Map side inputs.

// TODO: (Extension) Add providers for state, timers,
// restriction trackers, counters, etc.

import { requireForSerialization } from "../serialization";
requireForSerialization("apache-beam/transforms/pardo", exports);
