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
import { PTransform, extractName } from "./transform";
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
// (For PTransforms, it's a major usability issue, but maybe we can always
// await when calling user code.  OTOH, I don't know what the performance
// impact would be for creating promises for every element of every operation
// which is typically a very performance critical spot to optimize.)

export class ParDo<InputT, OutputT, ContextT = undefined> extends PTransform<
  PCollection<InputT>,
  PCollection<OutputT>
> {
  private doFn: DoFn<InputT, OutputT, ContextT>;
  private context: ContextT;
  // static urn: string = runnerApi.StandardPTransforms_Primitives.PAR_DO.urn;
  // TODO: (Cleanup) use above line, not below line.
  static urn: string = "beam:transform:pardo:v1";
  // TODO: (Typescript) Can the arg be optional iff ContextT is undefined?
  constructor(
    doFn: DoFn<InputT, OutputT, ContextT>,
    contextx: ContextT = undefined!
  ) {
    super(() => "ParDo(" + extractName(doFn) + ")");
    this.doFn = doFn;
    this.context = contextx;
  }

  expandInternal(
    input: PCollection<InputT>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    // Extract and populate side inputs from the context.
    var context;
    const sideInputs = {};
    if (typeof this.context == "object") {
      context = Object.create(this.context as Object);
      const components = pipeline.context.components;
      for (const [name, value] of Object.entries(this.context)) {
        if (value instanceof SideInputParam) {
          const inputName = "side." + name;
          transformProto.inputs[inputName] = value.pcoll.getId();
          context[name] = copySideInputWithId(value, inputName);
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
                : mainWindowingStrategyId == sideWindowingStrategyId
                ? urns.IDENTITY_WINDOW_MAPPING_FN_URN
                : urns.ASSIGN_MAX_TIMESTAMP_WINDOW_MAPPING_FN_URN,
              value: new Uint8Array(),
            },
          };
        } else {
          context[name] = value;
        }
      }
    } else {
      context = this.context;
    }

    // Now finally construct the proto.
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SERIALIZED_JS_DOFN_INFO,
            payload: serializeFn({
              doFn: this.doFn,
              context: context,
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
}

// TODO: (API) Consider as top-level method.
// TODO: Naming.
// TODO: Allow default?  Technically splitter can be implemented/wrapped to produce such.
// TODO: Can we enforce splitter's output with the typing system to lie in targets?
// TODO: (Optionally?) delete the switched-on field.
// TODO: (API) Consider doing
//     [{a: aValue}, {g: bValue}, ...] => a: [aValue, ...], b: [bValue, ...]
// instead of
//     [{key: 'a', aValueFields}, {key: 'b', bValueFields}, ...] =>
//          a: [{key: 'a', aValueFields}, ...], b: [{key: 'b', aValueFields}, ...],
// (implemented below as Split2).
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
    input: PCollection<T>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.SPLITTING_JS_DOFN_URN,
            payload: serializeFn({ splitter: this.splitter }),
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

// TODO: (Typescript) Is it possible to type that this takes
// PCollection<{a: T, b: U, ...}> to {a: PCollection<T>, b: PCollection<U>, ...}
// Seems to requires a cast inside expandInternal. But at least the cast is contained there.
export class Split2<T extends { [key: string]: unknown }> extends PTransform<
  PCollection<T>,
  { [P in keyof T]: PCollection<T[P]> }
> {
  private tags: string[];
  constructor(...tags: string[]) {
    super("Split2(" + tags + ")");
    this.tags = tags;
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
            urn: urns.SPLITTING2_JS_DOFN_URN,
            payload: new Uint8Array(),
          }),
        })
      ),
    });

    const this_ = this;
    return Object.fromEntries(
      this_.tags.map((tag) => [
        tag,
        pipeline.createPCollectionInternal<T[typeof tag]>(
          pipeline.context.getPCollectionCoderId(input)
        ),
      ])
    ) as { [P in keyof T]: PCollection<T[P]> };
  }
}

/*
 * This is the root class of special parameters that can be provided in the
 * context of a map or DoFn.process method.  At runtime, one can invoke the
 * special `lookup` method to retrieve the relevant value associated with the
 * currently-being-processed element.
 */
export abstract class ParDoParam<T> {
  readonly parDoParamName: string;

  // Provided externally.
  private provider: ParamProvider | undefined;

  constructor(parDoParamName: string) {
    this.parDoParamName = parDoParamName;
  }

  // TODO: Nameing "get" seems to be special.
  lookup(): T {
    if (this.provider == undefined) {
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

export class WindowParam extends ParDoParam<Window> {
  constructor() {
    super("window");
  }
}

export class TimestampParam extends ParDoParam<Instant> {
  constructor() {
    super("timestamp");
  }
}

// TODO: Naming. Should this be PaneParam?
export class PaneInfoParam extends ParDoParam<PaneInfo> {
  constructor() {
    super("paneinfo");
  }
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

export class IterableSideInput<T> extends SideInputParam<
  T,
  Iterable<T>,
  Iterable<T>
> {
  constructor(pcoll: PCollection<T>) {
    super(pcoll, {
      accessPattern: "beam:side_input:iterable:v1",
      toValue: (iter: Iterable<T>) => iter,
    });
  }
}

export class SingletonSideInput<T> extends SideInputParam<T, Iterable<T>, T> {
  constructor(pcoll: PCollection<T>, defaultValue: T | undefined = undefined) {
    super(pcoll, {
      accessPattern: "beam:side_input:iterable:v1",
      toValue: (iter: Iterable<T>) => {
        const asArray = Array.from(iter);
        if (asArray.length == 0 && defaultValue != undefined) {
          return defaultValue;
        } else if (asArray.length == 1) {
          return asArray[0];
        } else {
          throw new Error("Expected a single element, got " + asArray.length);
        }
      },
    });
  }
}

// TODO: (Extension) Map side inputs.

// TODO: (Extension) Add providers for state, timers,
// restriction trackers, counters, etc.

import { requireForSerialization } from "../serialization";
requireForSerialization("apache_beam.transforms.pardo", exports);
