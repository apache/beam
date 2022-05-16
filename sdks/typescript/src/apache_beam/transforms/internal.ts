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

import { PTransform, withName } from "./transform";
import { PCollection, Root } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { Coder } from "../coders/coders";
import { BytesCoder, KVCoder, IterableCoder } from "../coders/required_coders";
import { ParDo } from "./pardo";
import { GeneralObjectCoder } from "../coders/js_coders";
import { KV } from "../values";
import { CombineFn } from "./group_and_combine";

/**
 * `Impulse` is the basic *source* primitive `PTransform`. It receives a Beam
 * Root as input, and returns a `PCollection` of `Uint8Array` with a single
 * element with length=0 (i.e. the empty byte array: `new Uint8Array("")`).
 *
 * `Impulse` is used to start the execution of a pipeline with a single element
 * that can trigger execution of a source or SDF.
 */
export class Impulse extends PTransform<Root, PCollection<Uint8Array>> {
  // static urn: string = runnerApi.StandardPTransforms_Primitives.IMPULSE.urn;
  // TODO: (Cleanup) use above line, not below line.
  static urn: string = "beam:transform:impulse:v1";

  constructor() {
    super("Impulse"); // TODO: (Unique names) pass null/nothing and get from reflection
  }

  expandInternal(
    input: Root,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ): PCollection<Uint8Array> {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: Impulse.urn,
      payload: urns.IMPULSE_BUFFER,
    });
    transformProto.environmentId = "";
    return pipeline.createPCollectionInternal(new BytesCoder());
  }
}

// TODO: (API) Should we offer a method on PCollection to do this?
export class WithCoderInternal<T> extends PTransform<
  PCollection<T>,
  PCollection<T>
> {
  constructor(private coder: Coder<T>) {
    super("WithCoderInternal(" + coder + ")");
  }
  expandInternal(
    input: PCollection<T>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    // IDENTITY rather than Flatten for better fusion.
    transformProto.spec = {
      urn: ParDo.urn,
      payload: runnerApi.ParDoPayload.toBinary(
        runnerApi.ParDoPayload.create({
          doFn: runnerApi.FunctionSpec.create({
            urn: urns.IDENTITY_DOFN_URN,
            payload: undefined!,
          }),
        })
      ),
    };

    return pipeline.createPCollectionInternal<T>(this.coder);
  }
}

/**
 * **Note**: Users should not be using `GroupByKey` transforms directly. Use instead
 * `GroupBy`, and `Combine` transforms.
 *
 * `GroupByKey` is the primitive transform in Beam to force *shuffling* of data,
 * which helps us group data of the same key together. It's a necessary primitive
 * for any Beam SDK.
 *
 * `GroupByKey` operations are used under the hood to execute combines,
 * streaming triggers, stateful transforms, etc.
 */
export class GroupByKey<K, V> extends PTransform<
  PCollection<KV<K, V>>,
  PCollection<KV<K, Iterable<V>>>
> {
  // static urn: string = runnerApi.StandardPTransforms_Primitives.GROUP_BY_KEY.urn;
  // TODO: (Cleanup) use above line, not below line.
  static urn: string = "beam:transform:group_by_key:v1";

  expandInternal(
    input: PCollection<KV<K, V>>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform
  ) {
    const pipelineComponents: runnerApi.Components =
      pipeline.getProto().components!;
    const inputCoderProto =
      pipelineComponents.coders[
        pipelineComponents.pcollections[input.getId()].coderId
      ];

    if (inputCoderProto.spec!.urn != KVCoder.URN) {
      return input
        .apply(
          new WithCoderInternal(
            new KVCoder(new GeneralObjectCoder(), new GeneralObjectCoder())
          )
        )
        .apply(new GroupByKey());
    }

    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: GroupByKey.urn,
      payload: undefined!,
    });
    transformProto.environmentId = "";

    // TODO: (Cleanup) warn about BsonObjectCoder and (non)deterministic key ordering?
    const keyCoder = pipeline.getCoder(inputCoderProto.componentCoderIds[0]);
    const valueCoder = pipeline.getCoder(inputCoderProto.componentCoderIds[1]);
    const iterableValueCoder = new IterableCoder(valueCoder);
    const outputCoder = new KVCoder(keyCoder, iterableValueCoder);
    return pipeline.createPCollectionInternal(outputCoder);
  }
}

/**
 * This transform is used to perform aggregations over groups of elements.
 *
 * It receives a `CombineFn`, which defines functions to create an intermediate
 * aggregator, add elements to it, and transform the aggregator into the expected
 * output.
 *
 * Combines are a valuable transform because they allow for optimizations that
 * can reduce the amount of data being exchanged between workers
 * (a.k.a. "shuffled"). They do this by performing partial aggregations
 * before a `GroupByKey` and after the `GroupByKey`. The partial aggregations
 * help reduce the original data into a single aggregator per key per worker.
 */
export class CombinePerKey<K, InputT, AccT, OutputT> extends PTransform<
  PCollection<KV<K, InputT>>,
  PCollection<KV<K, OutputT>>
> {
  constructor(private combineFn: CombineFn<InputT, AccT, OutputT>) {
    super();
  }

  // Let the runner do the combiner lifting, when possible, handling timestamps,
  // windowing, and triggering as needed.
  expand(input: PCollection<KV<any, InputT>>) {
    const combineFn = this.combineFn;
    return input.apply(new GroupByKey()).map(
      withName("applyCombine", (kv) => ({
        key: kv.key,
        value: combineFn.extractOutput(
          kv.value.reduce(
            combineFn.addInput.bind(combineFn),
            combineFn.createAccumulator()
          )
        ),
      }))
    );
  }
}
