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

import {
  PTransform,
  PTransformClass,
  withName,
  extractName,
} from "./transform";
import { PCollection, Root } from "../pvalue";
import { Pipeline } from "../internal/pipeline";
import { Coder } from "../coders/coders";
import { BytesCoder, KVCoder, IterableCoder } from "../coders/required_coders";
import { parDo } from "./pardo";
import { GeneralObjectCoder } from "../coders/js_coders";
import { RowCoder } from "../coders/row_coder";
import { KV } from "../values";
import { CombineFn } from "./group_and_combine";
import { serializeFn } from "../internal/serialize";
import { CombinePerKeyPrecombineOperator } from "../worker/operators";

/**
 * `Impulse` is the basic *source* primitive `PTransformClass`. It receives a Beam
 * Root as input, and returns a `PCollection` of `Uint8Array` with a single
 * element with length=0 (i.e. the empty byte array: `new Uint8Array("")`).
 *
 * `Impulse` is used to start the execution of a pipeline with a single element
 * that can trigger execution of a source or SDF.
 */
export function impulse(): PTransform<Root, PCollection<Uint8Array>> {
  function expandInternal(
    input: Root,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ) {
    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: impulse.urn,
      payload: urns.IMPULSE_BUFFER,
    });
    transformProto.environmentId = "";
    return pipeline.createPCollectionInternal(new BytesCoder());
  }

  return withName("impulse", expandInternal);
}

impulse.urn = "beam:transform:impulse:v1";

// TODO: (API) Should we offer a method on PCollection to do this?
export function withCoderInternal<T>(
  coder: Coder<T>,
): PTransform<PCollection<T>, PCollection<T>> {
  return withName(
    `withCoderInternal(${extractName(coder)})`,
    (
      input: PCollection<T>,
      pipeline: Pipeline,
      transformProto: runnerApi.PTransform,
    ) => {
      // IDENTITY rather than Flatten for better fusion.
      transformProto.spec = {
        urn: parDo.urn,
        payload: runnerApi.ParDoPayload.toBinary(
          runnerApi.ParDoPayload.create({
            doFn: runnerApi.FunctionSpec.create({
              urn: urns.IDENTITY_DOFN_URN,
              payload: undefined!,
            }),
          }),
        ),
      };

      return pipeline.createPCollectionInternal<T>(coder);
    },
  );
}

/**
 * Returns a PTransform returning a PCollection with the same contents as the
 * input PCollection, but with the given Row (aka Schema'd) Coder, based on
 * an "example" element. E.g.
 *
 *```js
 * const unknown_schema_pcoll = ...
 * const schema_pcoll = unknown_schema_pcoll.apply(
 *     withRowCoder({int_field: 0, str_field: ""}));
 *```
 *
 * This is particularly useful for declaring the type of the PCollection to
 * invoke cross-language transforms.
 */
export function withRowCoder<T extends Object>(
  exemplar: T,
): PTransform<PCollection<T>, PCollection<T>> {
  return withCoderInternal(RowCoder.fromJSON(exemplar));
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
export function groupByKey<K, V>(): PTransform<
  PCollection<KV<K, V>>,
  PCollection<KV<K, Iterable<V>>>
> {
  function expandInternal(
    input: PCollection<KV<K, V>>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ) {
    const pipelineComponents: runnerApi.Components =
      pipeline.getProto().components!;
    const inputCoderProto =
      pipelineComponents.coders[
        pipelineComponents.pcollections[input.getId()].coderId
      ];

    if (inputCoderProto.spec!.urn !== KVCoder.URN) {
      return input
        .apply(
          withCoderInternal(
            new KVCoder(new GeneralObjectCoder(), new GeneralObjectCoder()),
          ),
        )
        .apply(groupByKey());
    }

    transformProto.spec = runnerApi.FunctionSpec.create({
      urn: groupByKey.urn,
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

  return withName("groupByKey", expandInternal);
}

// TODO: (Cleanup) runnerApi.StandardPTransformClasss_Primitives.GROUP_BY_KEY.urn.
groupByKey.urn = "beam:transform:group_by_key:v1";

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
export function combinePerKey<K, InputT, AccT, OutputT>(
  combineFn: CombineFn<InputT, AccT, OutputT>,
): PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {
  function expandInternal(
    input: PCollection<KV<any, InputT>>,
    pipeline: Pipeline,
    transformProto: runnerApi.PTransform,
  ) {
    const pipelineComponents: runnerApi.Components =
      pipeline.getProto().components!;
    const inputProto = pipelineComponents.pcollections[input.getId()];

    try {
      // If this fails, we cannot lift, so we skip setting the liftable URN.
      CombinePerKeyPrecombineOperator.checkSupportsWindowing(
        pipelineComponents.windowingStrategies[inputProto.windowingStrategyId],
      );

      // Ensure the input is using the KV coder.
      const inputCoderProto = pipelineComponents.coders[inputProto.coderId];
      if (inputCoderProto.spec!.urn !== KVCoder.URN) {
        return input
          .apply(
            withCoderInternal(
              new KVCoder(new GeneralObjectCoder(), new GeneralObjectCoder()),
            ),
          )
          .apply(combinePerKey(combineFn));
      }

      const inputValueCoder = pipeline.context.getCoder<InputT>(
        inputCoderProto.componentCoderIds[1],
      );

      transformProto.spec = runnerApi.FunctionSpec.create({
        urn: combinePerKey.urn,
        payload: runnerApi.CombinePayload.toBinary({
          combineFn: {
            urn: urns.SERIALIZED_JS_COMBINEFN_INFO,
            payload: serializeFn({ combineFn }),
          },
          accumulatorCoderId: pipeline.context.getCoderId(
            combineFn.accumulatorCoder
              ? combineFn.accumulatorCoder(inputValueCoder)
              : new GeneralObjectCoder(),
          ),
        }),
      });
    } catch (err) {
      // Execute this as an unlifted combine.
    }

    return input //
      .apply(groupByKey())
      .map(
        withName("applyCombine", (kv) => {
          // Artificially use multiple accumulators to emulate what would
          // happen in a distributed combine for better testing.
          const accumulators = [
            combineFn.createAccumulator(),
            combineFn.createAccumulator(),
            combineFn.createAccumulator(),
          ];
          let ix = 0;
          for (const value of kv.value) {
            accumulators[ix % 3] = combineFn.addInput(
              accumulators[ix % 3],
              value,
            );
          }
          return {
            key: kv.key,
            value: combineFn.extractOutput(
              combineFn.mergeAccumulators(accumulators),
            ),
          };
        }),
      );
  }

  return withName(`combinePerKey(${extractName(combineFn)})`, expandInternal);
}

combinePerKey.urn = "beam:transform:combine_per_key:v1";
