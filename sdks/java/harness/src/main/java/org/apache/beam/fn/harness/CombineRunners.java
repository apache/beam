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
package org.apache.beam.fn.harness;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;

/**
 * Executes different components of Combine PTransforms.
 */
public class CombineRunners<InputT, OutputT> {

  /**
   * A registrar which provides a factory to handle combine component PTransforms.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_PRECOMBINE),
          MapFnRunners.forValueMapFnFactory(CombineRunners::createPrecombineMapFunction),
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_MERGE_ACCUMULATORS),
          MapFnRunners.forValueMapFnFactory(CombineRunners::createMergeAccumulatorsMapFunction),
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_EXTRACT_OUTPUTS),
          MapFnRunners.forValueMapFnFactory(CombineRunners::createExtractOutputsMapFunction),
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_GROUPED_VALUES),
          MapFnRunners.forValueMapFnFactory(CombineRunners::createCombineGroupedValuesMapFunction));
    }
  }

  static <KeyT, InputT, AccumT>
  ThrowingFunction<KV<KeyT, InputT>, KV<KeyT, AccumT>> createPrecombineMapFunction(
      String pTransformId, PTransform pTransform) throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<InputT, AccumT, ?> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getSpec().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, InputT> input) ->
        KV.of(input.getKey(), combineFn.addInput(combineFn.createAccumulator(), input.getValue()));
  }

  static <KeyT, AccumT>
  ThrowingFunction<KV<KeyT, Iterable<AccumT>>, KV<KeyT, AccumT>>
  createMergeAccumulatorsMapFunction(String pTransformId, PTransform pTransform)
      throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<?, AccumT, ?> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getSpec().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, Iterable<AccumT>> input) ->
        KV.of(input.getKey(), combineFn.mergeAccumulators(input.getValue()));
  }

  static <KeyT, AccumT, OutputT>
  ThrowingFunction<KV<KeyT, AccumT>, KV<KeyT, OutputT>> createExtractOutputsMapFunction(
      String pTransformId, PTransform pTransform) throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<?, AccumT, OutputT> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getSpec().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, AccumT> input) ->
        KV.of(input.getKey(), combineFn.extractOutput(input.getValue()));
  }

  static <KeyT, InputT, AccumT, OutputT>
  ThrowingFunction<KV<KeyT, Iterable<InputT>>, KV<KeyT, OutputT>>
  createCombineGroupedValuesMapFunction(String pTransformId, PTransform pTransform)
      throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<InputT, AccumT, OutputT> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getSpec().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, Iterable<InputT>> input) -> {
      AccumT accumulator = combineFn.createAccumulator();
      Iterable<InputT> inputValues = input.getValue();
      for (InputT inputValue : inputValues) {
        accumulator = combineFn.addInput(accumulator, inputValue);
      }
      return KV.of(input.getKey(), combineFn.extractOutput(accumulator));
    };
  }
}
