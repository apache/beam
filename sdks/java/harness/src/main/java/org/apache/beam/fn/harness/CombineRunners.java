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
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** Executes different components of Combine PTransforms. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness",
  "keyfor"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class CombineRunners {

  /** A registrar which provides a factory to handle combine component PTransforms. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          PTransformTranslation.COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
          new PrecombineFactory(),
          PTransformTranslation.COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
          MapFnRunners.forValueMapFnFactory(CombineRunners::createMergeAccumulatorsMapFunction),
          PTransformTranslation.COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
          MapFnRunners.forValueMapFnFactory(CombineRunners::createExtractOutputsMapFunction),
          PTransformTranslation.COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS_TRANSFORM_URN,
          MapFnRunners.forValueMapFnFactory(CombineRunners::createConvertToAccumulatorsMapFunction),
          PTransformTranslation.COMBINE_GROUPED_VALUES_TRANSFORM_URN,
          MapFnRunners.forValueMapFnFactory(CombineRunners::createCombineGroupedValuesMapFunction));
    }
  }

  private static class PrecombineRunner<KeyT, InputT, AccumT> {
    private final PipelineOptions options;
    private final String ptransformId;
    private final Supplier<Cache<?, ?>> bundleCache;
    private final CombineFn<InputT, AccumT, ?> combineFn;
    private final FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> output;
    private final Coder<KeyT> keyCoder;
    private PrecombineGroupingTable<KeyT, InputT, AccumT> groupingTable;
    private boolean isGloballyWindowed;

    PrecombineRunner(
        PipelineOptions options,
        String ptransformId,
        Supplier<Cache<?, ?>> bundleCache,
        CombineFn<InputT, AccumT, ?> combineFn,
        FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> output,
        Coder<KeyT> keyCoder) {
      this(options, ptransformId, bundleCache, combineFn, output, keyCoder, false);
    }

    PrecombineRunner(
        PipelineOptions options,
        String ptransformId,
        Supplier<Cache<?, ?>> bundleCache,
        CombineFn<InputT, AccumT, ?> combineFn,
        FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> output,
        Coder<KeyT> keyCoder,
        boolean isGloballyWindowed) {
      this.options = options;
      this.ptransformId = ptransformId;
      this.bundleCache = bundleCache;
      this.combineFn = combineFn;
      this.output = output;
      this.keyCoder = keyCoder;
      this.isGloballyWindowed = isGloballyWindowed;
    }

    void startBundle() {
      groupingTable =
          PrecombineGroupingTable.combiningAndSampling(
              options,
              Caches.subCache(bundleCache.get(), ptransformId),
              combineFn,
              keyCoder,
              0.001 /*sizeEstimatorSampleRate*/,
              isGloballyWindowed);
    }

    void processElement(WindowedValue<KV<KeyT, InputT>> elem) throws Exception {
      groupingTable.put(elem, output::accept);
    }

    void finishBundle() throws Exception {
      groupingTable.flush(output::accept);
      groupingTable = null;
    }
  }

  /** A factory for {@link PrecombineRunner}s. */
  @VisibleForTesting
  public static class PrecombineFactory<KeyT, InputT, AccumT>
      implements PTransformRunnerFactory<PrecombineRunner<KeyT, InputT, AccumT>> {

    @Override
    public PrecombineRunner<KeyT, InputT, AccumT> createRunnerForPTransform(Context context)
        throws IOException {
      // Get objects needed to create the runner.
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(
              RunnerApi.Components.newBuilder()
                  .putAllCoders(context.getCoders())
                  .putAllWindowingStrategies(context.getWindowingStrategies())
                  .build());
      String mainInputTag =
          Iterables.getOnlyElement(context.getPTransform().getInputsMap().keySet());
      RunnerApi.PCollection mainInput =
          context.getPCollections().get(context.getPTransform().getInputsOrThrow(mainInputTag));

      // Input coder may sometimes be WindowedValueCoder depending on runner, instead of the
      // expected KvCoder.
      Coder<?> uncastInputCoder = rehydratedComponents.getCoder(mainInput.getCoderId());
      KvCoder<KeyT, InputT> inputCoder;
      boolean isGloballyWindowed =
          rehydratedComponents
              .getWindowingStrategy(mainInput.getWindowingStrategyId())
              .getWindowFn()
              .equals(new GlobalWindows());
      if (uncastInputCoder instanceof WindowedValueCoder) {
        inputCoder =
            (KvCoder<KeyT, InputT>)
                ((WindowedValueCoder<KV<KeyT, InputT>>) uncastInputCoder).getValueCoder();
      } else {
        inputCoder = (KvCoder<KeyT, InputT>) rehydratedComponents.getCoder(mainInput.getCoderId());
      }
      Coder<KeyT> keyCoder = inputCoder.getKeyCoder();

      CombinePayload combinePayload =
          CombinePayload.parseFrom(context.getPTransform().getSpec().getPayload());
      CombineFn<InputT, AccumT, ?> combineFn =
          (CombineFn)
              SerializableUtils.deserializeFromByteArray(
                  combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");

      FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> consumer =
          (FnDataReceiver)
              context.getPCollectionConsumer(
                  Iterables.getOnlyElement(context.getPTransform().getOutputsMap().values()));

      PrecombineRunner<KeyT, InputT, AccumT> runner =
          new PrecombineRunner<>(
              context.getPipelineOptions(),
              context.getPTransformId(),
              context.getBundleCacheSupplier(),
              combineFn,
              consumer,
              keyCoder,
              isGloballyWindowed);

      // Register the appropriate handlers.
      context.addStartBundleFunction(runner::startBundle);
      context.addPCollectionConsumer(
          Iterables.getOnlyElement(context.getPTransform().getInputsMap().values()),
          (FnDataReceiver)
              (FnDataReceiver<WindowedValue<KV<KeyT, InputT>>>) runner::processElement);
      context.addFinishBundleFunction(runner::finishBundle);

      return runner;
    }
  }

  static <KeyT, AccumT>
      ThrowingFunction<KV<KeyT, Iterable<AccumT>>, KV<KeyT, AccumT>>
          createMergeAccumulatorsMapFunction(String pTransformId, PTransform pTransform)
              throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<?, AccumT, ?> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");

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
                combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, AccumT> input) ->
        KV.of(input.getKey(), combineFn.extractOutput(input.getValue()));
  }

  static <KeyT, InputT, AccumT>
      ThrowingFunction<KV<KeyT, InputT>, KV<KeyT, AccumT>> createConvertToAccumulatorsMapFunction(
          String pTransformId, PTransform pTransform) throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<InputT, AccumT, ?> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, InputT> input) ->
        KV.of(input.getKey(), combineFn.addInput(combineFn.createAccumulator(), input.getValue()));
  }

  static <KeyT, InputT, AccumT, OutputT>
      ThrowingFunction<KV<KeyT, Iterable<InputT>>, KV<KeyT, OutputT>>
          createCombineGroupedValuesMapFunction(String pTransformId, PTransform pTransform)
              throws IOException {
    CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
    CombineFn<InputT, AccumT, OutputT> combineFn =
        (CombineFn)
            SerializableUtils.deserializeFromByteArray(
                combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");

    return (KV<KeyT, Iterable<InputT>> input) -> {
      return KV.of(input.getKey(), combineFn.apply(input.getValue()));
    };
  }
}
