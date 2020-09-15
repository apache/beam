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
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.BeamFnTimerClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Executes different components of Combine PTransforms. */
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
    private PipelineOptions options;
    private CombineFn<InputT, AccumT, ?> combineFn;
    private FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> output;
    private Coder<KeyT> keyCoder;
    private GroupingTable<WindowedValue<KeyT>, InputT, AccumT> groupingTable;
    private Coder<AccumT> accumCoder;

    PrecombineRunner(
        PipelineOptions options,
        CombineFn<InputT, AccumT, ?> combineFn,
        FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> output,
        Coder<KeyT> keyCoder,
        Coder<AccumT> accumCoder) {
      this.options = options;
      this.combineFn = combineFn;
      this.output = output;
      this.keyCoder = keyCoder;
      this.accumCoder = accumCoder;
    }

    void startBundle() {
      groupingTable =
          PrecombineGroupingTable.combiningAndSampling(
              options, combineFn, keyCoder, accumCoder, 0.001 /*sizeEstimatorSampleRate*/);
    }

    void processElement(WindowedValue<KV<KeyT, InputT>> elem) throws Exception {
      groupingTable.put(
          elem, (Object outputElem) -> output.accept((WindowedValue<KV<KeyT, AccumT>>) outputElem));
    }

    void finishBundle() throws Exception {
      groupingTable.flush(
          (Object outputElem) -> output.accept((WindowedValue<KV<KeyT, AccumT>>) outputElem));
    }
  }

  /** A factory for {@link PrecombineRunner}s. */
  @VisibleForTesting
  public static class PrecombineFactory<KeyT, InputT, AccumT>
      implements PTransformRunnerFactory<PrecombineRunner<KeyT, InputT, AccumT>> {

    @Override
    public PrecombineRunner<KeyT, InputT, AccumT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        BeamFnTimerClient beamFnTimerClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        Consumer<ThrowingRunnable> addResetFunction,
        Consumer<ThrowingRunnable> tearDownFunctions,
        Consumer<ProgressRequestCallback> addProgressRequestCallback,
        BundleSplitListener splitListener,
        BundleFinalizer bundleFinalizer)
        throws IOException {
      // Get objects needed to create the runner.
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(
              RunnerApi.Components.newBuilder()
                  .putAllCoders(coders)
                  .putAllWindowingStrategies(windowingStrategies)
                  .build());
      String mainInputTag = Iterables.getOnlyElement(pTransform.getInputsMap().keySet());
      RunnerApi.PCollection mainInput = pCollections.get(pTransform.getInputsOrThrow(mainInputTag));

      // Input coder may sometimes be WindowedValueCoder depending on runner, instead of the
      // expected KvCoder.
      Coder<?> uncastInputCoder = rehydratedComponents.getCoder(mainInput.getCoderId());
      KvCoder<KeyT, InputT> inputCoder;
      if (uncastInputCoder instanceof WindowedValueCoder) {
        inputCoder =
            (KvCoder<KeyT, InputT>)
                ((WindowedValueCoder<KV<KeyT, InputT>>) uncastInputCoder).getValueCoder();
      } else {
        inputCoder = (KvCoder<KeyT, InputT>) rehydratedComponents.getCoder(mainInput.getCoderId());
      }
      Coder<KeyT> keyCoder = inputCoder.getKeyCoder();

      CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
      CombineFn<InputT, AccumT, ?> combineFn =
          (CombineFn)
              SerializableUtils.deserializeFromByteArray(
                  combinePayload.getCombineFn().getPayload().toByteArray(), "CombineFn");
      Coder<AccumT> accumCoder =
          (Coder<AccumT>) rehydratedComponents.getCoder(combinePayload.getAccumulatorCoderId());

      FnDataReceiver<WindowedValue<KV<KeyT, AccumT>>> consumer =
          (FnDataReceiver)
              pCollectionConsumerRegistry.getMultiplexingConsumer(
                  Iterables.getOnlyElement(pTransform.getOutputsMap().values()));

      PrecombineRunner<KeyT, InputT, AccumT> runner =
          new PrecombineRunner<>(pipelineOptions, combineFn, consumer, keyCoder, accumCoder);

      // Register the appropriate handlers.
      startFunctionRegistry.register(pTransformId, runner::startBundle);
      pCollectionConsumerRegistry.register(
          Iterables.getOnlyElement(pTransform.getInputsMap().values()),
          pTransformId,
          (FnDataReceiver)
              (FnDataReceiver<WindowedValue<KV<KeyT, InputT>>>) runner::processElement);
      finishFunctionRegistry.register(pTransformId, runner::finishBundle);

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
