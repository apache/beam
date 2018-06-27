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
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.MultiplexingFnDataReceiver;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms;
import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/** Executes different components of Combine PTransforms. */
public class CombineRunners<InputT, OutputT> {

  /** A registrar which provides a factory to handle combine component PTransforms. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_PRECOMBINE),
          new PrecombineFactory(),
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_MERGE_ACCUMULATORS),
          MapFnRunners.forValueMapFnFactory(CombineRunners::createMergeAccumulatorsMapFunction),
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_PER_KEY_EXTRACT_OUTPUTS),
          MapFnRunners.forValueMapFnFactory(CombineRunners::createExtractOutputsMapFunction),
          BeamUrns.getUrn(StandardPTransforms.CombineComponents.COMBINE_GROUPED_VALUES),
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
      GroupingTables.Combiner<WindowedValue<KeyT>, InputT, AccumT, ?> valueCombiner =
          new ValueCombiner<>(
              GlobalCombineFnRunners.create(combineFn), NullSideInputReader.empty(), options);

      groupingTable =
          GroupingTables.combiningAndSampling(
              new WindowingCoderGroupingKeyCreator<>(keyCoder),
              PairInfo.create(),
              valueCombiner,
              new CoderSizeEstimator<>(WindowedValue.getValueOnlyCoder(keyCoder)),
              new CoderSizeEstimator<>(accumCoder),
              0.001 /*sizeEstimatorSampleRate*/);
    }

    void processElement(WindowedValue<KV<KeyT, InputT>> elem) throws Exception {
      groupingTable.put(
          elem,
          (Object outputElem) ->
              output.accept((WindowedValue<KV<KeyT, AccumT>>) outputElem)
      );
    }

    void finishBundle() throws Exception {
      groupingTable.flush(
          (Object outputElem) ->
              output.accept((WindowedValue<KV<KeyT, AccumT>>) outputElem)
      );
    }
  }

  /** A factory for {@link PrecombineRunner}s. */
  private static class PrecombineFactory<KeyT, InputT, AccumT>
      implements PTransformRunnerFactory<PrecombineRunner<KeyT, InputT, AccumT>> {

    @Override
    public PrecombineRunner<KeyT, InputT, AccumT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction,
        BundleSplitListener splitListener)
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
      WindowedValueCoder<KV<KeyT, InputT>> inputCoder =
          (WindowedValueCoder<KV<KeyT, InputT>>)
              rehydratedComponents.getCoder(mainInput.getCoderId());
      Coder<KeyT> keyCoder = ((KvCoder<KeyT, InputT>) inputCoder.getValueCoder()).getKeyCoder();

      CombinePayload combinePayload = CombinePayload.parseFrom(pTransform.getSpec().getPayload());
      CombineFn<InputT, AccumT, ?> combineFn =
          (CombineFn)
              SerializableUtils.deserializeFromByteArray(
                  combinePayload.getCombineFn().getSpec().getPayload().toByteArray(), "CombineFn");
      Coder<AccumT> accumCoder =
          (Coder<AccumT>) rehydratedComponents.getCoder(combinePayload.getAccumulatorCoderId());

      Collection<FnDataReceiver<WindowedValue<KV<KeyT, InputT>>>> consumers =
          (Collection)
              pCollectionIdsToConsumers.get(
                  Iterables.getOnlyElement(pTransform.getOutputsMap().values()));

      // Create the runner.
      PrecombineRunner<KeyT, InputT, AccumT> runner =
          new PrecombineRunner(
              pipelineOptions,
              combineFn,
              MultiplexingFnDataReceiver.forConsumers(consumers),
              keyCoder,
              accumCoder);

      // Register the appropriate handlers.
      addStartFunction.accept(runner::startBundle);
      pCollectionIdsToConsumers.put(
          Iterables.getOnlyElement(pTransform.getInputsMap().values()),
          (FnDataReceiver)
              (FnDataReceiver<WindowedValue<KV<KeyT, InputT>>>) runner::processElement);
      addStartFunction.accept(runner::startBundle);
      addFinishFunction.accept(runner::finishBundle);

      return runner;
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

  /** Implements Precombine GroupingKeyCreator via Coder. */
  public static class WindowingCoderGroupingKeyCreator<K>
      implements GroupingTables.GroupingKeyCreator<WindowedValue<K>> {

    private static final Instant ignored = BoundedWindow.TIMESTAMP_MIN_VALUE;

    private final Coder<K> coder;

    WindowingCoderGroupingKeyCreator(Coder<K> coder) {
      this.coder = coder;
    }

    @Override
    public Object createGroupingKey(WindowedValue<K> key) {
      // Ignore timestamp for grouping purposes.
      // The Precombine output will inherit the timestamp of one of its inputs.
      return WindowedValue.of(
          coder.structuralValue(key.getValue()), ignored, key.getWindows(), key.getPane());
    }
  }

  /** Implements Precombine SizeEstimator via Coder. */
  public static class CoderSizeEstimator<T> implements GroupingTables.SizeEstimator<T> {
    /** Basic implementation of {@link ElementByteSizeObserver} for use in size estimation. */
    private static class Observer extends ElementByteSizeObserver {
      private long observedSize = 0;

      @Override
      protected void reportElementSize(long elementSize) {
        observedSize += elementSize;
      }
    }

    final Coder<T> coder;

    CoderSizeEstimator(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public long estimateSize(T value) throws Exception {
      // First try using byte size observer
      Observer observer = new Observer();
      coder.registerByteSizeObserver(value, observer);

      if (!observer.getIsLazy()) {
        observer.advance();
        return observer.observedSize;
      } else {
        // Coder byte size observation is lazy (requires iteration for observation) so fall back to
        // counting output stream
        CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream());
        coder.encode(value, os);
        return os.getCount();
      }
    }
  }

  /** Implements Precombine PairInfo via KVs. */
  public static class PairInfo implements GroupingTables.PairInfo {
    private static PairInfo theInstance = new PairInfo();

    public static PairInfo create() {
      return theInstance;
    }

    private PairInfo() {}

    @Override
    public Object getKeyFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.withValue(windowedKv.getValue().getKey());
    }

    @Override
    public Object getValueFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.getValue().getValue();
    }

    @Override
    public Object makeOutputPair(Object key, Object values) {
      WindowedValue<?> windowedKey = (WindowedValue<?>) key;
      return windowedKey.withValue(KV.of(windowedKey.getValue(), values));
    }
  }

  /** Implements Precombine Combiner via Combine.KeyedCombineFn. */
  public static class ValueCombiner<K, InputT, AccumT, OutputT>
      implements GroupingTables.Combiner<WindowedValue<K>, InputT, AccumT, OutputT> {
    private final GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFn;
    private final SideInputReader sideInputReader;
    private final PipelineOptions options;

    private ValueCombiner(
        GlobalCombineFnRunner<InputT, AccumT, OutputT> combineFn,
        SideInputReader sideInputReader,
        PipelineOptions options) {
      this.combineFn = combineFn;
      this.sideInputReader = sideInputReader;
      this.options = options;
    }

    @Override
    public AccumT createAccumulator(WindowedValue<K> windowedKey) {
      return this.combineFn.createAccumulator(options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT add(WindowedValue<K> windowedKey, AccumT accumulator, InputT value) {
      return this.combineFn.addInput(
          accumulator, value, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT merge(WindowedValue<K> windowedKey, Iterable<AccumT> accumulators) {
      return this.combineFn.mergeAccumulators(
          accumulators, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public AccumT compact(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.compact(
          accumulator, options, sideInputReader, windowedKey.getWindows());
    }

    @Override
    public OutputT extract(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.extractOutput(
          accumulator, options, sideInputReader, windowedKey.getWindows());
    }
  }
}
