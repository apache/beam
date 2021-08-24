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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.runners.dataflow.BatchViewOverrides.GroupByKeyAndSortValuesOnly;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@link PTransformOverrideFactory PTransformOverrideFactories} that expands to correctly implement
 * stateful {@link ParDo} using window-unaware {@link GroupByKeyAndSortValuesOnly} to linearize
 * processing per key.
 *
 * <p>For the Fn API, the {@link PTransformOverrideFactory} is only required to perform per key
 * grouping and expansion.
 *
 * <p>This implementation relies on implementation details of the Dataflow runner, specifically
 * standard fusion behavior of {@link ParDo} tranforms following a {@link GroupByKey}.
 */
public class BatchStatefulParDoOverrides {

  /**
   * Returns a {@link PTransformOverrideFactory} that replaces a single-output {@link ParDo} with a
   * composite transform specialized for the {@link DataflowRunner}.
   */
  public static <K, InputT, OutputT>
      PTransformOverrideFactory<
              PCollection<KV<K, InputT>>,
              PCollection<OutputT>,
              ParDo.SingleOutput<KV<K, InputT>, OutputT>>
          singleOutputOverrideFactory() {
    return new SingleOutputOverrideFactory<>();
  }

  /**
   * Returns a {@link PTransformOverrideFactory} that replaces a multi-output {@link ParDo} with a
   * composite transform specialized for the {@link DataflowRunner}.
   */
  public static <K, InputT, OutputT>
      PTransformOverrideFactory<
              PCollection<KV<K, InputT>>,
              PCollectionTuple,
              ParDo.MultiOutput<KV<K, InputT>, OutputT>>
          multiOutputOverrideFactory(DataflowPipelineOptions options) {
    return new MultiOutputOverrideFactory<>();
  }

  private static class SingleOutputOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, InputT>>,
          PCollection<OutputT>,
          ParDo.SingleOutput<KV<K, InputT>, OutputT>> {

    private SingleOutputOverrideFactory() {}

    @Override
    public PTransformReplacement<PCollection<KV<K, InputT>>, PCollection<OutputT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, InputT>>,
                    PCollection<OutputT>,
                    ParDo.SingleOutput<KV<K, InputT>, OutputT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StatefulSingleOutputParDo<>(transform.getTransform()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<OutputT> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class MultiOutputOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, InputT>>, PCollectionTuple, ParDo.MultiOutput<KV<K, InputT>, OutputT>> {

    private MultiOutputOverrideFactory() {}

    @Override
    public PTransformReplacement<PCollection<KV<K, InputT>>, PCollectionTuple>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, InputT>>,
                    PCollectionTuple,
                    ParDo.MultiOutput<KV<K, InputT>, OutputT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StatefulMultiOutputParDo<>(transform.getTransform()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  static class StatefulSingleOutputParDo<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private final ParDo.SingleOutput<InputT, OutputT> originalParDo;

    StatefulSingleOutputParDo(ParDo.SingleOutput<InputT, OutputT> originalParDo) {
      this.originalParDo = originalParDo;
    }

    ParDo.SingleOutput<InputT, OutputT> getOriginalParDo() {
      return originalParDo;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      DoFn fn = originalParDo.getFn();
      verifyFnIsStateful(fn);
      DataflowPipelineOptions options =
          input.getPipeline().getOptions().as(DataflowPipelineOptions.class);
      DataflowRunner.verifyDoFnSupported(fn, false, DataflowRunner.useStreamingEngine(options));
      DataflowRunner.verifyStateSupportForWindowingStrategy(input.getWindowingStrategy());

      PCollection keyedInput = input;
      // ParDo does this in ParDo.MultiOutput.expand. However since we're replacing
      // ParDo.SingleOutput, the results
      // of the initial expansion of ParDo.MultiOutput are thrown away, so we need to add the key
      // back in.
      DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
      @Nullable FieldAccessDescriptor keyFieldAccess = ParDo.getDoFnFieldsDescriptor(signature, fn);
      if (keyFieldAccess != null) {
        if (!input.hasSchema()) {
          throw new IllegalArgumentException(
              "Cannot specify a @StateKeyFields if not using a schema");
        }
        keyedInput = input.apply("Extract schema keys", ParDo.getWithSchemaKeys(keyFieldAccess));
      }
      return continueExpand(keyedInput, fn);
    }

    @SuppressWarnings({"rawtypes"})
    public <K, V> PCollection<OutputT> continueExpand(PCollection<KV<K, V>> input, DoFn fn) {
      ParDo.SingleOutput<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>, OutputT>
          statefulParDo =
              ParDo.of(new BatchStatefulDoFn<K, V, OutputT>(fn))
                  .withSideInputs(originalParDo.getSideInputs());

      return input.apply(new GbkBeforeStatefulParDo<>()).apply(statefulParDo);
    }
  }

  static class StatefulMultiOutputParDo<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionTuple> {

    private final ParDo.MultiOutput<InputT, OutputT> originalParDo;

    StatefulMultiOutputParDo(ParDo.MultiOutput<InputT, OutputT> originalParDo) {
      this.originalParDo = originalParDo;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public PCollectionTuple expand(PCollection<InputT> input) {
      DoFn fn = originalParDo.getFn();
      verifyFnIsStateful(fn);
      DataflowPipelineOptions options =
          input.getPipeline().getOptions().as(DataflowPipelineOptions.class);
      DataflowRunner.verifyDoFnSupported(fn, false, DataflowRunner.useStreamingEngine(options));
      DataflowRunner.verifyStateSupportForWindowingStrategy(input.getWindowingStrategy());

      PCollection keyedInput = input;
      // ParDo does this in ParDo.MultiOutput.expand. However since we're replacing
      // ParDo.SingleOutput, the results
      // of the initial expansion of ParDo.MultiOutput are thrown away, so we need to add the key
      // back in.
      DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
      @Nullable FieldAccessDescriptor keyFieldAccess = ParDo.getDoFnFieldsDescriptor(signature, fn);
      if (keyFieldAccess != null) {
        if (!input.hasSchema()) {
          throw new IllegalArgumentException(
              "Cannot specify a @StateKeyFields if not using a schema");
        }
        keyedInput = input.apply("Extract schema keys", ParDo.getWithSchemaKeys(keyFieldAccess));
      }

      DoFnSchemaInformation schemaInformation =
          Preconditions.checkNotNull(originalParDo.getDoFnSchemaInformation());
      return continueExpand(keyedInput, fn, schemaInformation);
    }

    @SuppressWarnings({"rawtypes"})
    public <K, V> PCollectionTuple continueExpand(
        PCollection<KV<K, V>> input, DoFn fn, DoFnSchemaInformation doFnSchemaInformation) {
      ParDo.MultiOutput<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>, OutputT>
          statefulParDo =
              ParDo.of(new BatchStatefulDoFn<K, V, OutputT>(fn))
                  .withSideInputs(originalParDo.getSideInputs())
                  .withOutputTags(
                      originalParDo.getMainOutputTag(), originalParDo.getAdditionalOutputTags())
                  .withDoFnSchemaInformation(doFnSchemaInformation);

      return input.apply(new GbkBeforeStatefulParDo<>()).apply(statefulParDo);
    }

    public ParDo.MultiOutput<InputT, OutputT> getOriginalParDo() {
      return originalParDo;
    }
  }

  static class GbkBeforeStatefulParDo<K, V>
      extends PTransform<
          PCollection<KV<K, V>>,
          PCollection<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>>> {

    @Override
    public PCollection<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>> expand(
        PCollection<KV<K, V>> input) {

      WindowingStrategy<?, ?> inputWindowingStrategy = input.getWindowingStrategy();

      // A KvCoder is required since this goes through GBK. Further, WindowedValueCoder
      // is not registered by default, so we explicitly set the relevant coders.
      checkState(
          input.getCoder() instanceof KvCoder,
          "Input to a %s using state requires a %s, but the coder was %s. PColleciton %s",
          ParDo.class.getSimpleName(),
          KvCoder.class.getSimpleName(),
          input.getCoder(),
          input.getName());
      KvCoder<K, V> kvCoder = (KvCoder<K, V>) input.getCoder();
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends BoundedWindow> windowCoder =
          inputWindowingStrategy.getWindowFn().windowCoder();

      return input
          // Stash the original timestamps, etc, for when it is fed to the user's DoFn
          .apply("ReifyWindows", ParDo.of(new ReifyWindowedValueFn<>()))
          .setCoder(
              KvCoder.of(
                  keyCoder,
                  KvCoder.of(InstantCoder.of(), WindowedValue.getFullCoder(kvCoder, windowCoder))))

          // Group by key and sort by timestamp, dropping windows as they are reified
          .apply("PartitionKeys", new GroupByKeyAndSortValuesOnly<>())

          // The GBKO sets the windowing strategy to the global default
          .setWindowingStrategyInternal(inputWindowingStrategy);
    }
  }

  /** A key-preserving {@link DoFn} that reifies a windowed value. */
  static class ReifyWindowedValueFn<K, V>
      extends DoFn<KV<K, V>, KV<K, KV<Instant, WindowedValue<KV<K, V>>>>> {
    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      c.output(
          KV.of(
              c.element().getKey(),
              KV.of(
                  c.timestamp(), WindowedValue.of(c.element(), c.timestamp(), window, c.pane()))));
    }
  }

  /**
   * A key-preserving {@link DoFn} that explodes an iterable that has been grouped by key and
   * window.
   */
  public static class BatchStatefulDoFn<K, InputT, OutputT>
      extends DoFn<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, InputT>>>>>, OutputT> {

    private final DoFn<?, OutputT> underlyingDoFn;

    BatchStatefulDoFn(DoFn<?, OutputT> underlyingDoFn) {
      this.underlyingDoFn = underlyingDoFn;
    }

    public DoFn<?, OutputT> getUnderlyingDoFn() {
      return underlyingDoFn;
    }

    @Setup
    public void setup(final PipelineOptions options) {
      DoFnInvokers.tryInvokeSetupFor(underlyingDoFn, options);
    }

    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      throw new UnsupportedOperationException(
          "BatchStatefulDoFn.ProcessElement should never be invoked");
    }

    @Teardown
    public void teardown() {
      DoFnInvokers.invokerFor(underlyingDoFn).invokeTeardown();
    }

    @Override
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return underlyingDoFn.getOutputTypeDescriptor();
    }
  }

  private static <InputT, OutputT> void verifyFnIsStateful(DoFn<InputT, OutputT> fn) {
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());

    // It is still correct to use this without state or timers, but a bad idea.
    // Since it is internal it should never be used wrong, so it is OK to crash.
    checkState(
        signature.usesState() || signature.usesTimers() || signature.onWindowExpiration() != null,
        "%s used for %s that does not use state or timers.",
        BatchStatefulParDoOverrides.class.getSimpleName(),
        ParDo.class.getSimpleName());
  }
}
