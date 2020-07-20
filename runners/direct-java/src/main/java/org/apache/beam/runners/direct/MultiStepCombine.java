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
package org.apache.beam.runners.direct;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.PerKey;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.checkerframework.dataflow.qual.SideEffectFree;
import org.joda.time.Instant;

/** A {@link Combine} that performs the combine in multiple steps. */
class MultiStepCombine<
        K extends @Nullable Object,
        InputT extends @Nullable Object,
        AccumT extends @Nullable Object,
        OutputT extends @Nullable Object>
    extends RawPTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {
  public static PTransformMatcher matcher() {
    return new PTransformMatcher() {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        if (PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN.equals(
            PTransformTranslation.urnForTransformOrNull(application.getTransform()))) {
          GlobalCombineFn<?, ?, ?> fn = ((Combine.PerKey) application.getTransform()).getFn();
          return isApplicable(application.getInputs(), fn);
        }
        return false;
      }

      private <K, InputT> boolean isApplicable(
          Map<TupleTag<?>, PValue> inputs, GlobalCombineFn<InputT, ?, ?> fn) {
        if (!(fn instanceof CombineFn)) {
          return false;
        }
        if (inputs.size() == 1) {
          PCollection<KV<K, InputT>> input =
              (PCollection<KV<K, InputT>>) Iterables.getOnlyElement(inputs.values());
          WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
          boolean windowFnApplicable = windowingStrategy.getWindowFn().isNonMerging();
          // Triggering with count based triggers is not appropriately handled here. Disabling
          // most triggers is safe, though more broad than is technically required.
          boolean triggerApplicable = DefaultTrigger.of().equals(windowingStrategy.getTrigger());
          boolean accumulatorCoderAvailable;
          try {
            if (input.getCoder() instanceof KvCoder) {
              KvCoder<K, InputT> kvCoder = (KvCoder<K, InputT>) input.getCoder();
              Coder<?> accumulatorCoder =
                  fn.getAccumulatorCoder(
                      input.getPipeline().getCoderRegistry(), kvCoder.getValueCoder());
              accumulatorCoderAvailable = accumulatorCoder != null;
            } else {
              accumulatorCoderAvailable = false;
            }
          } catch (CannotProvideCoderException e) {
            throw new RuntimeException(
                String.format(
                    "Could not construct an accumulator %s for %s. Accumulator %s for a %s may be"
                        + " null, but may not throw an exception",
                    Coder.class.getSimpleName(),
                    fn,
                    Coder.class.getSimpleName(),
                    Combine.class.getSimpleName()),
                e);
          }
          return windowFnApplicable && triggerApplicable && accumulatorCoderAvailable;
        }
        return false;
      }
    };
  }

  static class Factory<
          K extends @Nullable Object,
          InputT extends @Nullable Object,
          AccumT extends @Nullable Object,
          OutputT extends @Nullable Object>
      extends SingleInputOutputOverrideFactory<
          PCollection<KV<K, InputT>>,
          PCollection<KV<K, OutputT>>,
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {
    public static PTransformOverrideFactory create() {
      return new Factory<>();
    }

    private Factory() {}

    @Override
    public PTransformReplacement<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, InputT>>,
                    PCollection<KV<K, OutputT>>,
                    PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>>
                transform) {
      GlobalCombineFn<?, ?, ?> globalFn = ((Combine.PerKey) transform.getTransform()).getFn();
      checkState(
          globalFn instanceof CombineFn,
          "%s.matcher() should only match %s instances using %s, got %s",
          MultiStepCombine.class.getSimpleName(),
          PerKey.class.getSimpleName(),
          CombineFn.class.getSimpleName(),
          globalFn.getClass().getName());
      @SuppressWarnings("unchecked")
      CombineFn<InputT, AccumT, OutputT> fn = (CombineFn<InputT, AccumT, OutputT>) globalFn;
      @SuppressWarnings("unchecked")
      PCollection<KV<K, InputT>> input =
          (PCollection<KV<K, InputT>>) Iterables.getOnlyElement(transform.getInputs().values());
      @SuppressWarnings("unchecked")
      PCollection<KV<K, OutputT>> output =
          (PCollection<KV<K, OutputT>>) Iterables.getOnlyElement(transform.getOutputs().values());
      return PTransformReplacement.of(input, MultiStepCombine.of(fn, output.getCoder()));
    }
  }

  // ===========================================================================================

  private final CombineFn<InputT, AccumT, OutputT> combineFn;
  private final Coder<KV<K, OutputT>> outputCoder;

  public static <
          K extends @Nullable Object,
          InputT extends @Nullable Object,
          AccumT extends @Nullable Object,
          OutputT extends @Nullable Object>
      MultiStepCombine<K, InputT, AccumT, OutputT> of(
          CombineFn<InputT, AccumT, OutputT> combineFn, Coder<KV<K, OutputT>> outputCoder) {
    return new MultiStepCombine<>(combineFn, outputCoder);
  }

  private MultiStepCombine(
      CombineFn<InputT, AccumT, OutputT> combineFn, Coder<KV<K, OutputT>> outputCoder) {
    this.combineFn = combineFn;
    this.outputCoder = outputCoder;
  }

  @Override
  public @NonNull String getUrn() {
    return "beam:directrunner:transforms:multistepcombine:v1";
  }

  @Override
  public @Nullable FunctionSpec getSpec() {
    return null;
  }

  @Override
  public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
    checkArgument(
        input.getCoder() instanceof KvCoder,
        "Expected input to have a %s of type %s, got %s",
        Coder.class.getSimpleName(),
        KvCoder.class.getSimpleName(),
        input.getCoder());
    KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();
    Coder<InputT> inputValueCoder = inputCoder.getValueCoder();
    Coder<AccumT> accumulatorCoder;
    try {
      accumulatorCoder =
          combineFn.getAccumulatorCoder(input.getPipeline().getCoderRegistry(), inputValueCoder);
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(
          String.format(
              "Could not construct an Accumulator Coder with the provided %s %s",
              CombineFn.class.getSimpleName(), combineFn),
          e);
    }
    return input
        .apply(
            ParDo.of(
                CombineInputs.of(
                    combineFn,
                    input.getWindowingStrategy().getTimestampCombiner(),
                    inputCoder.getKeyCoder())))
        .setCoder(KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder))
        .apply(GroupByKey.create())
        .apply(MergeAndExtractAccumulatorOutput.of(combineFn, outputCoder));
  }

  private static class CombineInputs<
          K extends @Nullable Object,
          InputT extends @Nullable Object,
          AccumT extends @Nullable Object>
      extends DoFn<KV<K, InputT>, KV<K, AccumT>> {
    private final CombineFn<InputT, AccumT, ?> combineFn;
    private final TimestampCombiner timestampCombiner;
    private final Coder<K> keyCoder;

    /**
     * Per-bundle state. Accumulators and output timestamps should only be tracked while a bundle is
     * being processed, and must be cleared when a bundle is completed.
     */
    private transient @Nullable Map<WindowedStructuralKey<K>, AccumT> accumulators;

    private transient @Nullable Map<WindowedStructuralKey<K>, Instant> timestamps;

    private CombineInputs(
        CombineFn<InputT, AccumT, ?> combineFn,
        TimestampCombiner timestampCombiner,
        Coder<K> keyCoder) {
      this.combineFn = combineFn;
      this.timestampCombiner = timestampCombiner;
      this.keyCoder = keyCoder;
    }

    public static <
            K extends @Nullable Object,
            InputT extends @Nullable Object,
            AccumT extends @Nullable Object>
        CombineInputs<K, InputT, AccumT> of(
            CombineFn<InputT, AccumT, ?> combineFn,
            TimestampCombiner timestampCombiner,
            Coder<K> coder) {
      return new CombineInputs<>(combineFn, timestampCombiner, coder);
    }

    @StartBundle
    @EnsuresNonNull({"accumulators", "timestamps"})
    public void startBundle() {
      accumulators = new LinkedHashMap<>();
      timestamps = new LinkedHashMap<>();
    }

    @ProcessElement
    @RequiresNonNull({"accumulators", "timestamps"})
    public void processElement(ProcessContext context, BoundedWindow window) {
      // Establish immutable non-null handles to demonstrate that calling other
      // methods cannot make them null
      final Map<WindowedStructuralKey<K>, Instant> timestamps = this.timestamps;
      final Map<WindowedStructuralKey<K>, AccumT> accumulators = this.accumulators;

      Instant assignedTimestamp = timestampCombiner.assign(window, context.timestamp());

      WindowedStructuralKey<K> key =
          WindowedStructuralKey.create(keyCoder, context.element().getKey(), window);

      @Nullable AccumT accumulator = accumulators.get(key);
      if (accumulator == null) {
        accumulator = combineFn.createAccumulator();
      }

      @Nullable Instant combinedTimestamp = timestamps.get(key);
      if (combinedTimestamp == null) {
        combinedTimestamp = assignedTimestamp;
      }

      accumulators.put(key, combineFn.addInput(accumulator, context.element().getValue()));
      timestamps.put(key, timestampCombiner.combine(assignedTimestamp, combinedTimestamp));
    }

    @FinishBundle
    @RequiresNonNull({"accumulators", "timestamps"})
    public void outputAccumulators(FinishBundleContext context) {
      // Establish immutable non-null handles to demonstrate that calling other
      // methods cannot make them null
      final Map<WindowedStructuralKey<K>, AccumT> accumulators = this.accumulators;
      final Map<WindowedStructuralKey<K>, Instant> timestamps = this.timestamps;

      for (Map.Entry<WindowedStructuralKey<K>, Instant> timestampEntry : timestamps.entrySet()) {
        WindowedStructuralKey<K> key = timestampEntry.getKey();
        Instant timestamp = timestampEntry.getValue();
        // Note that preCombineAccum may be null because no data arrives, or may be null because
        // the accumulator type allows null. For this reason, we must iterate the timestamp entrySet
        AccumT preCombineAccum = accumulators.get(key);
        context.output(
            KV.of(key.getKey(), combineFn.compact(preCombineAccum)), timestamp, key.getWindow());
      }
      this.accumulators = null;
      this.timestamps = null;
    }
  }

  static class WindowedStructuralKey<K extends @Nullable Object> {
    @SideEffectFree
    public static <K extends @Nullable Object> WindowedStructuralKey<K> create(
        Coder<K> keyCoder, K key, BoundedWindow window) {
      return new WindowedStructuralKey<>(StructuralKey.of(key, keyCoder), window);
    }

    private final StructuralKey<K> key;
    private final BoundedWindow window;

    private WindowedStructuralKey(StructuralKey<K> key, BoundedWindow window) {
      this.key = checkNotNull(key, "key cannot be null");
      this.window = checkNotNull(window, "Window cannot be null");
    }

    public K getKey() {
      return key.getKey();
    }

    public BoundedWindow getWindow() {
      return window;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof MultiStepCombine.WindowedStructuralKey)) {
        return false;
      }
      WindowedStructuralKey that = (WindowedStructuralKey<?>) other;
      return this.window.equals(that.window) && this.key.equals(that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(window, key);
    }
  }

  static final String DIRECT_MERGE_ACCUMULATORS_EXTRACT_OUTPUT_URN =
      "beam:directrunner:transforms:merge_accumulators_extract_output:v1";
  /**
   * A primitive {@link PTransform} that merges iterables of accumulators and extracts the output.
   *
   * <p>Required to ensure that Immutability Enforcement is not applied. Accumulators are explicitly
   * mutable.
   */
  static class MergeAndExtractAccumulatorOutput<
          K extends @Nullable Object,
          AccumT extends @Nullable Object,
          OutputT extends @Nullable Object>
      extends RawPTransform<PCollection<KV<K, Iterable<AccumT>>>, PCollection<KV<K, OutputT>>> {
    private final CombineFn<?, AccumT, OutputT> combineFn;
    private final Coder<KV<K, OutputT>> outputCoder;

    private MergeAndExtractAccumulatorOutput(
        CombineFn<?, AccumT, OutputT> combineFn, Coder<KV<K, OutputT>> outputCoder) {
      this.combineFn = combineFn;
      this.outputCoder = outputCoder;
    }

    public static <
            K extends @Nullable Object,
            AccumT extends @Nullable Object,
            OutputT extends @Nullable Object>
        MergeAndExtractAccumulatorOutput<K, AccumT, OutputT> of(
            CombineFn<?, AccumT, OutputT> combineFn, Coder<KV<K, OutputT>> outputCoder) {
      return new MergeAndExtractAccumulatorOutput<>(combineFn, outputCoder);
    }

    CombineFn<?, AccumT, OutputT> getCombineFn() {
      return combineFn;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, Iterable<AccumT>>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), outputCoder);
    }

    @Nonnull
    @Override
    public String getUrn() {
      return DIRECT_MERGE_ACCUMULATORS_EXTRACT_OUTPUT_URN;
    }

    @Override
    public @Nullable FunctionSpec getSpec() {
      return null;
    }
  }

  static class MergeAndExtractAccumulatorOutputEvaluatorFactory
      implements TransformEvaluatorFactory {
    private final EvaluationContext ctxt;

    public MergeAndExtractAccumulatorOutputEvaluatorFactory(EvaluationContext ctxt) {
      this.ctxt = ctxt;
    }

    @Override
    public <InputT> TransformEvaluator<InputT> forApplication(
        AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
      return createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
    }

    private <K, AccumT, OutputT> TransformEvaluator<KV<K, Iterable<AccumT>>> createEvaluator(
        AppliedPTransform<
                PCollection<KV<K, Iterable<AccumT>>>,
                PCollection<KV<K, OutputT>>,
                MergeAndExtractAccumulatorOutput<K, AccumT, OutputT>>
            application,
        CommittedBundle<KV<K, Iterable<AccumT>>> inputBundle) {
      return new MergeAccumulatorsAndExtractOutputEvaluator<>(ctxt, application);
    }

    @Override
    public void cleanup() throws Exception {}
  }

  private static class MergeAccumulatorsAndExtractOutputEvaluator<
          K extends @Nullable Object,
          AccumT extends @Nullable Object,
          OutputT extends @Nullable Object>
      implements TransformEvaluator<KV<K, Iterable<AccumT>>> {
    private final AppliedPTransform<
            PCollection<KV<K, Iterable<AccumT>>>,
            PCollection<KV<K, OutputT>>,
            MergeAndExtractAccumulatorOutput<K, AccumT, OutputT>>
        application;
    private final CombineFn<?, AccumT, OutputT> combineFn;
    private final UncommittedBundle<KV<K, OutputT>> output;

    public MergeAccumulatorsAndExtractOutputEvaluator(
        EvaluationContext ctxt,
        AppliedPTransform<
                PCollection<KV<K, Iterable<AccumT>>>,
                PCollection<KV<K, OutputT>>,
                MergeAndExtractAccumulatorOutput<K, AccumT, OutputT>>
            application) {
      this.application = application;
      this.combineFn = application.getTransform().getCombineFn();
      this.output =
          ctxt.createBundle(
              (PCollection<KV<K, OutputT>>)
                  Iterables.getOnlyElement(application.getOutputs().values()));
    }

    @Override
    public void processElement(WindowedValue<KV<K, Iterable<AccumT>>> element) throws Exception {
      checkState(
          element.getWindows().size() == 1,
          "Expected inputs to %s to be in exactly one window. Got %s",
          MergeAccumulatorsAndExtractOutputEvaluator.class.getSimpleName(),
          element.getWindows().size());
      Iterable<AccumT> inputAccumulators = element.getValue().getValue();
      try {
        AccumT first = combineFn.createAccumulator();
        AccumT merged =
            combineFn.mergeAccumulators(
                Iterables.concat(
                    Collections.singleton(first),
                    inputAccumulators,
                    Collections.singleton(combineFn.createAccumulator())));
        OutputT extracted = combineFn.extractOutput(merged);
        output.add(element.withValue(KV.of(element.getValue().getKey(), extracted)));
      } catch (Exception e) {
        throw UserCodeException.wrap(e);
      }
    }

    @Override
    public TransformResult<KV<K, Iterable<AccumT>>> finishBundle() throws Exception {
      return StepTransformResult.<KV<K, Iterable<AccumT>>>withoutHold(application)
          .addOutput(output)
          .build();
    }
  }
}
