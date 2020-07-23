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
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.StateSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.TimerFamilySpec;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.ParDoTranslation.ParDoLike;
import org.apache.beam.runners.core.construction.ReadTranslation.BoundedReadPayloadTranslator;
import org.apache.beam.runners.core.construction.ReadTranslation.UnboundedReadPayloadTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.ArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.BaseArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A utility transform that executes a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} by expanding it into a
 * network of simpler transforms:
 *
 * <ol>
 *   <li>Pair each element with an initial restriction
 *   <li>Split each restriction into sub-restrictions
 *   <li>Explode windows, since splitting within each window has to happen independently
 *   <li>Assign a unique key to each element/restriction pair
 *   <li>Process the keyed element/restriction pairs in a runner-specific way with the splittable
 *       {@link DoFn}'s {@link DoFn.ProcessElement} method.
 * </ol>
 *
 * <p>This transform is intended as a helper for internal use by runners when implementing {@code
 * ParDo.of(splittable DoFn)}, but not for direct use by pipeline writers.
 */
public class SplittableParDo<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
    extends PTransform<PCollection<InputT>, PCollectionTuple> {
  /**
   * A {@link PTransformOverrideFactory} that overrides a <a
   * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a> with {@link SplittableParDo}.
   */
  public static class OverrideFactory<InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<InputT>, PCollectionTuple, MultiOutput<InputT, OutputT>> {
    @Override
    public PTransformReplacement<PCollection<InputT>, PCollectionTuple> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollectionTuple, MultiOutput<InputT, OutputT>>
            transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform), forAppliedParDo(transform));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  private final DoFn<InputT, OutputT> doFn;
  private final List<PCollectionView<?>> sideInputs;
  private final TupleTag<OutputT> mainOutputTag;
  private final TupleTagList additionalOutputTags;
  private final Map<TupleTag<?>, Coder<?>> outputTagsToCoders;

  public static final String SPLITTABLE_PROCESS_URN =
      "beam:runners_core:transforms:splittable_process:v1";

  public static final String SPLITTABLE_GBKIKWI_URN =
      "beam:runners_core:transforms:splittable_gbkikwi:v1";

  private SplittableParDo(
      DoFn<InputT, OutputT> doFn,
      List<PCollectionView<?>> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      TupleTagList additionalOutputTags,
      Map<TupleTag<?>, Coder<?>> outputTagsToCoders) {
    checkArgument(
        DoFnSignatures.getSignature(doFn.getClass()).processElement().isSplittable(),
        "fn must be a splittable DoFn");
    this.doFn = doFn;
    this.sideInputs = sideInputs;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.outputTagsToCoders = outputTagsToCoders;
  }

  /**
   * Creates the transform for a {@link ParDo}-compatible {@link AppliedPTransform}.
   *
   * <p>The input may generally be a deserialized transform so it may not actually be a {@link
   * ParDo}. Instead {@link ParDoTranslation} will be used to extract fields.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <InputT, OutputT> SplittableParDo<InputT, OutputT, ?, ?> forAppliedParDo(
      AppliedPTransform<PCollection<InputT>, PCollectionTuple, ?> parDo) {
    checkArgument(parDo != null, "parDo must not be null");

    try {
      Map<TupleTag<?>, Coder<?>> outputTagsToCoders = Maps.newHashMap();
      for (Map.Entry<TupleTag<?>, PValue> entry : parDo.getOutputs().entrySet()) {
        outputTagsToCoders.put(entry.getKey(), ((PCollection) entry.getValue()).getCoder());
      }
      return new SplittableParDo(
          ParDoTranslation.getDoFn(parDo),
          ParDoTranslation.getSideInputs(parDo),
          ParDoTranslation.getMainOutputTag(parDo),
          ParDoTranslation.getAdditionalOutputTags(parDo),
          outputTagsToCoders);
    } catch (IOException exc) {
      throw new RuntimeException(exc);
    }
  }

  @Override
  public PCollectionTuple expand(PCollection<InputT> input) {
    Coder<RestrictionT> restrictionCoder =
        DoFnInvokers.invokerFor(doFn)
            .invokeGetRestrictionCoder(input.getPipeline().getCoderRegistry());
    Coder<WatermarkEstimatorStateT> watermarkEstimatorStateCoder =
        DoFnInvokers.invokerFor(doFn)
            .invokeGetWatermarkEstimatorStateCoder(input.getPipeline().getCoderRegistry());
    Coder<KV<InputT, RestrictionT>> splitCoder = KvCoder.of(input.getCoder(), restrictionCoder);

    PCollection<KV<byte[], KV<InputT, RestrictionT>>> keyedRestrictions =
        input
            .apply(
                "Pair with initial restriction",
                ParDo.of(new PairWithRestrictionFn<InputT, OutputT, RestrictionT>(doFn)))
            .setCoder(splitCoder)
            .apply("Split restriction", ParDo.of(new SplitRestrictionFn<>(doFn)))
            .setCoder(splitCoder)
            // ProcessFn requires all input elements to be in a single window and have a single
            // element per work item. This must precede the unique keying so each key has a single
            // associated element.
            .apply("Explode windows", ParDo.of(new ExplodeWindowsFn<>()))
            .apply("Assign unique key", WithKeys.of(new RandomUniqueKeyFn<>()));

    return keyedRestrictions.apply(
        "ProcessKeyedElements",
        new ProcessKeyedElements<>(
            doFn,
            input.getCoder(),
            restrictionCoder,
            watermarkEstimatorStateCoder,
            (WindowingStrategy<InputT, ?>) input.getWindowingStrategy(),
            sideInputs,
            mainOutputTag,
            additionalOutputTags,
            outputTagsToCoders));
  }

  @Override
  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return PCollectionViews.toAdditionalInputs(sideInputs);
  }

  /**
   * A {@link DoFn} that forces each of its outputs to be in a single window, by indicating to the
   * runner that it observes the window of its input element, so the runner is forced to apply it to
   * each input in a single window and thus its output is also in a single window.
   */
  private static class ExplodeWindowsFn<InputT> extends DoFn<InputT, InputT> {
    @ProcessElement
    public void process(ProcessContext c, BoundedWindow window) {
      c.output(c.element());
    }
  }

  /**
   * Runner-specific primitive {@link PTransform} that invokes the {@link DoFn.ProcessElement}
   * method for a splittable {@link DoFn} on each {@link KV} of the input {@link PCollection} of
   * {@link KV KVs} keyed with arbitrary but globally unique keys.
   */
  public static class ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
      extends PTransform<PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple> {
    private final DoFn<InputT, OutputT> fn;
    private final Coder<InputT> elementCoder;
    private final Coder<RestrictionT> restrictionCoder;
    private final Coder<WatermarkEstimatorStateT> watermarkEstimatorStateCoder;
    private final WindowingStrategy<InputT, ?> windowingStrategy;
    private final List<PCollectionView<?>> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final TupleTagList additionalOutputTags;
    private final Map<TupleTag<?>, Coder<?>> outputTagsToCoders;

    /**
     * @param fn the splittable {@link DoFn}.
     * @param windowingStrategy the {@link WindowingStrategy} of the input collection.
     * @param sideInputs list of side inputs that should be available to the {@link DoFn}.
     * @param mainOutputTag {@link TupleTag Tag} of the {@link DoFn DoFn's} main output.
     * @param additionalOutputTags {@link TupleTagList Tags} of the {@link DoFn DoFn's} additional
     * @param outputTagsToCoders A map from output tag to the coder for that output, which should
     *     provide mappings for the main and all additional tags.
     */
    public ProcessKeyedElements(
        DoFn<InputT, OutputT> fn,
        Coder<InputT> elementCoder,
        Coder<RestrictionT> restrictionCoder,
        Coder<WatermarkEstimatorStateT> watermarkEstimatorStateCoder,
        WindowingStrategy<InputT, ?> windowingStrategy,
        List<PCollectionView<?>> sideInputs,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        Map<TupleTag<?>, Coder<?>> outputTagsToCoders) {
      this.fn = fn;
      this.elementCoder = elementCoder;
      this.restrictionCoder = restrictionCoder;
      this.watermarkEstimatorStateCoder = watermarkEstimatorStateCoder;
      this.windowingStrategy = windowingStrategy;
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
      this.outputTagsToCoders = outputTagsToCoders;
    }

    public DoFn<InputT, OutputT> getFn() {
      return fn;
    }

    public Coder<InputT> getElementCoder() {
      return elementCoder;
    }

    public Coder<RestrictionT> getRestrictionCoder() {
      return restrictionCoder;
    }

    public Coder<WatermarkEstimatorStateT> getWatermarkEstimatorStateCoder() {
      return watermarkEstimatorStateCoder;
    }

    public WindowingStrategy<InputT, ?> getInputWindowingStrategy() {
      return windowingStrategy;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    public TupleTag<OutputT> getMainOutputTag() {
      return mainOutputTag;
    }

    public TupleTagList getAdditionalOutputTags() {
      return additionalOutputTags;
    }

    public Map<TupleTag<?>, Coder<?>> getOutputTagsToCoders() {
      return outputTagsToCoders;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<byte[], KV<InputT, RestrictionT>>> input) {
      return createPrimitiveOutputFor(
          input, fn, mainOutputTag, additionalOutputTags, outputTagsToCoders, windowingStrategy);
    }

    public static <OutputT> PCollectionTuple createPrimitiveOutputFor(
        PCollection<?> input,
        DoFn<?, OutputT> fn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        Map<TupleTag<?>, Coder<?>> outputTagsToCoders,
        WindowingStrategy<?, ?> windowingStrategy) {
      DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
      PCollectionTuple outputs =
          PCollectionTuple.ofPrimitiveOutputsInternal(
              input.getPipeline(),
              TupleTagList.of(mainOutputTag).and(additionalOutputTags.getAll()),
              outputTagsToCoders,
              windowingStrategy,
              input.isBounded().and(signature.isBoundedPerElement()));

      // Set output type descriptor similarly to how ParDo.MultiOutput does it.
      outputs.get(mainOutputTag).setTypeDescriptor(fn.getOutputTypeDescriptor());

      return outputs;
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }
  }

  /** Registers {@link UnboundedReadPayloadTranslator} and {@link BoundedReadPayloadTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(ProcessKeyedElements.class, new ProcessKeyedElementsTranslator())
          .build();
    }
  }

  /** A translator for {@link ProcessKeyedElements}. */
  public static class ProcessKeyedElementsTranslator
      implements PTransformTranslation.TransformPayloadTranslator<
          ProcessKeyedElements<?, ?, ?, ?>> {

    public static TransformPayloadTranslator create() {
      return new ProcessKeyedElementsTranslator();
    }

    private ProcessKeyedElementsTranslator() {}

    @Override
    public String getUrn(ProcessKeyedElements<?, ?, ?, ?> transform) {
      return PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, ProcessKeyedElements<?, ?, ?, ?>> transform,
        SdkComponents components)
        throws IOException {
      ProcessKeyedElements<?, ?, ?, ?> pke = transform.getTransform();
      final DoFn<?, ?> fn = pke.getFn();
      final DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
      final String restrictionCoderId = components.registerCoder(pke.getRestrictionCoder());

      ParDoPayload payload =
          ParDoTranslation.payloadForParDoLike(
              new ParDoLike() {
                @Override
                public FunctionSpec translateDoFn(SdkComponents newComponents) {
                  // Schemas not yet supported on splittable DoFn.
                  return ParDoTranslation.translateDoFn(
                      fn,
                      pke.getMainOutputTag(),
                      Collections.emptyMap(),
                      DoFnSchemaInformation.create(),
                      newComponents);
                }

                @Override
                public Map<String, SideInput> translateSideInputs(SdkComponents components) {
                  return ParDoTranslation.translateSideInputs(pke.getSideInputs(), components);
                }

                @Override
                public Map<String, StateSpec> translateStateSpecs(SdkComponents components) {
                  // SDFs don't have state.
                  return ImmutableMap.of();
                }

                @Override
                public Map<String, TimerFamilySpec> translateTimerFamilySpecs(
                    SdkComponents newComponents) {
                  // SDFs don't have timers.
                  return ImmutableMap.of();
                }

                @Override
                public boolean isStateful() {
                  return !signature.stateDeclarations().isEmpty()
                      || !signature.timerDeclarations().isEmpty()
                      || !signature.timerFamilyDeclarations().isEmpty();
                }

                @Override
                public boolean isSplittable() {
                  return true;
                }

                @Override
                public boolean isRequiresStableInput() {
                  return signature.processElement().requiresStableInput();
                }

                @Override
                public boolean isRequiresTimeSortedInput() {
                  return signature.processElement().requiresTimeSortedInput();
                }

                @Override
                public boolean requestsFinalization() {
                  return (signature.startBundle() != null
                          && signature
                              .startBundle()
                              .extraParameters()
                              .contains(DoFnSignature.Parameter.bundleFinalizer()))
                      || (signature.processElement() != null
                          && signature
                              .processElement()
                              .extraParameters()
                              .contains(DoFnSignature.Parameter.bundleFinalizer()))
                      || (signature.finishBundle() != null
                          && signature
                              .finishBundle()
                              .extraParameters()
                              .contains(DoFnSignature.Parameter.bundleFinalizer()));
                }

                @Override
                public String translateRestrictionCoderId(SdkComponents newComponents) {
                  return restrictionCoderId;
                }
              },
              components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(pke))
          .setPayload(payload.toByteString())
          .build();
    }
  }

  /**
   * Assigns a random unique key to each element of the input collection, so that the output
   * collection is effectively the same elements as input, but the per-key state and timers are now
   * effectively per-element.
   */
  private static class RandomUniqueKeyFn<T> implements SerializableFunction<T, byte[]> {
    @Override
    public byte[] apply(T input) {
      byte[] key = new byte[128];
      ThreadLocalRandom.current().nextBytes(key);
      return key;
    }
  }

  /**
   * Pairs each input element with its initial restriction using the given splittable {@link DoFn}.
   */
  private static class PairWithRestrictionFn<InputT, OutputT, RestrictionT>
      extends DoFn<InputT, KV<InputT, RestrictionT>> {
    private DoFn<InputT, OutputT> fn;

    // Initialized in setup()
    private transient @Nullable DoFnInvoker<InputT, OutputT> invoker;

    PairWithRestrictionFn(DoFn<InputT, OutputT> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.invokerFor(fn);
      invoker.invokeSetup();
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow w) {
      context.output(
          KV.of(
              context.element(),
              invoker.invokeGetInitialRestriction(
                  new BaseArgumentProvider<InputT, OutputT>() {
                    @Override
                    public InputT element(DoFn<InputT, OutputT> doFn) {
                      return context.element();
                    }

                    @Override
                    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                      return context.timestamp();
                    }

                    @Override
                    public PipelineOptions pipelineOptions() {
                      return context.getPipelineOptions();
                    }

                    @Override
                    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                      return context.pane();
                    }

                    @Override
                    public BoundedWindow window() {
                      return w;
                    }

                    @Override
                    public String getErrorContext() {
                      return PairWithRestrictionFn.class.getSimpleName()
                          + ".invokeGetInitialRestriction";
                    }
                  })));
    }

    @Teardown
    public void tearDown() {
      invoker.invokeTeardown();
      invoker = null;
    }
  }

  /** Splits the restriction using the given {@link SplitRestriction} method. */
  private static class SplitRestrictionFn<InputT, RestrictionT>
      extends DoFn<KV<InputT, RestrictionT>, KV<InputT, RestrictionT>> {
    private final DoFn<InputT, ?> splittableFn;

    // Initialized in setup()
    private transient @Nullable DoFnInvoker<InputT, ?> invoker;

    SplitRestrictionFn(DoFn<InputT, ?> splittableFn) {
      this.splittableFn = splittableFn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.invokerFor(splittableFn);
      invoker.invokeSetup();
    }

    @ProcessElement
    public void processElement(final ProcessContext c, BoundedWindow w) {
      invoker.invokeSplitRestriction(
          (ArgumentProvider)
              new BaseArgumentProvider<InputT, RestrictionT>() {
                @Override
                public InputT element(DoFn<InputT, RestrictionT> doFn) {
                  return c.element().getKey();
                }

                @Override
                public Object restriction() {
                  return c.element().getValue();
                }

                @Override
                public RestrictionTracker<?, ?> restrictionTracker() {
                  return invoker.invokeNewTracker((DoFnInvoker.BaseArgumentProvider) this);
                }

                @Override
                public Instant timestamp(DoFn<InputT, RestrictionT> doFn) {
                  return c.timestamp();
                }

                @Override
                public PipelineOptions pipelineOptions() {
                  return c.getPipelineOptions();
                }

                @Override
                public PaneInfo paneInfo(DoFn<InputT, RestrictionT> doFn) {
                  return c.pane();
                }

                @Override
                public BoundedWindow window() {
                  return w;
                }

                @Override
                public OutputReceiver<RestrictionT> outputReceiver(
                    DoFn<InputT, RestrictionT> doFn) {
                  return new OutputReceiver<RestrictionT>() {
                    @Override
                    public void output(RestrictionT part) {
                      c.output(KV.of(c.element().getKey(), part));
                    }

                    @Override
                    public void outputWithTimestamp(RestrictionT part, Instant timestamp) {
                      throw new UnsupportedOperationException();
                    }
                  };
                }

                @Override
                public String getErrorContext() {
                  return SplitRestrictionFn.class.getSimpleName() + ".invokeSplitRestriction";
                }
              });
    }

    @Teardown
    public void tearDown() {
      invoker.invokeTeardown();
      invoker = null;
    }
  }
}
