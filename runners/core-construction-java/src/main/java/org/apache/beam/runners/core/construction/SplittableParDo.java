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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A utility transform that executes a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} by expanding it into a
 * network of simpler transforms:
 *
 * <ol>
 * <li>Pair each element with an initial restriction
 * <li>Split each restriction into sub-restrictions
 * <li>Explode windows, since splitting within each window has to happen independently
 * <li>Assign a unique key to each element/restriction pair
 * <li>Process the keyed element/restriction pairs in a runner-specific way with the splittable
 *     {@link DoFn}'s {@link DoFn.ProcessElement} method.
 * </ol>
 *
 * <p>This transform is intended as a helper for internal use by runners when implementing {@code
 * ParDo.of(splittable DoFn)}, but not for direct use by pipeline writers.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class SplittableParDo<InputT, OutputT, RestrictionT>
    extends PTransform<PCollection<InputT>, PCollectionTuple> {

  private final DoFn<InputT, OutputT> doFn;
  private final List<PCollectionView<?>> sideInputs;
  private final TupleTag<OutputT> mainOutputTag;
  private final TupleTagList additionalOutputTags;

  public static final String SPLITTABLE_PROCESS_URN =
      "urn:beam:runners_core:transforms:splittable_process:v1";

  public static final String SPLITTABLE_PROCESS_KEYED_ELEMENTS_URN =
      "urn:beam:runners_core:transforms:splittable_process_keyed_elements:v1";

  public static final String SPLITTABLE_GBKIKWI_URN =
      "urn:beam:runners_core:transforms:splittable_gbkikwi:v1";

  private SplittableParDo(
      DoFn<InputT, OutputT> doFn,
      TupleTag<OutputT> mainOutputTag,
      List<PCollectionView<?>> sideInputs,
      TupleTagList additionalOutputTags) {
    checkArgument(
        DoFnSignatures.getSignature(doFn.getClass()).processElement().isSplittable(),
        "fn must be a splittable DoFn");
    this.doFn = doFn;
    this.mainOutputTag = mainOutputTag;
    this.sideInputs = sideInputs;
    this.additionalOutputTags = additionalOutputTags;
  }

  /**
   * Creates a {@link SplittableParDo} from an original Java {@link ParDo}.
   *
   * @param parDo The splittable {@link ParDo} transform.
   */
  public static <InputT, OutputT> SplittableParDo<InputT, OutputT, ?> forJavaParDo(
      ParDo.MultiOutput<InputT, OutputT> parDo) {
    checkArgument(parDo != null, "parDo must not be null");
    checkArgument(
        DoFnSignatures.getSignature(parDo.getFn().getClass()).processElement().isSplittable(),
        "fn must be a splittable DoFn");
    return new SplittableParDo(
        parDo.getFn(),
        parDo.getMainOutputTag(),
        parDo.getSideInputs(),
        parDo.getAdditionalOutputTags());
  }

  /**
   * Creates the transform for a {@link ParDo}-compatible {@link AppliedPTransform}.
   *
   * <p>The input may generally be a deserialized transform so it may not actually be a {@link
   * ParDo}. Instead {@link ParDoTranslation} will be used to extract fields.
   */
  public static SplittableParDo<?, ?, ?> forAppliedParDo(AppliedPTransform<?, ?, ?> parDo) {
    checkArgument(parDo != null, "parDo must not be null");

    try {
      return new SplittableParDo<>(
          ParDoTranslation.getDoFn(parDo),
          (TupleTag) ParDoTranslation.getMainOutputTag(parDo),
          ParDoTranslation.getSideInputs(parDo),
          ParDoTranslation.getAdditionalOutputTags(parDo));
    } catch (IOException exc) {
      throw new RuntimeException(exc);
    }
  }

  @Override
  public PCollectionTuple expand(PCollection<InputT> input) {
    Coder<RestrictionT> restrictionCoder =
        DoFnInvokers.invokerFor(doFn)
            .invokeGetRestrictionCoder(input.getPipeline().getCoderRegistry());
    Coder<KV<InputT, RestrictionT>> splitCoder = KvCoder.of(input.getCoder(), restrictionCoder);

    PCollection<KV<String, KV<InputT, RestrictionT>>> keyedRestrictions =
        input
            .apply(
                "Pair with initial restriction",
                ParDo.of(new PairWithRestrictionFn<InputT, OutputT, RestrictionT>(doFn)))
            .setCoder(splitCoder)
            .apply(
                "Split restriction", ParDo.of(new SplitRestrictionFn<InputT, RestrictionT>(doFn)))
            .setCoder(splitCoder)
            // ProcessFn requires all input elements to be in a single window and have a single
            // element per work item. This must precede the unique keying so each key has a single
            // associated element.
            .apply("Explode windows", ParDo.of(new ExplodeWindowsFn<KV<InputT, RestrictionT>>()))
            .apply(
                "Assign unique key",
                WithKeys.of(new RandomUniqueKeyFn<KV<InputT, RestrictionT>>()));

    return keyedRestrictions.apply(
        "ProcessKeyedElements",
        new ProcessKeyedElements<>(
            doFn,
            input.getCoder(),
            restrictionCoder,
            (WindowingStrategy<InputT, ?>) input.getWindowingStrategy(),
            sideInputs,
            mainOutputTag,
            additionalOutputTags));
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
  public static class ProcessKeyedElements<InputT, OutputT, RestrictionT>
      extends RawPTransform<PCollection<KV<String, KV<InputT, RestrictionT>>>, PCollectionTuple> {
    private final DoFn<InputT, OutputT> fn;
    private final Coder<InputT> elementCoder;
    private final Coder<RestrictionT> restrictionCoder;
    private final WindowingStrategy<InputT, ?> windowingStrategy;
    private final List<PCollectionView<?>> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final TupleTagList additionalOutputTags;

    /**
     * @param fn the splittable {@link DoFn}.
     * @param windowingStrategy the {@link WindowingStrategy} of the input collection.
     * @param sideInputs list of side inputs that should be available to the {@link DoFn}.
     * @param mainOutputTag {@link TupleTag Tag} of the {@link DoFn DoFn's} main output.
     * @param additionalOutputTags {@link TupleTagList Tags} of the {@link DoFn DoFn's} additional
     *     outputs.
     */
    public ProcessKeyedElements(
        DoFn<InputT, OutputT> fn,
        Coder<InputT> elementCoder,
        Coder<RestrictionT> restrictionCoder,
        WindowingStrategy<InputT, ?> windowingStrategy,
        List<PCollectionView<?>> sideInputs,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags) {
      this.fn = fn;
      this.elementCoder = elementCoder;
      this.restrictionCoder = restrictionCoder;
      this.windowingStrategy = windowingStrategy;
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
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

    @Override
    public PCollectionTuple expand(PCollection<KV<String, KV<InputT, RestrictionT>>> input) {
      return createPrimitiveOutputFor(
          input, fn, mainOutputTag, additionalOutputTags, windowingStrategy);
    }

    public static <OutputT> PCollectionTuple createPrimitiveOutputFor(
        PCollection<?> input,
        DoFn<?, OutputT> fn,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        WindowingStrategy<?, ?> windowingStrategy) {
      DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
      PCollectionTuple outputs =
          PCollectionTuple.ofPrimitiveOutputsInternal(
              input.getPipeline(),
              TupleTagList.of(mainOutputTag).and(additionalOutputTags.getAll()),
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

    @Override
    public String getUrn() {
      return SPLITTABLE_PROCESS_KEYED_ELEMENTS_URN;
    }
  }

  /**
   * Assigns a random unique key to each element of the input collection, so that the output
   * collection is effectively the same elements as input, but the per-key state and timers are now
   * effectively per-element.
   */
  private static class RandomUniqueKeyFn<T> implements SerializableFunction<T, String> {
    @Override
    public String apply(T input) {
      return UUID.randomUUID().toString();
    }
  }

  /**
   * Pairs each input element with its initial restriction using the given splittable {@link DoFn}.
   */
  private static class PairWithRestrictionFn<InputT, OutputT, RestrictionT>
      extends DoFn<InputT, KV<InputT, RestrictionT>> {
    private DoFn<InputT, OutputT> fn;
    private transient DoFnInvoker<InputT, OutputT> invoker;

    PairWithRestrictionFn(DoFn<InputT, OutputT> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.invokerFor(fn);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          KV.of(
              context.element(),
              invoker.<RestrictionT>invokeGetInitialRestriction(context.element())));
    }
  }

  /** Splits the restriction using the given {@link SplitRestriction} method. */
  private static class SplitRestrictionFn<InputT, RestrictionT>
      extends DoFn<KV<InputT, RestrictionT>, KV<InputT, RestrictionT>> {
    private final DoFn<InputT, ?> splittableFn;
    private transient DoFnInvoker<InputT, ?> invoker;

    SplitRestrictionFn(DoFn<InputT, ?> splittableFn) {
      this.splittableFn = splittableFn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.invokerFor(splittableFn);
    }

    @ProcessElement
    public void processElement(final ProcessContext c) {
      final InputT element = c.element().getKey();
      invoker.invokeSplitRestriction(
          element,
          c.element().getValue(),
          new OutputReceiver<RestrictionT>() {
            @Override
            public void output(RestrictionT part) {
              c.output(KV.of(element, part));
            }
          });
    }
  }
}
