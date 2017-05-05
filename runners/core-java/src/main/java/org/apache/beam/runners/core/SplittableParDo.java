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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A utility transform that executes a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} by expanding it into a
 * network of simpler transforms:
 *
 * <ol>
 * <li>Pair each element with an initial restriction
 * <li>Split each restriction into sub-restrictions
 * <li>Assign a unique key to each element/restriction pair
 * <li>Group by key (so that work is partitioned by key and we can access state/timers)
 * <li>Process each keyed element/restriction pair with the splittable {@link DoFn}'s {@link
 *     DoFn.ProcessElement} method, using state and timers API.
 * </ol>
 *
 * <p>This transform is intended as a helper for internal use by runners when implementing {@code
 * ParDo.of(splittable DoFn)}, but not for direct use by pipeline writers.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class SplittableParDo<InputT, OutputT, RestrictionT>
    extends PTransform<PCollection<InputT>, PCollectionTuple> {
  private final ParDo.MultiOutput<InputT, OutputT> parDo;

  /**
   * Creates the transform for the given original multi-output {@link ParDo}.
   *
   * @param parDo The splittable {@link ParDo} transform.
   */
  public SplittableParDo(ParDo.MultiOutput<InputT, OutputT> parDo) {
    checkNotNull(parDo, "parDo must not be null");
    this.parDo = parDo;
    checkArgument(
        DoFnSignatures.getSignature(parDo.getFn().getClass()).processElement().isSplittable(),
        "fn must be a splittable DoFn");
  }

  @Override
  public PCollectionTuple expand(PCollection<InputT> input) {
    return applyTyped(input);
  }

  private PCollectionTuple applyTyped(PCollection<InputT> input) {
    DoFn<InputT, OutputT> fn = parDo.getFn();
    Coder<RestrictionT> restrictionCoder =
        DoFnInvokers.invokerFor(fn)
            .invokeGetRestrictionCoder(input.getPipeline().getCoderRegistry());
    PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>> keyedWorkItems =
        applySplitIntoKeyedWorkItems(input, fn, restrictionCoder);
    return keyedWorkItems.apply(
        "Process",
        new ProcessElements<>(
            fn,
            input.getCoder(),
            restrictionCoder,
            (WindowingStrategy<InputT, ?>) input.getWindowingStrategy(),
            parDo.getSideInputs(),
            parDo.getMainOutputTag(),
            parDo.getAdditionalOutputTags()));
  }

  private static <InputT, OutputT, RestrictionT>
      PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>
          applySplitIntoKeyedWorkItems(
              PCollection<InputT> input,
              DoFn<InputT, OutputT> fn,
              Coder<RestrictionT> restrictionCoder) {
    Coder<ElementAndRestriction<InputT, RestrictionT>> splitCoder =
        ElementAndRestrictionCoder.of(input.getCoder(), restrictionCoder);

    PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>> keyedWorkItems =
        input
            .apply(
                "Pair with initial restriction",
                ParDo.of(new PairWithRestrictionFn<InputT, OutputT, RestrictionT>(fn)))
            .setCoder(splitCoder)
            .apply("Split restriction", ParDo.of(new SplitRestrictionFn<InputT, RestrictionT>(fn)))
            .setCoder(splitCoder)
            // ProcessFn requires all input elements to be in a single window and have a single
            // element per work item. This must precede the unique keying so each key has a single
            // associated element.
            .apply(
                "Explode windows",
                ParDo.of(new ExplodeWindowsFn<ElementAndRestriction<InputT, RestrictionT>>()))
            .apply(
                "Assign unique key",
                WithKeys.of(new RandomUniqueKeyFn<ElementAndRestriction<InputT, RestrictionT>>()))
            .apply(
                "Group by key",
                new GBKIntoKeyedWorkItems<String, ElementAndRestriction<InputT, RestrictionT>>())
            .setCoder(
                KeyedWorkItemCoder.of(
                    StringUtf8Coder.of(),
                    splitCoder,
                    input.getWindowingStrategy().getWindowFn().windowCoder()));
    checkArgument(
        keyedWorkItems.getWindowingStrategy().getWindowFn() instanceof GlobalWindows,
        "GBKIntoKeyedWorkItems must produce a globally windowed collection, "
            + "but windowing strategy was: %s",
        keyedWorkItems.getWindowingStrategy());
    return keyedWorkItems;
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
   * Runner-specific primitive {@link GroupByKey GroupByKey-like} {@link PTransform} that produces
   * {@link KeyedWorkItem KeyedWorkItems} so that downstream transforms can access state and timers.
   *
   * <p>Unlike a real {@link GroupByKey}, ignores the input's windowing and triggering strategy and
   * emits output immediately.
   */
  public static class GBKIntoKeyedWorkItems<KeyT, InputT>
      extends PTransform<PCollection<KV<KeyT, InputT>>, PCollection<KeyedWorkItem<KeyT, InputT>>> {
    @Override
    public PCollection<KeyedWorkItem<KeyT, InputT>> expand(PCollection<KV<KeyT, InputT>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), input.isBounded());
    }
  }

  /**
   * Runner-specific primitive {@link PTransform} that invokes the {@link DoFn.ProcessElement}
   * method for a splittable {@link DoFn}.
   */
  public static class ProcessElements<
          InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
      extends PTransform<
          PCollection<? extends KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>,
          PCollectionTuple> {
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
    public ProcessElements(
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

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    public TupleTag<OutputT> getMainOutputTag() {
      return mainOutputTag;
    }

    public TupleTagList getAdditionalOutputTags() {
      return additionalOutputTags;
    }

    public ProcessFn<InputT, OutputT, RestrictionT, TrackerT> newProcessFn(
        DoFn<InputT, OutputT> fn) {
      return new SplittableParDo.ProcessFn<>(
          fn, elementCoder, restrictionCoder, windowingStrategy);
    }

    @Override
    public PCollectionTuple expand(
        PCollection<? extends KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>
            input) {
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
    public <T> Coder<T> getDefaultOutputCoder(
        PCollection<? extends KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>
            input,
        PCollection<T> output)
        throws CannotProvideCoderException {
      // Similar logic to ParDo.MultiOutput.getOutputCoder.
      @SuppressWarnings("unchecked")
      KeyedWorkItemCoder<String, ElementAndRestriction<InputT, RestrictionT>> kwiCoder =
          (KeyedWorkItemCoder) input.getCoder();
      Coder<InputT> inputCoder =
          ((ElementAndRestrictionCoder<InputT, RestrictionT>) kwiCoder.getElementCoder())
              .getElementCoder();
      return input
          .getPipeline()
          .getCoderRegistry()
          .getCoder(output.getTypeDescriptor(), fn.getInputTypeDescriptor(), inputCoder);
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
      extends DoFn<InputT, ElementAndRestriction<InputT, RestrictionT>> {
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
          ElementAndRestriction.of(
              context.element(),
              invoker.<RestrictionT>invokeGetInitialRestriction(context.element())));
    }
  }

  /**
   * The heart of splittable {@link DoFn} execution: processes a single (element, restriction) pair
   * by creating a tracker for the restriction and checkpointing/resuming processing later if
   * necessary.
   *
   * <p>Takes {@link KeyedWorkItem} and assumes that the KeyedWorkItem contains a single element
   * (or a single timer set by {@link ProcessFn itself}, in a single window. This is necessary
   * because {@link ProcessFn} sets timers, and timers are namespaced to a single window and it
   * should be the window of the input element.
   *
   * <p>See also: https://issues.apache.org/jira/browse/BEAM-1983
   */
  @VisibleForTesting
  public static class ProcessFn<
          InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
      extends DoFn<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT> {
    /**
     * The state cell containing a watermark hold for the output of this {@link DoFn}. The hold is
     * acquired during the first {@link DoFn.ProcessElement} call for each element and restriction,
     * and is released when the {@link DoFn.ProcessElement} call returns and there is no residual
     * restriction captured by the {@link SplittableProcessElementInvoker}.
     *
     * <p>A hold is needed to avoid letting the output watermark immediately progress together with
     * the input watermark when the first {@link DoFn.ProcessElement} call for this element
     * completes.
     */
    private static final StateTag<WatermarkHoldState> watermarkHoldTag =
        StateTags.makeSystemTagInternal(
            StateTags.<GlobalWindow>watermarkStateInternal(
                "hold", TimestampCombiner.LATEST));

    /**
     * The state cell containing a copy of the element. Written during the first {@link
     * DoFn.ProcessElement} call and read during subsequent calls in response to timer firings, when
     * the original element is no longer available.
     */
    private final StateTag<ValueState<WindowedValue<InputT>>> elementTag;

    /**
     * The state cell containing a restriction representing the unprocessed part of work for this
     * element.
     */
    private StateTag<ValueState<RestrictionT>> restrictionTag;

    private final DoFn<InputT, OutputT> fn;
    private final Coder<InputT> elementCoder;
    private final Coder<RestrictionT> restrictionCoder;
    private final WindowingStrategy<InputT, ?> inputWindowingStrategy;

    private transient StateInternalsFactory<String> stateInternalsFactory;
    private transient TimerInternalsFactory<String> timerInternalsFactory;
    private transient SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT>
        processElementInvoker;

    private transient DoFnInvoker<InputT, OutputT> invoker;

    public ProcessFn(
        DoFn<InputT, OutputT> fn,
        Coder<InputT> elementCoder,
        Coder<RestrictionT> restrictionCoder,
        WindowingStrategy<InputT, ?> inputWindowingStrategy) {
      this.fn = fn;
      this.elementCoder = elementCoder;
      this.restrictionCoder = restrictionCoder;
      this.inputWindowingStrategy = inputWindowingStrategy;
      this.elementTag =
          StateTags.value(
              "element",
              WindowedValue.getFullCoder(
                  elementCoder, inputWindowingStrategy.getWindowFn().windowCoder()));
      this.restrictionTag = StateTags.value("restriction", restrictionCoder);
    }

    public void setStateInternalsFactory(StateInternalsFactory<String> stateInternalsFactory) {
      this.stateInternalsFactory = stateInternalsFactory;
    }

    public void setTimerInternalsFactory(TimerInternalsFactory<String> timerInternalsFactory) {
      this.timerInternalsFactory = timerInternalsFactory;
    }

    public void setProcessElementInvoker(
        SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT> invoker) {
      this.processElementInvoker = invoker;
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
      return inputWindowingStrategy;
    }

    @Setup
    public void setup() throws Exception {
      invoker = DoFnInvokers.invokerFor(fn);
      invoker.invokeSetup();
    }

    @Teardown
    public void tearDown() throws Exception {
      invoker.invokeTeardown();
    }

    @StartBundle
    public void startBundle(StartBundleContext c) throws Exception {
      invoker.invokeStartBundle(wrapContextAsStartBundle(c));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      invoker.invokeFinishBundle(wrapContextAsFinishBundle(c));
    }

    @ProcessElement
    public void processElement(final ProcessContext c) {
      String key = c.element().key();
      StateInternals stateInternals =
          stateInternalsFactory.stateInternalsForKey(key);
      TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey(key);

      // Initialize state (element and restriction) depending on whether this is the seed call.
      // The seed call is the first call for this element, which actually has the element.
      // Subsequent calls are timer firings and the element has to be retrieved from the state.
      TimerInternals.TimerData timer = Iterables.getOnlyElement(c.element().timersIterable(), null);
      boolean isSeedCall = (timer == null);
      StateNamespace stateNamespace;
      if (isSeedCall) {
        WindowedValue<ElementAndRestriction<InputT, RestrictionT>> windowedValue =
            Iterables.getOnlyElement(c.element().elementsIterable());
        BoundedWindow window = Iterables.getOnlyElement(windowedValue.getWindows());
        stateNamespace =
            StateNamespaces.window(
                (Coder<BoundedWindow>) inputWindowingStrategy.getWindowFn().windowCoder(), window);
      } else {
        stateNamespace = timer.getNamespace();
      }

      ValueState<WindowedValue<InputT>> elementState =
          stateInternals.state(stateNamespace, elementTag);
      ValueState<RestrictionT> restrictionState =
          stateInternals.state(stateNamespace, restrictionTag);
      WatermarkHoldState holdState =
          stateInternals.state(stateNamespace, watermarkHoldTag);

      ElementAndRestriction<WindowedValue<InputT>, RestrictionT> elementAndRestriction;
      if (isSeedCall) {
        WindowedValue<ElementAndRestriction<InputT, RestrictionT>> windowedValue =
            Iterables.getOnlyElement(c.element().elementsIterable());
        WindowedValue<InputT> element = windowedValue.withValue(windowedValue.getValue().element());
        elementState.write(element);
        elementAndRestriction =
            ElementAndRestriction.of(element, windowedValue.getValue().restriction());
      } else {
        // This is not the first ProcessElement call for this element/restriction - rather,
        // this is a timer firing, so we need to fetch the element and restriction from state.
        elementState.readLater();
        restrictionState.readLater();
        elementAndRestriction =
            ElementAndRestriction.of(elementState.read(), restrictionState.read());
      }

      final TrackerT tracker = invoker.invokeNewTracker(elementAndRestriction.restriction());
      SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT>.Result result =
          processElementInvoker.invokeProcessElement(
              invoker, elementAndRestriction.element(), tracker);

      // Save state for resuming.
      if (result.getResidualRestriction() == null) {
        // All work for this element/restriction is completed. Clear state and release hold.
        elementState.clear();
        restrictionState.clear();
        holdState.clear();
        return;
      }
      restrictionState.write(result.getResidualRestriction());
      Instant futureOutputWatermark = result.getFutureOutputWatermark();
      if (futureOutputWatermark == null) {
        futureOutputWatermark = elementAndRestriction.element().getTimestamp();
      }
      holdState.add(futureOutputWatermark);
      // Set a timer to continue processing this element.
      timerInternals.setTimer(
          TimerInternals.TimerData.of(
              stateNamespace, timerInternals.currentProcessingTime(), TimeDomain.PROCESSING_TIME));
    }

    private DoFn<InputT, OutputT>.StartBundleContext wrapContextAsStartBundle(
        final StartBundleContext baseContext) {
      return fn.new StartBundleContext() {
        @Override
        public PipelineOptions getPipelineOptions() {
          return baseContext.getPipelineOptions();
        }

        private void throwUnsupportedOutput() {
          throw new UnsupportedOperationException(
              String.format(
                  "Splittable DoFn can only output from @%s",
                  ProcessElement.class.getSimpleName()));
        }
      };
    }

    private DoFn<InputT, OutputT>.FinishBundleContext wrapContextAsFinishBundle(
        final FinishBundleContext baseContext) {
      return fn.new FinishBundleContext() {
        @Override
        public void output(OutputT output, Instant timestamp, BoundedWindow window) {
          throwUnsupportedOutput();
        }

        @Override
        public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
          throwUnsupportedOutput();
        }

        @Override
        public PipelineOptions getPipelineOptions() {
          return baseContext.getPipelineOptions();
        }

        private void throwUnsupportedOutput() {
          throw new UnsupportedOperationException(
              String.format(
                  "Splittable DoFn can only output from @%s",
                  ProcessElement.class.getSimpleName()));
        }
      };
    }

  }

  /** Splits the restriction using the given {@link DoFn.SplitRestriction} method. */
  private static class SplitRestrictionFn<InputT, RestrictionT>
      extends DoFn<
          ElementAndRestriction<InputT, RestrictionT>,
          ElementAndRestriction<InputT, RestrictionT>> {
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
      final InputT element = c.element().element();
      invoker.invokeSplitRestriction(
          element,
          c.element().restriction(),
          new OutputReceiver<RestrictionT>() {
            @Override
            public void output(RestrictionT part) {
              c.output(ElementAndRestriction.of(element, part));
            }
          });
    }
  }
}
