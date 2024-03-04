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

import static org.apache.beam.sdk.util.construction.SplittableParDo.SPLITTABLE_PROCESS_URN;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.BaseArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.SplittableParDo.ProcessKeyedElements;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Utility transforms and overrides for running (typically unbounded) splittable DoFn's with
 * checkpointing by implementing {@link ProcessKeyedElements} using {@link KeyedWorkItem} and
 * runner-specific {@link StateInternals} and {@link TimerInternals}.
 *
 * <p>A runner that uses {@link OverrideFactory} will need to also provide runner-specific overrides
 * for {@link GBKIntoKeyedWorkItems} and {@link ProcessElements}.
 */
public class SplittableParDoViaKeyedWorkItems {
  /**
   * Runner-specific primitive {@link GroupByKey GroupByKey-like} {@link PTransform} that produces
   * {@link KeyedWorkItem KeyedWorkItems} so that downstream transforms can access state and timers.
   *
   * <p>Unlike a real {@link GroupByKey}, ignores the input's windowing and triggering strategy and
   * emits output immediately.
   */
  public static class GBKIntoKeyedWorkItems<KeyT, InputT>
      extends RawPTransform<
          PCollection<KV<KeyT, InputT>>, PCollection<KeyedWorkItem<KeyT, InputT>>> {
    @Override
    public PCollection<KeyedWorkItem<KeyT, InputT>> expand(PCollection<KV<KeyT, InputT>> input) {
      KvCoder<KeyT, InputT> kvCoder = (KvCoder<KeyT, InputT>) input.getCoder();
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          input.isBounded(),
          KeyedWorkItemCoder.of(
              kvCoder.getKeyCoder(),
              kvCoder.getValueCoder(),
              input.getWindowingStrategy().getWindowFn().windowCoder()));
    }

    @Override
    public String getUrn() {
      return SplittableParDo.SPLITTABLE_GBKIKWI_URN;
    }

    @Override
    public RunnerApi.FunctionSpec getSpec() {
      throw new UnsupportedOperationException(
          String.format("%s should never be serialized to proto", getClass().getSimpleName()));
    }
  }

  /** Overrides a {@link ProcessKeyedElements} into {@link SplittableProcessViaKeyedWorkItems}. */
  public static class OverrideFactory<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
      implements PTransformOverrideFactory<
          PCollection<KV<byte[], KV<InputT, RestrictionT>>>,
          PCollectionTuple,
          ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>> {
    @Override
    public PTransformReplacement<
            PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<byte[], KV<InputT, RestrictionT>>>,
                    PCollectionTuple,
                    ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new SplittableProcessViaKeyedWorkItems<>(transform.getTransform()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  /**
   * Runner-specific primitive {@link PTransform} that invokes the {@link DoFn.ProcessElement}
   * method for a splittable {@link DoFn}.
   */
  public static class SplittableProcessViaKeyedWorkItems<
          InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
      extends PTransform<PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple> {
    private final ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
        original;

    public SplittableProcessViaKeyedWorkItems(
        ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT> original) {
      this.original = original;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<byte[], KV<InputT, RestrictionT>>> input) {
      return input
          .apply(new GBKIntoKeyedWorkItems<>())
          .setCoder(
              KeyedWorkItemCoder.of(
                  ByteArrayCoder.of(),
                  ((KvCoder<byte[], KV<InputT, RestrictionT>>) input.getCoder()).getValueCoder(),
                  input.getWindowingStrategy().getWindowFn().windowCoder()))
          .apply(new ProcessElements<>(original));
    }
  }

  /** A primitive transform wrapping around {@link ProcessFn}. */
  public static class ProcessElements<
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      extends PTransform<
          PCollection<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple> {
    private final ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
        original;

    public ProcessElements(
        ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT> original) {
      this.original = original;
    }

    public ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
        newProcessFn(DoFn<InputT, OutputT> fn) {
      return new ProcessFn<>(
          fn,
          original.getElementCoder(),
          original.getRestrictionCoder(),
          original.getWatermarkEstimatorStateCoder(),
          original.getInputWindowingStrategy(),
          original.getSideInputMapping());
    }

    public DoFn<InputT, OutputT> getFn() {
      return original.getFn();
    }

    public List<PCollectionView<?>> getSideInputs() {
      return original.getSideInputs();
    }

    public Map<String, PCollectionView<?>> getSideInputMapping() {
      return original.getSideInputMapping();
    }

    public TupleTag<OutputT> getMainOutputTag() {
      return original.getMainOutputTag();
    }

    public TupleTagList getAdditionalOutputTags() {
      return original.getAdditionalOutputTags();
    }

    @Override
    public PCollectionTuple expand(
        PCollection<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> input) {
      return ProcessKeyedElements.createPrimitiveOutputFor(
          input,
          original.getFn(),
          original.getMainOutputTag(),
          original.getAdditionalOutputTags(),
          original.getOutputTagsToCoders(),
          original.getInputWindowingStrategy());
    }
  }

  /**
   * The heart of splittable {@link DoFn} execution: processes a single (element, restriction) pair
   * by creating a tracker for the restriction and checkpointing/resuming processing later if
   * necessary.
   *
   * <p>Takes {@link KeyedWorkItem} and assumes that the KeyedWorkItem contains a single element (or
   * a single timer set by {@link ProcessFn itself}, in a single window. This is necessary because
   * {@link ProcessFn} sets timers, and timers are namespaced to a single window and it should be
   * the window of the input element.
   *
   * <p>See also: https://github.com/apache/beam/issues/18366
   */
  @VisibleForTesting
  public static class ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      extends DoFn<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> {
    /**
     * The state cell containing a watermark hold for the output of this {@link DoFn}. The hold is
     * acquired during the first {@link DoFn.ProcessElement} call for each element and restriction,
     * and is released when the {@link DoFn.ProcessElement} call returns {@link
     * ProcessContinuation#stop()}.
     *
     * <p>A hold is needed to avoid letting the output watermark immediately progress together with
     * the input watermark when the first {@link DoFn.ProcessElement} call for this element
     * completes.
     */
    private static final StateTag<WatermarkHoldState> watermarkHoldTag =
        StateTags.makeSystemTagInternal(
            StateTags.<GlobalWindow>watermarkStateInternal("hold", TimestampCombiner.LATEST));

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

    private StateTag<ValueState<WatermarkEstimatorStateT>> watermarkEstimatorStateTag;

    private final DoFn<InputT, OutputT> fn;
    private final Coder<InputT> elementCoder;
    private final Coder<RestrictionT> restrictionCoder;
    private final WindowingStrategy<InputT, ?> inputWindowingStrategy;
    private final Map<String, PCollectionView<?>> sideInputMapping;

    private transient @Nullable StateInternalsFactory<byte[]> stateInternalsFactory;
    private transient @Nullable TimerInternalsFactory<byte[]> timerInternalsFactory;
    private transient @Nullable SideInputReader sideInputReader;
    private transient @Nullable SplittableProcessElementInvoker<
            InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
        processElementInvoker;

    private transient @Nullable DoFnInvoker<InputT, OutputT> invoker;

    public ProcessFn(
        DoFn<InputT, OutputT> fn,
        Coder<InputT> elementCoder,
        Coder<RestrictionT> restrictionCoder,
        Coder<WatermarkEstimatorStateT> watermarkEstimatorStateCoder,
        WindowingStrategy<InputT, ?> inputWindowingStrategy,
        Map<String, PCollectionView<?>> sideInputMapping) {
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
      this.watermarkEstimatorStateTag =
          StateTags.value("watermarkEstimatorState", watermarkEstimatorStateCoder);
      this.sideInputMapping = sideInputMapping;
    }

    public void setStateInternalsFactory(StateInternalsFactory<byte[]> stateInternalsFactory) {
      this.stateInternalsFactory = stateInternalsFactory;
    }

    public void setTimerInternalsFactory(TimerInternalsFactory<byte[]> timerInternalsFactory) {
      this.timerInternalsFactory = timerInternalsFactory;
    }

    public void setSideInputReader(SideInputReader sideInputReader) {
      this.sideInputReader = sideInputReader;
    }

    public void setProcessElementInvoker(
        SplittableProcessElementInvoker<
                InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
            invoker) {
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
    public void setup(PipelineOptions options) throws Exception {
      invoker = DoFnInvokers.invokerFor(fn);
      invoker.invokeSetup(wrapOptionsAsSetup(options));
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

    /**
     * Processes an element and restriction pair storing the restriction inside of state.
     *
     * <p>Uses a processing timer to resume execution if processing returns a continuation.
     *
     * <p>Uses a watermark hold to control watermark advancement.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) {
      byte[] key = c.element().key();
      StateInternals stateInternals = stateInternalsFactory.stateInternalsForKey(key);
      TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey(key);

      // Initialize state (element and restriction) depending on whether this is the seed call.
      // The seed call is the first call for this element, which actually has the element.
      // Subsequent calls are timer firings and the element has to be retrieved from the state.
      TimerInternals.TimerData timer = Iterables.getOnlyElement(c.element().timersIterable(), null);
      boolean isSeedCall = (timer == null);
      StateNamespace stateNamespace;
      if (isSeedCall) {
        WindowedValue<KV<InputT, RestrictionT>> windowedValue =
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
      ValueState<WatermarkEstimatorStateT> watermarkEstimatorState =
          stateInternals.state(stateNamespace, watermarkEstimatorStateTag);
      WatermarkHoldState holdState = stateInternals.state(stateNamespace, watermarkHoldTag);

      KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction;
      WatermarkEstimatorStateT watermarkEstimatorStateT;
      if (isSeedCall) {
        WindowedValue<KV<InputT, RestrictionT>> windowedValue =
            Iterables.getOnlyElement(c.element().elementsIterable());
        WindowedValue<InputT> element = windowedValue.withValue(windowedValue.getValue().getKey());
        elementState.write(element);
        elementAndRestriction = KV.of(element, windowedValue.getValue().getValue());
        watermarkEstimatorStateT =
            invoker.invokeGetInitialWatermarkEstimatorState(
                new BaseArgumentProvider<InputT, OutputT>() {
                  @Override
                  public InputT element(DoFn<InputT, OutputT> doFn) {
                    return elementAndRestriction.getKey().getValue();
                  }

                  @Override
                  public Object restriction() {
                    return elementAndRestriction.getValue();
                  }

                  @Override
                  public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                    return elementAndRestriction.getKey().getTimestamp();
                  }

                  @Override
                  public PipelineOptions pipelineOptions() {
                    return c.getPipelineOptions();
                  }

                  @Override
                  public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                    return elementAndRestriction.getKey().getPane();
                  }

                  @Override
                  public BoundedWindow window() {
                    return Iterables.getOnlyElement(elementAndRestriction.getKey().getWindows());
                  }

                  @Override
                  public Object sideInput(String tagId) {
                    PCollectionView<?> view = sideInputMapping.get(tagId);
                    if (view == null) {
                      throw new IllegalArgumentException(
                          "calling getSideInput() with unknown view");
                    }
                    return sideInputReader.get(
                        view, view.getWindowMappingFn().getSideInputWindow(window()));
                  }

                  @Override
                  public String getErrorContext() {
                    return ProcessFn.class.getSimpleName()
                        + ".invokeGetInitialWatermarkEstimatorState";
                  }
                });
      } else {
        // This is not the first ProcessElement call for this element/restriction - rather,
        // this is a timer firing, so we need to fetch the element and restriction from state.
        elementState.readLater();
        restrictionState.readLater();
        watermarkEstimatorState.readLater();
        elementAndRestriction = KV.of(elementState.read(), restrictionState.read());
        watermarkEstimatorStateT = watermarkEstimatorState.read();
      }

      final WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator =
          invoker.invokeNewWatermarkEstimator(
              new BaseArgumentProvider<InputT, OutputT>() {
                @Override
                public InputT element(DoFn<InputT, OutputT> doFn) {
                  return elementAndRestriction.getKey().getValue();
                }

                @Override
                public Object restriction() {
                  return elementAndRestriction.getValue();
                }

                @Override
                public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                  return elementAndRestriction.getKey().getTimestamp();
                }

                @Override
                public PipelineOptions pipelineOptions() {
                  return c.getPipelineOptions();
                }

                @Override
                public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                  return elementAndRestriction.getKey().getPane();
                }

                @Override
                public BoundedWindow window() {
                  return Iterables.getOnlyElement(elementAndRestriction.getKey().getWindows());
                }

                @Override
                public Object watermarkEstimatorState() {
                  return watermarkEstimatorStateT;
                }

                @Override
                public Object sideInput(String tagId) {
                  PCollectionView<?> view = sideInputMapping.get(tagId);
                  if (view == null) {
                    throw new IllegalArgumentException("calling getSideInput() with unknown view");
                  }
                  return sideInputReader.get(
                      view, view.getWindowMappingFn().getSideInputWindow(window()));
                }

                @Override
                public String getErrorContext() {
                  return ProcessFn.class.getSimpleName() + ".invokeNewWatermarkEstimator";
                }
              });

      final RestrictionTracker<RestrictionT, PositionT> tracker =
          invoker.invokeNewTracker(
              new BaseArgumentProvider<InputT, OutputT>() {
                @Override
                public InputT element(DoFn<InputT, OutputT> doFn) {
                  return elementAndRestriction.getKey().getValue();
                }

                @Override
                public Object restriction() {
                  return elementAndRestriction.getValue();
                }

                @Override
                public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                  return elementAndRestriction.getKey().getTimestamp();
                }

                @Override
                public PipelineOptions pipelineOptions() {
                  return c.getPipelineOptions();
                }

                @Override
                public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                  return elementAndRestriction.getKey().getPane();
                }

                @Override
                public BoundedWindow window() {
                  return Iterables.getOnlyElement(elementAndRestriction.getKey().getWindows());
                }

                @Override
                public Object sideInput(String tagId) {
                  PCollectionView<?> view = sideInputMapping.get(tagId);
                  if (view == null) {
                    throw new IllegalArgumentException("calling getSideInput() with unknown view");
                  }
                  return sideInputReader.get(
                      view, view.getWindowMappingFn().getSideInputWindow(window()));
                }

                @Override
                public String getErrorContext() {
                  return ProcessFn.class.getSimpleName() + ".invokeNewTracker";
                }
              });
      SplittableProcessElementInvoker<
                  InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
              .Result
          result =
              processElementInvoker.invokeProcessElement(
                  invoker,
                  elementAndRestriction.getKey(),
                  tracker,
                  watermarkEstimator,
                  sideInputMapping);

      // Save state for resuming.
      if (result.getResidualRestriction() == null) {
        // All work for this element/restriction is completed. Clear state and release hold.
        elementState.clear();
        restrictionState.clear();
        watermarkEstimatorState.clear();
        holdState.clear();
        return;
      }

      restrictionState.write(result.getResidualRestriction());
      watermarkEstimatorState.write(result.getFutureWatermarkEstimatorState());
      @Nullable Instant futureOutputWatermark = result.getFutureOutputWatermark();
      if (futureOutputWatermark == null) {
        futureOutputWatermark = elementAndRestriction.getKey().getTimestamp();
      }
      Instant wakeupTime =
          timerInternals.currentProcessingTime().plus(result.getContinuation().resumeDelay());
      holdState.add(futureOutputWatermark);
      // Set a timer to continue processing this element.
      timerInternals.setTimer(
          TimerInternals.TimerData.of(
              stateNamespace, wakeupTime, wakeupTime, TimeDomain.PROCESSING_TIME));
    }

    private DoFnInvoker.ArgumentProvider<InputT, OutputT> wrapOptionsAsSetup(
        final PipelineOptions options) {
      return new BaseArgumentProvider<InputT, OutputT>() {
        @Override
        public PipelineOptions pipelineOptions() {
          return options;
        }

        @Override
        public String getErrorContext() {
          return "SplittableParDoViaKeyedWorkItems/Setup";
        }
      };
    }

    private DoFnInvoker.ArgumentProvider<InputT, OutputT> wrapContextAsStartBundle(
        final StartBundleContext baseContext) {
      return new BaseArgumentProvider<InputT, OutputT>() {
        @Override
        public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
            DoFn<InputT, OutputT> doFn) {
          return fn.new StartBundleContext() {
            @Override
            public PipelineOptions getPipelineOptions() {
              return baseContext.getPipelineOptions();
            }
          };
        }

        @Override
        public PipelineOptions pipelineOptions() {
          return baseContext.getPipelineOptions();
        }

        @Override
        public String getErrorContext() {
          return "SplittableParDoViaKeyedWorkItems/StartBundle";
        }
      };
    }

    private DoFnInvoker.ArgumentProvider<InputT, OutputT> wrapContextAsFinishBundle(
        final FinishBundleContext baseContext) {
      return new BaseArgumentProvider<InputT, OutputT>() {
        @Override
        public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
            DoFn<InputT, OutputT> doFn) {
          return fn.new FinishBundleContext() {
            @Override
            public void output(OutputT output, Instant timestamp, BoundedWindow window) {
              throwUnsupportedOutput();
            }

            @Override
            public <T> void output(
                TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
              throwUnsupportedOutput();
            }

            @Override
            public PipelineOptions getPipelineOptions() {
              return baseContext.getPipelineOptions();
            }

            private void throwUnsupportedOutput() {
              throw new UnsupportedOperationException(
                  String.format(
                      "KWI Splittable DoFn can only output from @%s",
                      ProcessElement.class.getSimpleName()));
            }
          };
        }

        @Override
        public PipelineOptions pipelineOptions() {
          return baseContext.getPipelineOptions();
        }

        @Override
        public String getErrorContext() {
          return "SplittableParDoViaKeyedWorkItems/FinishBundle";
        }
      };
    }
  }

  /**
   * Registers {@link PTransformTranslation.TransformPayloadTranslator TransformPayloadTranslators}
   * for {@link Combine Combines}.
   */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @SuppressWarnings("rawtypes")
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              SplittableParDoViaKeyedWorkItems.ProcessElements.class,
              PTransformTranslation.TransformPayloadTranslator.NotSerializable.forUrn(
                  SPLITTABLE_PROCESS_URN))
          .build();
    }
  }
}
