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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.DelegatingArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A runner for the PTransform that takes restrictions and splits them, outputting
 *
 * <p>This is meant to consume the output of {@link SplittablePairWithRestrictionDoFnRunner}.
 *
 * <p>The DoFn will have type DoFn<InputT, OutputT> but this transform's Fn API input and output
 * types are:
 *
 * <ul>
 *   <li>Input: {@code WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>}
 *   <li>Output: {@code WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>,
 *       Double>>}
 * </ul>
 *
 * <p>In addition to this, it passes {@Code OutputReceiver<RestrictionT>} to the DoFn GetRestriction
 * method.
 */
@Internal
public class SplittableSplitAndSizeRestrictionsDoFnRunner<
        InputT, RestrictionT extends @NonNull Object, PositionT, WatermarkEstimatorStateT, OutputT>
    implements FnApiStateAccessor.MutatingStateContext<Void, BoundedWindow> {

  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.<String, PTransformRunnerFactory>builder()
          .put(PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN, new Factory())
          .build();
    }
  }

  static class Factory implements PTransformRunnerFactory {

    @Override
    public final void addRunnerForPTransform(Context context) throws IOException {
      addRunnerForSplitAndSizeRestriction(context);
    }

    private <
            InputT,
            RestrictionT extends @NonNull Object,
            PositionT,
            WatermarkEstimatorStateT,
            OutputT>
        void addRunnerForSplitAndSizeRestriction(Context context) throws IOException {

      FnApiStateAccessor<Void> stateAccessor =
          FnApiStateAccessor.Factory.<Void>factoryForPTransformContext(context).create();

      SplittableSplitAndSizeRestrictionsDoFnRunner<
              InputT, RestrictionT, PositionT, WatermarkEstimatorStateT, OutputT>
          runner =
              new SplittableSplitAndSizeRestrictionsDoFnRunner<>(
                  context.getPipelineOptions(),
                  context.getPTransform(),
                  context.getComponents(),
                  context::getPCollectionConsumer,
                  context.getBundleFinalizer(),
                  stateAccessor);

      stateAccessor.setKeyAndWindowContext(runner);

      context.addPCollectionConsumer(
          context
              .getPTransform()
              .getInputsOrThrow(ParDoTranslation.getMainInputName(context.getPTransform())),
          runner::processElement);
      context.addTearDownFunction(runner::tearDown);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final boolean observesWindow;
  private final PipelineOptions pipelineOptions;

  private final DoFnInvoker<InputT, OutputT> doFnInvoker;

  private final FnDataReceiver<
          WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>>
      mainOutputConsumer;

  private final FnApiStateAccessor<Void> stateAccessor;
  private final SplitRestrictionArgumentProvider mutableArgumentProvider;

  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  private @Nullable RestrictionT currentRestriction = null;
  private @Nullable WatermarkEstimatorStateT currentWatermarkEstimatorState = null;
  private @Nullable RestrictionTracker<RestrictionT, PositionT> currentTracker;
  private @Nullable WindowedValue<InputT> currentElement;
  private @Nullable BoundedWindow currentWindow;

  SplittableSplitAndSizeRestrictionsDoFnRunner(
      PipelineOptions pipelineOptions,
      PTransform pTransform,
      RunnerApi.Components components,
      Function<String, FnDataReceiver<WindowedValue<?>>> getPCollectionConsumer,
      BundleFinalizer bundleFinalizer,
      FnApiStateAccessor<Void> stateAccessor)
      throws IOException {
    this.pipelineOptions = pipelineOptions;

    ParDoPayload parDoPayload = ParDoPayload.parseFrom(pTransform.getSpec().getPayload());

    // DoFn and metadata
    DoFn<InputT, OutputT> doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(parDoPayload);
    DoFnSignature doFnSignature = DoFnSignatures.signatureForDoFn(doFn);
    this.doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn, pipelineOptions);
    this.doFnSchemaInformation = ParDoTranslation.getSchemaInformation(parDoPayload);

    // Main output
    checkArgument(
        pTransform.getOutputsMap().size() == 1,
        "SplitAndSizeRestrictions expects exact one output, but got: ",
        pTransform.getOutputsMap().size());
    TupleTag<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> mainOutputTag =
        new TupleTag<>(Iterables.getOnlyElement(pTransform.getOutputsMap().keySet()));
    @SuppressWarnings("rawtypes") // cannot do this multi-level cast without rawtypes
    FnDataReceiver<
            WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>>
        mainOutputConsumer =
            (FnDataReceiver)
                getPCollectionConsumer.apply(pTransform.getOutputsOrThrow(mainOutputTag.getId()));
    this.mainOutputConsumer = mainOutputConsumer;

    // Side inputs
    this.sideInputMapping = ParDoTranslation.getSideInputMapping(parDoPayload);

    // Register processing methods
    this.observesWindow =
        (doFnSignature.splitRestriction() != null
                && doFnSignature.splitRestriction().observesWindow())
            || (doFnSignature.newTracker() != null && doFnSignature.newTracker().observesWindow())
            || (doFnSignature.getSize() != null && doFnSignature.getSize().observesWindow())
            || !sideInputMapping.isEmpty();

    if (observesWindow) {
      this.mutableArgumentProvider = new SizedRestrictionWindowObservingArgumentProvider();
    } else {
      this.mutableArgumentProvider = new SizedRestrictionNonWindowObservingArgumentProvider();
    }

    this.stateAccessor = stateAccessor;
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public BoundedWindow getCurrentWindow() {
    return checkStateNotNull(
        this.currentWindow,
        "Attempt to access window outside windowed element processing context.");
  }

  private WindowedValue<InputT> getCurrentElement() {
    return checkStateNotNull(
        this.currentElement, "Attempt to access element outside element processing context.");
  }

  private Object getCurrentRestriction() {
    return checkStateNotNull(
        this.currentRestriction,
        "Attempt to access restriction outside element processing context.");
  }

  // Because WatermarkEstimatorStateT may allow nulls, we cannot use checkStateNotNull to ensure we
  // are in an element processing context.
  //
  // But because it may _not_ accept nulls, we cannot safely return currentWatermarkEstimatorState
  // without checking for null.
  //
  // There is no solution for the type of currentWatermarkEstimatorState; we would have to introduce
  // a "MutableOptional" to hold the present-or-absent value. Ultimately, the root cause is the
  // antipattern
  // of a class where fields are mutated to non-null then back to null, creating sensitive state
  // machine
  // invariants between methods.
  @SuppressWarnings("nullness")
  private WatermarkEstimatorStateT getCurrentWatermarkEstimatorState() {
    checkStateNotNull(
        this.currentElement,
        "Attempt to access watermark estimator state outside element processing context.");
    return this.currentWatermarkEstimatorState;
  }

  private RestrictionTracker<RestrictionT, ?> getCurrentTracker() {
    return checkStateNotNull(
        this.currentTracker,
        "Attempt to access restriction tracker state outside element processing context.");
  }

  void processElement(WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>> elem) {
    if (observesWindow) {
      processElementForWindowObservingSplitRestriction(elem);
    } else {
      processElementForSplitRestriction(elem);
    }
  }

  private void processElementForSplitRestriction(
      WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>> elem) {
    currentElement = elem.withValue(elem.getValue().getKey());
    currentRestriction = elem.getValue().getValue().getKey();
    currentWatermarkEstimatorState = elem.getValue().getValue().getValue();
    currentTracker = doFnInvoker.invokeNewTracker(mutableArgumentProvider);
    try {
      doFnInvoker.invokeSplitRestriction(mutableArgumentProvider);
    } finally {
      currentElement = null;
      currentRestriction = null;
      currentWatermarkEstimatorState = null;
      currentTracker = null;
    }

    this.stateAccessor.finalizeState();
  }

  private void processElementForWindowObservingSplitRestriction(
      WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>> elem) {
    currentElement = elem.withValue(elem.getValue().getKey());
    currentRestriction = elem.getValue().getValue().getKey();
    currentWatermarkEstimatorState = elem.getValue().getValue().getValue();
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        currentTracker = doFnInvoker.invokeNewTracker(mutableArgumentProvider);
        doFnInvoker.invokeSplitRestriction(mutableArgumentProvider);
      }
    } finally {
      currentElement = null;
      currentRestriction = null;
      currentWatermarkEstimatorState = null;
      currentWindow = null;
      currentTracker = null;
    }

    this.stateAccessor.finalizeState();
  }

  private void tearDown() {
    doFnInvoker.invokeTeardown();
  }

  /** Outputs the given element to the specified set of consumers wrapping any exceptions. */
  private <T> void outputTo(FnDataReceiver<WindowedValue<T>> consumer, WindowedValue<T> output) {
    try {
      consumer.accept(output);
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  /** Provides arguments for a {@link DoFnInvoker} for a window observing method. */
  private class SizedRestrictionWindowObservingArgumentProvider
      extends SplitRestrictionArgumentProvider {
    @Override
    public BoundedWindow window() {
      return getCurrentWindow();
    }

    @Override
    public Object sideInput(String tagId) {
      PCollectionView<Object> pCollectionView =
          (PCollectionView<Object>)
              checkStateNotNull(sideInputMapping.get(tagId), "Side input tag not found: %s", tagId);

      return stateAccessor.get(pCollectionView, getCurrentWindow());
    }

    @Override
    public void output(RestrictionT subrestriction) {
      // This OutputReceiver is only for being passed to SplitRestriction OutputT == RestrictionT
      double size = getSize(subrestriction);

      // Don't need to check timestamp since we can always output using the input timestamp.
      outputTo(
          mainOutputConsumer,
          WindowedValues.of(
              KV.of(
                  KV.of(
                      getCurrentElement().getValue(),
                      KV.of(subrestriction, getCurrentWatermarkEstimatorState())),
                  size),
              getCurrentElement().getTimestamp(),
              getCurrentWindow(),
              getCurrentElement().getPaneInfo()));
    }
  }

  /** This context outputs KV<KV<Element, KV<Restriction, WatermarkEstimatorState>>, Size>. */
  private class SizedRestrictionNonWindowObservingArgumentProvider
      extends SplitRestrictionArgumentProvider {
    @Override
    public void output(RestrictionT subrestriction) {
      double size = getSize(subrestriction);

      // Don't need to check timestamp since we can always output using the input timestamp.
      outputTo(
          mainOutputConsumer,
          getCurrentElement()
              .withValue(
                  KV.of(
                      KV.of(
                          getCurrentElement().getValue(),
                          KV.of(subrestriction, getCurrentWatermarkEstimatorState())),
                      size)));
    }
  }

  /**
   * Base implementation of {@link DoFnInvoker.ArgumentProvider} that provides all methods that do
   * not matter for being window aware or not.
   */
  private abstract class SplitRestrictionArgumentProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT>
      implements OutputReceiver<RestrictionT> {

    @Override
    public String getErrorContext() {
      return "SplitAndSizeRestriction";
    }

    protected double getSize(RestrictionT subrestriction) {
      return doFnInvoker.invokeGetSize(
          new DelegatingArgumentProvider<InputT, OutputT>(this, getErrorContext() + "/GetSize") {
            @Override
            public Object restriction() {
              return subrestriction;
            }

            @Override
            public Instant timestamp(DoFn<InputT, OutputT> doFn) {
              return getCurrentElement().getTimestamp();
            }

            @Override
            public RestrictionTracker<?, ?> restrictionTracker() {
              return doFnInvoker.invokeNewTracker(this);
            }
          });
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return getCurrentElement().getPaneInfo();
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return getCurrentElement().getValue();
    }

    @Override
    public Object schemaElement(int index) {
      SerializableFunction<InputT, Object> converter =
          (SerializableFunction<InputT, Object>)
              doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(getCurrentElement().getValue());
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return getCurrentElement().getTimestamp();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      // This is only to be used for passing to SplitRestriction method, where
      // the expected output type is RestrictionT
      return (OutputReceiver<OutputT>) this;
    }

    @Override
    public Object restriction() {
      return getCurrentRestriction();
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      return getCurrentTracker();
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public void outputWithTimestamp(RestrictionT output, Instant timestamp) {
      throw new UnsupportedOperationException("Cannot outputWithTimestamp from SplitRestriction");
    }
  }
}
