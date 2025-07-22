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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.splittabledofn.RestrictionTrackers;
import org.apache.beam.sdk.fn.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.DelegatingArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.TimestampObservingWatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Holder;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A runner for the PTransform that truncates sized restrictions, for the case of draining a
 * pipeline.
 *
 * <p>The input and output types for this transform are
 * <li>{@code WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>}
 */
public class SplittableTruncateSizedRestrictionsDoFnRunner<
        InputT, RestrictionT extends @NonNull Object, PositionT, WatermarkEstimatorStateT, OutputT>
    implements FnApiStateAccessor.MutatingStateContext<Void, BoundedWindow> {

  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      Factory factory = new Factory();
      return ImmutableMap.<String, PTransformRunnerFactory>builder()
          .put(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN, factory)
          .build();
    }
  }

  static class Factory implements PTransformRunnerFactory {
    @Override
    public final void addRunnerForPTransform(Context context) throws IOException {
      addRunnerForTruncateSizedRestrictions(context);
    }

    private <
            InputT,
            RestrictionT extends @NonNull Object,
            PositionT,
            WatermarkEstimatorStateT,
            OutputT>
        void addRunnerForTruncateSizedRestrictions(Context context) throws IOException {

      FnApiStateAccessor<Void> stateAccessor =
          FnApiStateAccessor.Factory.<Void>factoryForPTransformContext(context).create();

      // Main output
      checkArgument(
          context.getPTransform().getOutputsMap().size() == 1,
          "TruncateSizedRestrictions expects exact one output, but got: ",
          context.getPTransform().getOutputsMap().size());
      TupleTag<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> mainOutputTag =
          new TupleTag<>(
              Iterables.getOnlyElement(context.getPTransform().getOutputsMap().keySet()));

      FnDataReceiver<
              WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>>
          mainOutputConsumer =
              context.getPCollectionConsumer(
                  context.getPTransform().getOutputsOrThrow(mainOutputTag.getId()));

      SplittableTruncateSizedRestrictionsDoFnRunner<
              InputT, RestrictionT, PositionT, WatermarkEstimatorStateT, OutputT>
          runner =
              new SplittableTruncateSizedRestrictionsDoFnRunner<>(
                  context.getPipelineOptions(),
                  context.getPTransformId(),
                  context.getPTransform(),
                  context.getComponents(),
                  mainOutputConsumer,
                  stateAccessor);

      // Register input consumer that delegates splitting
      FnDataReceiver<
              WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>>
          mainInputConsumer;

      if (mainOutputConsumer instanceof HandlesSplits) {
        mainInputConsumer =
            new SplitDelegatingFnDataReceiver<>(runner, (HandlesSplits) mainOutputConsumer);
      } else {
        mainInputConsumer = runner::processElement;
      }

      context.addPCollectionConsumer(
          context
              .getPTransform()
              .getInputsOrThrow(ParDoTranslation.getMainInputName(context.getPTransform())),
          mainInputConsumer);
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

  private final FnApiStateAccessor<?> stateAccessor;

  private final TruncateSizedRestrictionArgumentProvider mutableArgumentProvider;

  private final String pTransformId;
  private final RunnerApi.PTransform pTransform;
  private final String mainInputId;
  private final Coder<WindowedValue<?>> fullInputCoder;

  /**
   * Used to guarantee a consistent view of this {@link
   * SplittableTruncateSizedRestrictionsDoFnRunner} while setting up for {@link
   * DoFnInvoker#invokeProcessElement} since {@link #trySplit} may access internal {@link
   * SplittableTruncateSizedRestrictionsDoFnRunner} state concurrently.
   */
  // TODO: explicitly mark guarded fields with @GuardedBy
  private final Object splitLock = new Object();

  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  ///
  // Mutating fields that change with the element and window being processed
  //
  private int windowCurrentIndex;
  private @Nullable List<BoundedWindow> currentWindows;
  private @Nullable RestrictionT currentRestriction;
  private @Nullable Holder<WatermarkEstimatorStateT> currentWatermarkEstimatorState;
  private @Nullable Instant initialWatermark;
  private WatermarkEstimators.@Nullable WatermarkAndStateObserver<WatermarkEstimatorStateT>
      currentWatermarkEstimator;
  private @Nullable BoundedWindow currentWindow;
  private @Nullable RestrictionTracker<RestrictionT, PositionT> currentTracker;
  private @Nullable WindowedValue<InputT> currentElement;

  /**
   * The window index at which processing should stop. The window with this index should not be
   * processed.
   */
  private int windowStopIndex;

  /**
   * The window index which is currently being processed. This should always be less than
   * windowStopIndex.
   */
  SplittableTruncateSizedRestrictionsDoFnRunner(
      PipelineOptions pipelineOptions,
      String pTransformId,
      PTransform pTransform,
      RunnerApi.Components components,
      FnDataReceiver<
              WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>>
          mainOutputConsumer,
      FnApiStateAccessor<Void> stateAccessor)
      throws IOException {
    this.pipelineOptions = pipelineOptions;
    this.stateAccessor = stateAccessor;
    this.pTransformId = pTransformId;
    this.pTransform = pTransform;

    ParDoPayload parDoPayload = ParDoPayload.parseFrom(pTransform.getSpec().getPayload());

    // DoFn and metadata
    DoFn<InputT, OutputT> doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(parDoPayload);
    DoFnSignature doFnSignature = DoFnSignatures.signatureForDoFn(doFn);
    this.doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn, pipelineOptions);
    this.doFnSchemaInformation = ParDoTranslation.getSchemaInformation(parDoPayload);

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
      this.mutableArgumentProvider = new TruncateSizedRestrictionWindowObservingArgumentProvider();
    } else {
      this.mutableArgumentProvider =
          new TruncateSizedRestrictionNonWindowObservingArgumentProvider();
    }

    // Main Input
    this.mainInputId = ParDoTranslation.getMainInputName(pTransform);
    RunnerApi.PCollection mainInput =
        components.getPcollectionsOrThrow(pTransform.getInputsOrThrow(mainInputId));
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components).withPipeline(Pipeline.create());
    Coder<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> inputCoder =
        (Coder<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>)
            rehydratedComponents.getCoder(mainInput.getCoderId());
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>)
            rehydratedComponents
                .getWindowingStrategy(mainInput.getWindowingStrategyId())
                .getWindowFn()
                .windowCoder();
    this.fullInputCoder =
        (Coder<WindowedValue<?>>) (Object) WindowedValues.getFullCoder(inputCoder, windowCoder);
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public BoundedWindow getCurrentWindow() {
    return checkStateNotNull(
        currentWindow, "Attempt to access window outside windowed element processing context.");
  }

  public List<BoundedWindow> getCurrentWindows() {
    return checkStateNotNull(
        currentWindows,
        "Attempt to access window collection outside windowed element processing context.");
  }

  public WindowedValue<InputT> getCurrentElement() {
    return checkStateNotNull(
        currentElement, "Attempt to access element outside element processing context.");
  }

  private RestrictionT getCurrentRestriction() {
    return checkStateNotNull(
        this.currentRestriction,
        "Attempt to access restriction outside element processing context.");
  }

  private RestrictionTracker<RestrictionT, ?> getCurrentTracker() {
    return checkStateNotNull(
        this.currentTracker,
        "Attempt to access restriction tracker state outside element processing context.");
  }

  private WatermarkEstimatorStateT getCurrentWatermarkEstimatorState() {
    checkStateNotNull(
        this.currentWatermarkEstimatorState,
        "Attempt to access watermark estimator state outside element processing context.");
    return this.currentWatermarkEstimatorState.get();
  }

  void processElement(
      WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> elem) {
    if (observesWindow) {
      processElementForWindowObservingTruncateRestriction(elem);
    } else {
      processElementForTruncateRestriction(elem);
    }
  }

  HandlesSplits.@Nullable SplitResult trySplit(
      double fractionOfRemainder, HandlesSplits splitDelegate) {
    if (observesWindow) {
      return trySplitForWindowObservingTruncateRestriction(fractionOfRemainder, splitDelegate);
    } else {
      return splitDelegate.trySplit(fractionOfRemainder);
    }
  }

  private void processElementForTruncateRestriction(
      WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> elem) {
    currentElement = elem.withValue(elem.getValue().getKey().getKey());
    currentRestriction = elem.getValue().getKey().getValue().getKey();
    currentWatermarkEstimatorState = Holder.of(elem.getValue().getKey().getValue().getValue());
    currentTracker =
        RestrictionTrackers.synchronize(doFnInvoker.invokeNewTracker(mutableArgumentProvider));
    try {
      TruncateResult<RestrictionT> truncatedRestriction =
          doFnInvoker.invokeTruncateRestriction(mutableArgumentProvider);
      if (truncatedRestriction != null) {
        mutableArgumentProvider.output(truncatedRestriction.getTruncatedRestriction());
      }
    } finally {
      currentTracker = null;
      currentElement = null;
      currentRestriction = null;
      currentWatermarkEstimatorState = null;
    }

    this.stateAccessor.finalizeState();
  }

  private void processElementForWindowObservingTruncateRestriction(
      WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> elem) {
    currentElement = elem.withValue(elem.getValue().getKey().getKey());
    windowCurrentIndex = -1;
    windowStopIndex = elem.getWindows().size();
    currentWindows = ImmutableList.copyOf(elem.getWindows());
    while (true) {
      synchronized (splitLock) {
        windowCurrentIndex++;
        if (windowCurrentIndex >= windowStopIndex) {
          // Careful to reset the split state under the same synchronized block.
          windowCurrentIndex = -1;
          windowStopIndex = 0;
          currentElement = null;
          currentWindows = null;
          currentRestriction = null;
          currentWatermarkEstimatorState = null;
          currentWindow = null;
          currentTracker = null;
          currentWatermarkEstimator = null;
          initialWatermark = null;
          break;
        }
        currentRestriction = elem.getValue().getKey().getValue().getKey();
        currentWatermarkEstimatorState = Holder.of(elem.getValue().getKey().getValue().getValue());
        currentWindow =
            checkStateNotNull(
                    currentWindows,
                    "internal error: currentWindows is null during element processing")
                .get(windowCurrentIndex);
        currentTracker =
            RestrictionTrackers.synchronize(doFnInvoker.invokeNewTracker(mutableArgumentProvider));
        currentWatermarkEstimator =
            WatermarkEstimators.threadSafe(
                doFnInvoker.invokeNewWatermarkEstimator(mutableArgumentProvider));
        initialWatermark = currentWatermarkEstimator.getWatermarkAndState().getKey();
      }
      TruncateResult<RestrictionT> truncatedRestriction =
          doFnInvoker.invokeTruncateRestriction(mutableArgumentProvider);
      if (truncatedRestriction != null) {
        mutableArgumentProvider.output(truncatedRestriction.getTruncatedRestriction());
      }
    }
    this.stateAccessor.finalizeState();
  }

  private @Nullable Progress getProgressFromWindowObservingTruncate(double elementCompleted) {
    synchronized (splitLock) {
      if (currentWindow != null) {
        return ProgressUtils.scaleProgress(
            Progress.from(elementCompleted, 1 - elementCompleted),
            windowCurrentIndex,
            windowStopIndex);
      }
    }
    return null;
  }

  private WindowedSplitResult calculateRestrictionSize(
      WindowedSplitResult splitResult, String errorContext) {
    double fullSize =
        splitResult.getResidualInUnprocessedWindowsRoot() == null
                && splitResult.getPrimaryInFullyProcessedWindowsRoot() == null
            ? 0
            : doFnInvoker.invokeGetSize(
                new DelegatingArgumentProvider<InputT, OutputT>(
                    mutableArgumentProvider, errorContext) {
                  @Override
                  public Object restriction() {
                    return getCurrentRestriction();
                  }

                  @Override
                  public RestrictionTracker<?, ?> restrictionTracker() {
                    return doFnInvoker.invokeNewTracker(this);
                  }
                });
    double primarySize =
        splitResult.getPrimarySplitRoot() == null
            ? 0
            : doFnInvoker.invokeGetSize(
                new DelegatingArgumentProvider<InputT, OutputT>(
                    mutableArgumentProvider, errorContext) {
                  @Override
                  public Object restriction() {
                    WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>
                        splitRoot =
                            (WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>)
                                splitResult.getPrimarySplitRoot();

                    return splitRoot.getValue().getValue().getKey();
                  }

                  @Override
                  public RestrictionTracker<?, ?> restrictionTracker() {
                    return doFnInvoker.invokeNewTracker(this);
                  }
                });
    double residualSize =
        splitResult.getResidualSplitRoot() == null
            ? 0
            : doFnInvoker.invokeGetSize(
                new DelegatingArgumentProvider<InputT, OutputT>(
                    mutableArgumentProvider, errorContext) {
                  @Override
                  public Object restriction() {
                    WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>
                        splitRoot =
                            (WindowedValue<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>>)
                                splitResult.getResidualSplitRoot();

                    return splitRoot.getValue().getValue().getKey();
                  }

                  @Override
                  public RestrictionTracker<?, ?> restrictionTracker() {
                    return doFnInvoker.invokeNewTracker(this);
                  }
                });
    return WindowedSplitResult.forRoots(
        splitResult.getPrimaryInFullyProcessedWindowsRoot() == null
            ? null
            : WindowedValues.of(
                KV.of(splitResult.getPrimaryInFullyProcessedWindowsRoot().getValue(), fullSize),
                splitResult.getPrimaryInFullyProcessedWindowsRoot().getTimestamp(),
                splitResult.getPrimaryInFullyProcessedWindowsRoot().getWindows(),
                splitResult.getPrimaryInFullyProcessedWindowsRoot().getPaneInfo()),
        splitResult.getPrimarySplitRoot() == null
            ? null
            : WindowedValues.of(
                KV.of(splitResult.getPrimarySplitRoot().getValue(), primarySize),
                splitResult.getPrimarySplitRoot().getTimestamp(),
                splitResult.getPrimarySplitRoot().getWindows(),
                splitResult.getPrimarySplitRoot().getPaneInfo()),
        splitResult.getResidualSplitRoot() == null
            ? null
            : WindowedValues.of(
                KV.of(splitResult.getResidualSplitRoot().getValue(), residualSize),
                splitResult.getResidualSplitRoot().getTimestamp(),
                splitResult.getResidualSplitRoot().getWindows(),
                splitResult.getResidualSplitRoot().getPaneInfo()),
        splitResult.getResidualInUnprocessedWindowsRoot() == null
            ? null
            : WindowedValues.of(
                KV.of(splitResult.getResidualInUnprocessedWindowsRoot().getValue(), fullSize),
                splitResult.getResidualInUnprocessedWindowsRoot().getTimestamp(),
                splitResult.getResidualInUnprocessedWindowsRoot().getWindows(),
                splitResult.getResidualInUnprocessedWindowsRoot().getPaneInfo()));
  }

  private HandlesSplits.@Nullable SplitResult trySplitForWindowObservingTruncateRestriction(
      double fractionOfRemainder, HandlesSplits splitDelegate) {
    WindowedSplitResult windowedSplitResult;
    HandlesSplits.SplitResult downstreamSplitResult;
    synchronized (splitLock) {
      // There is nothing to split if we are between truncate processing calls.
      if (currentWindow == null) {
        return null;
      }

      SplitResultsWithStopIndex splitResult =
          computeSplitForTruncate(
              getCurrentElement(),
              getCurrentRestriction(),
              getCurrentWindow(),
              getCurrentWindows(),
              getCurrentWatermarkEstimatorState(),
              fractionOfRemainder,
              splitDelegate,
              windowCurrentIndex,
              windowStopIndex);
      if (splitResult == null) {
        return null;
      }
      windowStopIndex = splitResult.getNewWindowStopIndex();
      windowedSplitResult =
          calculateRestrictionSize(
              splitResult.getWindowSplit(),
              PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN + "/GetSize");
      downstreamSplitResult = splitResult.getDownstreamSplit();
    }
    return constructSplitResult(
        windowedSplitResult,
        downstreamSplitResult,
        fullInputCoder,
        checkStateNotNull(
            initialWatermark, "Attempt to construct split result without initial watermark"),
        pTransformId,
        mainInputId,
        pTransform.getOutputsMap().keySet());
  }

  private static <WatermarkEstimatorStateT> WindowedSplitResult computeWindowSplitResult(
      WindowedValue<?> currentElement,
      Object currentRestriction,
      List<BoundedWindow> windows,
      WatermarkEstimatorStateT currentWatermarkEstimatorState,
      int toIndex,
      int fromIndex,
      int stopWindowIndex) {
    List<BoundedWindow> primaryFullyProcessedWindows = windows.subList(0, toIndex);
    List<BoundedWindow> residualUnprocessedWindows = windows.subList(fromIndex, stopWindowIndex);
    WindowedSplitResult windowedSplitResult;

    windowedSplitResult =
        WindowedSplitResult.forRoots(
            primaryFullyProcessedWindows.isEmpty()
                ? null
                : WindowedValues.of(
                    KV.of(
                        currentElement.getValue(),
                        KV.of(currentRestriction, currentWatermarkEstimatorState)),
                    currentElement.getTimestamp(),
                    primaryFullyProcessedWindows,
                    currentElement.getPaneInfo()),
            null,
            null,
            residualUnprocessedWindows.isEmpty()
                ? null
                : WindowedValues.of(
                    KV.of(
                        currentElement.getValue(),
                        KV.of(currentRestriction, currentWatermarkEstimatorState)),
                    currentElement.getTimestamp(),
                    residualUnprocessedWindows,
                    currentElement.getPaneInfo()));
    return windowedSplitResult;
  }

  @VisibleForTesting
  static <WatermarkEstimatorStateT> @Nullable SplitResultsWithStopIndex computeSplitForTruncate(
      WindowedValue<?> element,
      Object restriction,
      BoundedWindow window,
      List<BoundedWindow> windows,
      WatermarkEstimatorStateT watermarkEstimatorState,
      double fractionOfRemainder,
      HandlesSplits splitDelegate,
      int currentWindowIndex,
      int stopWindowIndex) {
    checkArgument(splitDelegate != null);

    WindowedSplitResult windowedSplitResult;
    HandlesSplits.@Nullable SplitResult downstreamSplitResult = null;
    int newWindowStopIndex;
    // If we are not on the last window, try to compute the split which is on the current window or
    // on a future window.
    if (currentWindowIndex != stopWindowIndex - 1) {
      // Compute the fraction of the remainder relative to the scaled progress.
      Progress elementProgress;
      double elementCompleted = splitDelegate.getProgress();
      elementProgress = Progress.from(elementCompleted, 1 - elementCompleted);
      Progress scaledProgress =
          ProgressUtils.scaleProgress(elementProgress, currentWindowIndex, stopWindowIndex);
      double scaledFractionOfRemainder = scaledProgress.getWorkRemaining() * fractionOfRemainder;

      // The fraction is out of the current window and hence we will split at the closest window
      // boundary.
      if (scaledFractionOfRemainder >= elementProgress.getWorkRemaining()) {
        newWindowStopIndex =
            (int)
                Math.min(
                    stopWindowIndex - 1,
                    currentWindowIndex
                        + Math.max(
                            1,
                            Math.round(
                                (elementProgress.getWorkCompleted() + scaledFractionOfRemainder)
                                    / (elementProgress.getWorkCompleted()
                                        + elementProgress.getWorkRemaining()))));
        windowedSplitResult =
            computeWindowSplitResult(
                element,
                restriction,
                windows,
                watermarkEstimatorState,
                newWindowStopIndex,
                newWindowStopIndex,
                stopWindowIndex);
      } else {
        // Compute the element split with the scaled fraction.
        downstreamSplitResult = splitDelegate.trySplit(scaledFractionOfRemainder);
        newWindowStopIndex = currentWindowIndex + 1;
        int toIndex = (downstreamSplitResult == null) ? newWindowStopIndex : currentWindowIndex;
        windowedSplitResult =
            computeWindowSplitResult(
                element,
                restriction,
                windows,
                watermarkEstimatorState,
                toIndex,
                newWindowStopIndex,
                stopWindowIndex);
      }
    } else {
      // We are on the last window then compute the element split with given fraction.
      newWindowStopIndex = stopWindowIndex;
      downstreamSplitResult = splitDelegate.trySplit(fractionOfRemainder);
      if (downstreamSplitResult == null) {
        return null;
      }
      windowedSplitResult =
          computeWindowSplitResult(
              element,
              restriction,
              windows,
              watermarkEstimatorState,
              currentWindowIndex,
              stopWindowIndex,
              stopWindowIndex);
    }
    return SplitResultsWithStopIndex.of(
        windowedSplitResult, downstreamSplitResult, newWindowStopIndex);
  }

  @VisibleForTesting
  static <WatermarkEstimatorStateT> HandlesSplits.SplitResult constructSplitResult(
      @Nullable WindowedSplitResult windowedSplitResult,
      HandlesSplits.@Nullable SplitResult downstreamElementSplit,
      Coder<WindowedValue<?>> fullInputCoder,
      Instant initialWatermark,
      String pTransformId,
      String mainInputId,
      Collection<String> outputIds) {
    // The element split cannot from both windowedSplitResult and downstreamElementSplit.
    checkArgument(
        (windowedSplitResult == null || windowedSplitResult.getResidualSplitRoot() == null)
            || downstreamElementSplit == null);
    List<BundleApplication> primaryRoots = new ArrayList<>();
    List<DelayedBundleApplication> residualRoots = new ArrayList<>();

    // Encode window splits.
    if (windowedSplitResult != null
        && windowedSplitResult.getPrimaryInFullyProcessedWindowsRoot() != null) {
      ByteStringOutputStream primaryInOtherWindowsBytes = new ByteStringOutputStream();
      try {
        fullInputCoder.encode(
            windowedSplitResult.getPrimaryInFullyProcessedWindowsRoot(),
            primaryInOtherWindowsBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      BundleApplication.Builder primaryApplicationInOtherWindows =
          BundleApplication.newBuilder()
              .setTransformId(pTransformId)
              .setInputId(mainInputId)
              .setElement(primaryInOtherWindowsBytes.toByteString());
      primaryRoots.add(primaryApplicationInOtherWindows.build());
    }
    if (windowedSplitResult != null
        && windowedSplitResult.getResidualInUnprocessedWindowsRoot() != null) {
      ByteStringOutputStream bytesOut = new ByteStringOutputStream();
      try {
        fullInputCoder.encode(windowedSplitResult.getResidualInUnprocessedWindowsRoot(), bytesOut);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      BundleApplication.Builder residualInUnprocessedWindowsRoot =
          BundleApplication.newBuilder()
              .setTransformId(pTransformId)
              .setInputId(mainInputId)
              .setElement(bytesOut.toByteString());
      // We don't want to change the output watermarks or set the checkpoint resume time since
      // that applies to the current window.
      Map<String, org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Timestamp>
          outputWatermarkMapForUnprocessedWindows = new HashMap<>();
      if (!initialWatermark.equals(GlobalWindow.TIMESTAMP_MIN_VALUE)) {
        org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Timestamp outputWatermark =
            org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(initialWatermark.getMillis() / 1000)
                .setNanos((int) (initialWatermark.getMillis() % 1000) * 1000000)
                .build();
        for (String outputId : outputIds) {
          outputWatermarkMapForUnprocessedWindows.put(outputId, outputWatermark);
        }
      }
      residualInUnprocessedWindowsRoot.putAllOutputWatermarks(
          outputWatermarkMapForUnprocessedWindows);
      residualRoots.add(
          DelayedBundleApplication.newBuilder()
              .setApplication(residualInUnprocessedWindowsRoot)
              .build());
    }

    // Encode element split from windowedSplitResult or from downstream element split. It's possible
    // that there is no element split.
    if (downstreamElementSplit != null) {
      primaryRoots.add(Iterables.getOnlyElement(downstreamElementSplit.getPrimaryRoots()));
      residualRoots.add(Iterables.getOnlyElement(downstreamElementSplit.getResidualRoots()));
    }

    return HandlesSplits.SplitResult.of(primaryRoots, residualRoots);
  }

  /** Outputs the given element to the specified set of consumers wrapping any exceptions. */
  private <T> void outputTo(FnDataReceiver<WindowedValue<T>> consumer, WindowedValue<T> output) {
    if (currentWatermarkEstimator instanceof TimestampObservingWatermarkEstimator) {
      ((TimestampObservingWatermarkEstimator) currentWatermarkEstimator)
          .observeTimestamp(output.getTimestamp());
    }
    try {
      consumer.accept(output);
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  private void tearDown() {
    doFnInvoker.invokeTeardown();
  }

  /** This context outputs KV<KV<Element, KV<Restriction, WatemarkEstimatorState>>, Size>. */
  private class TruncateSizedRestrictionWindowObservingArgumentProvider
      extends TruncateSizedRestrictionArgumentProvider {

    @Override
    public void output(RestrictionT output) {
      double size = getSize(output);
      outputTo(
          mainOutputConsumer,
          WindowedValues.of(
              KV.of(
                  KV.of(
                      getCurrentElement().getValue(),
                      KV.of(output, getCurrentWatermarkEstimatorState())),
                  size),
              getCurrentElement().getTimestamp(),
              getCurrentWindow(),
              getCurrentElement().getPaneInfo()));
    }

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
  }

  /** This context outputs KV<KV<Element, KV<Restriction, WatermarkEstimatorState>>, Size>. */
  private class TruncateSizedRestrictionNonWindowObservingArgumentProvider
      extends TruncateSizedRestrictionArgumentProvider {

    @Override
    public void output(RestrictionT truncatedRestriction) {
      double size = getSize(truncatedRestriction);
      outputTo(
          mainOutputConsumer,
          getCurrentElement()
              .withValue(
                  KV.of(
                      KV.of(
                          getCurrentElement().getValue(),
                          KV.of(truncatedRestriction, getCurrentWatermarkEstimatorState())),
                      size)));
    }
  }

  /** Base implementation that does not override methods which need to be window aware. */
  private abstract class TruncateSizedRestrictionArgumentProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT>
      implements OutputReceiver<RestrictionT> {

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
    public String getErrorContext() {
      return "TruncateRestriction";
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
      // OutputT == RestrictionT
      return (OutputReceiver<OutputT>) this;
    }

    @Override
    public Object restriction() {
      return getCurrentRestriction();
    }

    @Override
    @SuppressWarnings("nullness")
    public Object watermarkEstimatorState() {
      return getCurrentWatermarkEstimatorState();
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
      throw new UnsupportedOperationException(
          "Cannot outputWithTimestamp from TruncateRestriction");
    }
  }

  /**
   * Passes split requests downstream, by way of trySplit on the overall runner, which will split
   * per window if needed.
   */
  private static class SplitDelegatingFnDataReceiver<
          InputT, RestrictionT extends @NonNull Object, WatermarkEstimatorStateT>
      implements FnDataReceiver<
              WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>>>,
          HandlesSplits {

    private final HandlesSplits splitDelegate;
    private final SplittableTruncateSizedRestrictionsDoFnRunner<
            InputT, RestrictionT, ?, WatermarkEstimatorStateT, ?>
        runner;

    public SplitDelegatingFnDataReceiver(
        SplittableTruncateSizedRestrictionsDoFnRunner<
                InputT, RestrictionT, ?, WatermarkEstimatorStateT, ?>
            runner,
        HandlesSplits splitDelegate) {
      this.runner = runner;
      this.splitDelegate = splitDelegate;
    }

    @Override
    public void accept(
        WindowedValue<KV<KV<InputT, KV<RestrictionT, WatermarkEstimatorStateT>>, Double>> input) {
      runner.processElement(input);
    }

    @Override
    public @Nullable SplitResult trySplit(double fractionOfRemainder) {
      return runner.trySplit(fractionOfRemainder, splitDelegate);
    }

    @Override
    public double getProgress() {
      double delegateProgress = splitDelegate.getProgress();
      if (!runner.observesWindow) {
        return delegateProgress;
      } else {
        Progress progress = runner.getProgressFromWindowObservingTruncate(delegateProgress);
        if (progress != null) {
          double totalWork = progress.getWorkCompleted() + progress.getWorkRemaining();
          if (totalWork > 0) {
            return progress.getWorkCompleted() / totalWork;
          }
        }
        return 0;
      }
    }
  }
}
