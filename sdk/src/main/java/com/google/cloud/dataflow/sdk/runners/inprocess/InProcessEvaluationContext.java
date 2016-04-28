/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.inprocess.GroupByKeyEvaluatorFactory.InProcessGroupByKeyOnly;
import com.google.cloud.dataflow.sdk.runners.inprocess.InMemoryWatermarkManager.FiredTimers;
import com.google.cloud.dataflow.sdk.runners.inprocess.InMemoryWatermarkManager.TransformWatermarks;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.PCollectionViewWriter;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

/**
 * The evaluation context for a specific pipeline being executed by the
 * {@link InProcessPipelineRunner}. Contains state shared within the execution across all
 * transforms.
 *
 * <p>{@link InProcessEvaluationContext} contains shared state for an execution of the
 * {@link InProcessPipelineRunner} that can be used while evaluating a {@link PTransform}. This
 * consists of views into underlying state and watermark implementations, access to read and write
 * {@link PCollectionView PCollectionViews}, and constructing {@link CounterSet CounterSets} and
 * {@link ExecutionContext ExecutionContexts}. This includes executing callbacks asynchronously when
 * state changes to the appropriate point (e.g. when a {@link PCollectionView} is requested and
 * known to be empty).
 *
 * <p>{@link InProcessEvaluationContext} also handles results by committing finalizing bundles based
 * on the current global state and updating the global state appropriately. This includes updating
 * the per-{@link StepAndKey} state, updating global watermarks, and executing any callbacks that
 * can be executed.
 */
class InProcessEvaluationContext {
  /** The step name for each {@link AppliedPTransform} in the {@link Pipeline}. */
  private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

  /** The options that were used to create this {@link Pipeline}. */
  private final InProcessPipelineOptions options;

  private final BundleFactory bundleFactory;
  /** The current processing time and event time watermarks and timers. */
  private final InMemoryWatermarkManager watermarkManager;

  /** Executes callbacks based on the progression of the watermark. */
  private final WatermarkCallbackExecutor callbackExecutor;

  /** The stateInternals of the world, by applied PTransform and key. */
  private final ConcurrentMap<StepAndKey, CopyOnAccessInMemoryStateInternals<?>>
      applicationStateInternals;

  private final InProcessSideInputContainer sideInputContainer;

  private final CounterSet mergedCounters;

  public static InProcessEvaluationContext create(
      InProcessPipelineOptions options,
      BundleFactory bundleFactory,
      Collection<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames,
      Collection<PCollectionView<?>> views) {
    return new InProcessEvaluationContext(
        options, bundleFactory, rootTransforms, valueToConsumers, stepNames, views);
  }

  private InProcessEvaluationContext(
      InProcessPipelineOptions options,
      BundleFactory bundleFactory,
      Collection<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames,
      Collection<PCollectionView<?>> views) {
    this.options = checkNotNull(options);
    this.bundleFactory = checkNotNull(bundleFactory);
    checkNotNull(rootTransforms);
    checkNotNull(valueToConsumers);
    checkNotNull(stepNames);
    checkNotNull(views);
    this.stepNames = stepNames;

    this.watermarkManager =
        InMemoryWatermarkManager.create(
            NanosOffsetClock.create(), rootTransforms, valueToConsumers);
    this.sideInputContainer = InProcessSideInputContainer.create(this, views);

    this.applicationStateInternals = new ConcurrentHashMap<>();
    this.mergedCounters = new CounterSet();

    this.callbackExecutor = WatermarkCallbackExecutor.create();
  }

  /**
   * Handle the provided {@link InProcessTransformResult}, produced after evaluating the provided
   * {@link CommittedBundle} (potentially null, if the result of a root {@link PTransform}).
   *
   * <p>The result is the output of running the transform contained in the
   * {@link InProcessTransformResult} on the contents of the provided bundle.
   *
   * @param completedBundle the bundle that was processed to produce the result. Potentially
   *                        {@code null} if the transform that produced the result is a root
   *                        transform
   * @param completedTimers the timers that were delivered to produce the {@code completedBundle},
   *                        or an empty iterable if no timers were delivered
   * @param result the result of evaluating the input bundle
   * @return the committed bundles contained within the handled {@code result}
   */
  public synchronized CommittedResult handleResult(
      @Nullable CommittedBundle<?> completedBundle,
      Iterable<TimerData> completedTimers,
      InProcessTransformResult result) {
    Iterable<? extends CommittedBundle<?>> committedBundles =
        commitBundles(result.getOutputBundles());
    // Update watermarks and timers
    watermarkManager.updateWatermarks(
        completedBundle,
        result.getTransform(),
        result.getTimerUpdate().withCompletedTimers(completedTimers),
        committedBundles,
        result.getWatermarkHold());
    fireAllAvailableCallbacks();
    // Update counters
    if (result.getCounters() != null) {
      mergedCounters.merge(result.getCounters());
    }
    // Update state internals
    CopyOnAccessInMemoryStateInternals<?> theirState = result.getState();
    if (theirState != null) {
      CopyOnAccessInMemoryStateInternals<?> committedState = theirState.commit();
      StepAndKey stepAndKey =
          StepAndKey.of(
              result.getTransform(), completedBundle == null ? null : completedBundle.getKey());
      if (!committedState.isEmpty()) {
        applicationStateInternals.put(stepAndKey, committedState);
      } else {
        applicationStateInternals.remove(stepAndKey);
      }
    }
    return CommittedResult.create(result, committedBundles);
  }

  private Iterable<? extends CommittedBundle<?>> commitBundles(
      Iterable<? extends UncommittedBundle<?>> bundles) {
    ImmutableList.Builder<CommittedBundle<?>> completed = ImmutableList.builder();
    for (UncommittedBundle<?> inProgress : bundles) {
      AppliedPTransform<?, ?, ?> producing =
          inProgress.getPCollection().getProducingTransformInternal();
      TransformWatermarks watermarks = watermarkManager.getWatermarks(producing);
      CommittedBundle<?> committed =
          inProgress.commit(watermarks.getSynchronizedProcessingOutputTime());
      // Empty bundles don't impact watermarks and shouldn't trigger downstream execution, so
      // filter them out
      if (!Iterables.isEmpty(committed.getElements())) {
        completed.add(committed);
      }
    }
    return completed.build();
  }

  private void fireAllAvailableCallbacks() {
    for (AppliedPTransform<?, ?, ?> transform : stepNames.keySet()) {
      fireAvailableCallbacks(transform);
    }
  }

  private void fireAvailableCallbacks(AppliedPTransform<?, ?, ?> producingTransform) {
    TransformWatermarks watermarks = watermarkManager.getWatermarks(producingTransform);
    callbackExecutor.fireForWatermark(producingTransform, watermarks.getOutputWatermark());
  }

  /**
   * Create a {@link UncommittedBundle} for use by a source.
   */
  public <T> UncommittedBundle<T> createRootBundle(PCollection<T> output) {
    return bundleFactory.createRootBundle(output);
  }

  /**
   * Create a {@link UncommittedBundle} whose elements belong to the specified {@link
   * PCollection}.
   */
  public <T> UncommittedBundle<T> createBundle(CommittedBundle<?> input, PCollection<T> output) {
    return bundleFactory.createBundle(input, output);
  }

  /**
   * Create a {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@link InProcessGroupByKeyOnly} {@link PTransform PTransforms}.
   */
  public <T> UncommittedBundle<T> createKeyedBundle(
      CommittedBundle<?> input, Object key, PCollection<T> output) {
    return bundleFactory.createKeyedBundle(input, key, output);
  }

  /**
   * Create a {@link PCollectionViewWriter}, whose elements will be used in the provided
   * {@link PCollectionView}.
   */
  public <ElemT, ViewT> PCollectionViewWriter<ElemT, ViewT> createPCollectionViewWriter(
      PCollection<Iterable<ElemT>> input, final PCollectionView<ViewT> output) {
    return new PCollectionViewWriter<ElemT, ViewT>() {
      @Override
      public void add(Iterable<WindowedValue<ElemT>> values) {
        sideInputContainer.write(output, values);
      }
    };
  }

  /**
   * Schedule a callback to be executed after output would be produced for the given window
   * if there had been input.
   *
   * <p>Output would be produced when the watermark for a {@link PValue} passes the point at
   * which the trigger for the specified window (with the specified windowing strategy) must have
   * fired from the perspective of that {@link PValue}, as specified by the value of
   * {@link Trigger#getWatermarkThatGuaranteesFiring(BoundedWindow)} for the trigger of the
   * {@link WindowingStrategy}. When the callback has fired, either values will have been produced
   * for a key in that window, the window is empty, or all elements in the window are late. The
   * callback will be executed regardless of whether values have been produced.
   */
  public void scheduleAfterOutputWouldBeProduced(
      PValue value,
      BoundedWindow window,
      WindowingStrategy<?, ?> windowingStrategy,
      Runnable runnable) {
    AppliedPTransform<?, ?, ?> producing = getProducing(value);
    callbackExecutor.callOnGuaranteedFiring(producing, window, windowingStrategy, runnable);

    fireAvailableCallbacks(lookupProducing(value));
  }

  private AppliedPTransform<?, ?, ?> getProducing(PValue value) {
    if (value.getProducingTransformInternal() != null) {
      return value.getProducingTransformInternal();
    }
    return lookupProducing(value);
  }

  private AppliedPTransform<?, ?, ?> lookupProducing(PValue value) {
    for (AppliedPTransform<?, ?, ?> transform : stepNames.keySet()) {
      if (transform.getOutput().equals(value) || transform.getOutput().expand().contains(value)) {
        return transform;
      }
    }
    return null;
  }

  /**
   * Get the options used by this {@link Pipeline}.
   */
  public InProcessPipelineOptions getPipelineOptions() {
    return options;
  }

  /**
   * Get an {@link ExecutionContext} for the provided {@link AppliedPTransform} and key.
   */
  public InProcessExecutionContext getExecutionContext(
      AppliedPTransform<?, ?, ?> application, Object key) {
    StepAndKey stepAndKey = StepAndKey.of(application, key);
    return new InProcessExecutionContext(
        options.getClock(),
        key,
        (CopyOnAccessInMemoryStateInternals<Object>) applicationStateInternals.get(stepAndKey),
        watermarkManager.getWatermarks(application));
  }

  /**
   * Get all of the steps used in this {@link Pipeline}.
   */
  public Collection<AppliedPTransform<?, ?, ?>> getSteps() {
    return stepNames.keySet();
  }

  /**
   * Get the Step Name for the provided application.
   */
  public String getStepName(AppliedPTransform<?, ?, ?> application) {
    return stepNames.get(application);
  }

  /**
   * Returns a {@link ReadyCheckingSideInputReader} capable of reading the provided
   * {@link PCollectionView PCollectionViews}.
   *
   * @param sideInputs the {@link PCollectionView PCollectionViews} the result should be able to
   * read
   * @return a {@link SideInputReader} that can read all of the provided {@link PCollectionView
   * PCollectionViews}
   */
  public ReadyCheckingSideInputReader createSideInputReader(
      final List<PCollectionView<?>> sideInputs) {
    return sideInputContainer.createReaderForViews(sideInputs);
  }

  /**
   * A {@link SideInputReader} that allows callers to check to see if a {@link PCollectionView} has
   * had its contents set in a window.
   */
  interface ReadyCheckingSideInputReader extends SideInputReader {
    /**
     * Returns true if the {@link PCollectionView} is ready in the provided {@link BoundedWindow}.
     */
    boolean isReady(PCollectionView<?> view, BoundedWindow window);
  }

  /**
   * Create a {@link CounterSet} for this {@link Pipeline}. The {@link CounterSet} is independent
   * of all other {@link CounterSet CounterSets} created by this call.
   *
   * The {@link InProcessEvaluationContext} is responsible for unifying the counters present in
   * all created {@link CounterSet CounterSets} when the transforms that call this method
   * complete.
   */
  public CounterSet createCounterSet() {
    return new CounterSet();
  }

  /**
   * Returns all of the counters that have been merged into this context via calls to
   * {@link CounterSet#merge(CounterSet)}.
   */
  public CounterSet getCounters() {
    return mergedCounters;
  }

  /**
   * Extracts all timers that have been fired and have not already been extracted.
   *
   * <p>This is a destructive operation. Timers will only appear in the result of this method once
   * for each time they are set.
   */
  public Map<AppliedPTransform<?, ?, ?>, Map<Object, FiredTimers>> extractFiredTimers() {
    Map<AppliedPTransform<?, ?, ?>, Map<Object, FiredTimers>> fired =
        watermarkManager.extractFiredTimers();
    return fired;
  }

  /**
   * Returns true if the step will not produce additional output.
   *
   * <p>If the provided transform produces only {@link IsBounded#BOUNDED}
   * {@link PCollection PCollections}, returns true if the watermark is at
   * {@link BoundedWindow#TIMESTAMP_MAX_VALUE positive infinity}.
   *
   * <p>If the provided transform produces any {@link IsBounded#UNBOUNDED}
   * {@link PCollection PCollections}, returns the value of
   * {@link InProcessPipelineOptions#isShutdownUnboundedProducersWithMaxWatermark()}.
   */
  public boolean isDone(AppliedPTransform<?, ?, ?> transform) {
    // if the PTransform's watermark isn't at the max value, it isn't done
    if (watermarkManager
        .getWatermarks(transform)
        .getOutputWatermark()
        .isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      return false;
    }
    // If the PTransform has any unbounded outputs, and unbounded producers should not be shut down,
    // the PTransform may produce additional output. It is not done.
    for (PValue output : transform.getOutput().expand()) {
      if (output instanceof PCollection) {
        IsBounded bounded = ((PCollection<?>) output).isBounded();
        if (bounded.equals(IsBounded.UNBOUNDED)
            && !options.isShutdownUnboundedProducersWithMaxWatermark()) {
          return false;
        }
      }
    }
    // The PTransform's watermark was at positive infinity and all of its outputs are known to be
    // done. It is done.
    return true;
  }

  /**
   * Returns true if all steps are done.
   */
  public boolean isDone() {
    for (AppliedPTransform<?, ?, ?> transform : stepNames.keySet()) {
      if (!isDone(transform)) {
        return false;
      }
    }
    return true;
  }
}
