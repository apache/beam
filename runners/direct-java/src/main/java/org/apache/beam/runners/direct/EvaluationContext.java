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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.joda.time.Instant;

/**
 * The evaluation context for a specific pipeline being executed by the
 * {@link DirectRunner}. Contains state shared within the execution across all
 * transforms.
 *
 * <p>{@link EvaluationContext} contains shared state for an execution of the
 * {@link DirectRunner} that can be used while evaluating a {@link PTransform}. This
 * consists of views into underlying state and watermark implementations, access to read and write
 * {@link PCollectionView PCollectionViews}, and managing the
 * {@link ExecutionContext ExecutionContexts}. This includes executing callbacks asynchronously when
 * state changes to the appropriate point (e.g. when a {@link PCollectionView} is requested and
 * known to be empty).
 *
 * <p>{@link EvaluationContext} also handles results by committing finalizing bundles based
 * on the current global state and updating the global state appropriately. This includes updating
 * the per-{@link StepAndKey} state, updating global watermarks, and executing any callbacks that
 * can be executed.
 */
class EvaluationContext {
  /**
   * The graph representing this {@link Pipeline}.
   */
  private final DirectGraph graph;

  /** The options that were used to create this {@link Pipeline}. */
  private final DirectOptions options;
  private final Clock clock;

  private final BundleFactory bundleFactory;
  /** The current processing time and event time watermarks and timers. */
  private final WatermarkManager watermarkManager;

  /** Executes callbacks based on the progression of the watermark. */
  private final WatermarkCallbackExecutor callbackExecutor;

  /** The stateInternals of the world, by applied PTransform and key. */
  private final ConcurrentMap<StepAndKey, CopyOnAccessInMemoryStateInternals>
      applicationStateInternals;

  private final SideInputContainer sideInputContainer;

  private final DirectMetrics metrics;

  private final Set<PValue> keyedPValues;

  public static EvaluationContext create(
      DirectOptions options,
      Clock clock,
      BundleFactory bundleFactory,
      DirectGraph graph,
      Set<PValue> keyedPValues) {
    return new EvaluationContext(options, clock, bundleFactory, graph, keyedPValues);
  }

  private EvaluationContext(
      DirectOptions options,
      Clock clock,
      BundleFactory bundleFactory,
      DirectGraph graph,
      Set<PValue> keyedPValues) {
    this.options = checkNotNull(options);
    this.clock = clock;
    this.bundleFactory = checkNotNull(bundleFactory);
    this.graph = checkNotNull(graph);
    this.keyedPValues = keyedPValues;

    this.watermarkManager = WatermarkManager.create(clock, graph);
    this.sideInputContainer = SideInputContainer.create(this, graph.getViews());

    this.applicationStateInternals = new ConcurrentHashMap<>();
    this.metrics = new DirectMetrics();

    this.callbackExecutor =
        WatermarkCallbackExecutor.create(MoreExecutors.directExecutor());
  }

  public void initialize(
      Map<AppliedPTransform<?, ?, ?>, ? extends Iterable<CommittedBundle<?>>> initialInputs) {
    watermarkManager.initialize(initialInputs);
  }

  /**
   * Handle the provided {@link TransformResult}, produced after evaluating the provided
   * {@link CommittedBundle} (potentially null, if the result of a root {@link PTransform}).
   *
   * <p>The result is the output of running the transform contained in the
   * {@link TransformResult} on the contents of the provided bundle.
   *
   * @param completedBundle the bundle that was processed to produce the result. Potentially
   *                        {@code null} if the transform that produced the result is a root
   *                        transform
   * @param completedTimers the timers that were delivered to produce the {@code completedBundle},
   *                        or an empty iterable if no timers were delivered
   * @param result the result of evaluating the input bundle
   * @return the committed bundles contained within the handled {@code result}
   */
  public CommittedResult handleResult(
      @Nullable CommittedBundle<?> completedBundle,
      Iterable<TimerData> completedTimers,
      TransformResult<?> result) {
    Iterable<? extends CommittedBundle<?>> committedBundles =
        commitBundles(result.getOutputBundles());
    metrics.commitLogical(completedBundle, result.getLogicalMetricUpdates());

    // Update watermarks and timers
    EnumSet<OutputType> outputTypes = EnumSet.copyOf(result.getOutputTypes());
    if (Iterables.isEmpty(committedBundles)) {
      outputTypes.remove(OutputType.BUNDLE);
    } else {
      outputTypes.add(OutputType.BUNDLE);
    }
    CommittedResult committedResult = CommittedResult.create(result,
        completedBundle == null
            ? null
            : completedBundle.withElements((Iterable) result.getUnprocessedElements()),
        committedBundles,
        outputTypes);
    // Update state internals
    CopyOnAccessInMemoryStateInternals theirState = result.getState();
    if (theirState != null) {
      CopyOnAccessInMemoryStateInternals committedState = theirState.commit();
      StepAndKey stepAndKey =
          StepAndKey.of(
              result.getTransform(), completedBundle == null ? null : completedBundle.getKey());
      if (!committedState.isEmpty()) {
        applicationStateInternals.put(stepAndKey, committedState);
      } else {
        applicationStateInternals.remove(stepAndKey);
      }
    }
    // Watermarks are updated last to ensure visibility of any global state before progress is
    // permitted
    watermarkManager.updateWatermarks(
        completedBundle,
        result.getTimerUpdate().withCompletedTimers(completedTimers),
        committedResult,
        result.getWatermarkHold());
    return committedResult;
  }

  private Iterable<? extends CommittedBundle<?>> commitBundles(
      Iterable<? extends UncommittedBundle<?>> bundles) {
    ImmutableList.Builder<CommittedBundle<?>> completed = ImmutableList.builder();
    for (UncommittedBundle<?> inProgress : bundles) {
      AppliedPTransform<?, ?, ?> producing =
          graph.getProducer(inProgress.getPCollection());
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
    for (AppliedPTransform<?, ?, ?> transform : graph.getPrimitiveTransforms()) {
      fireAvailableCallbacks(transform);
    }
  }

  private void fireAvailableCallbacks(AppliedPTransform<?, ?, ?> producingTransform) {
    TransformWatermarks watermarks = watermarkManager.getWatermarks(producingTransform);
    Instant outputWatermark = watermarks.getOutputWatermark();
    callbackExecutor.fireForWatermark(producingTransform, outputWatermark);
  }

  /**
   * Create a {@link UncommittedBundle} for use by a source.
   */
  public <T> UncommittedBundle<T> createRootBundle() {
    return bundleFactory.createRootBundle();
  }

  /**
   * Create a {@link UncommittedBundle} whose elements belong to the specified {@link
   * PCollection}.
   */
  public <T> UncommittedBundle<T> createBundle(PCollection<T> output) {
    return bundleFactory.createBundle(output);
  }

  /**
   * Create a {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@link DirectGroupByKeyOnly} {@link PTransform PTransforms}.
   */
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      StructuralKey<K> key, PCollection<T> output) {
    return bundleFactory.createKeyedBundle(key, output);
  }

  /**
   * Indicate whether or not this {@link PCollection} has been determined to be
   * keyed.
   */
  public <T> boolean isKeyed(PValue pValue) {
    return keyedPValues.contains(pValue);
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
    AppliedPTransform<?, ?, ?> producing = graph.getProducer(value);
    callbackExecutor.callOnGuaranteedFiring(producing, window, windowingStrategy, runnable);

    fireAvailableCallbacks(producing);
  }

  /**
   * Schedule a callback to be executed after the given window is expired.
   *
   * <p>For example, upstream state associated with the window may be cleared.
   */
  public void scheduleAfterWindowExpiration(
      AppliedPTransform<?, ?, ?> producing,
      BoundedWindow window,
      WindowingStrategy<?, ?> windowingStrategy,
      Runnable runnable) {
    callbackExecutor.callOnWindowExpiration(producing, window, windowingStrategy, runnable);

    fireAvailableCallbacks(producing);
  }

  /**
   * Get the options used by this {@link Pipeline}.
   */
  public DirectOptions getPipelineOptions() {
    return options;
  }

  /**
   * Get an {@link ExecutionContext} for the provided {@link AppliedPTransform} and key.
   */
  public DirectExecutionContext getExecutionContext(
      AppliedPTransform<?, ?, ?> application, StructuralKey<?> key) {
    StepAndKey stepAndKey = StepAndKey.of(application, key);
    return new DirectExecutionContext(
        clock,
        key,
        (CopyOnAccessInMemoryStateInternals) applicationStateInternals.get(stepAndKey),
        watermarkManager.getWatermarks(application));
  }


  /**
   * Get the Step Name for the provided application.
   */
  String getStepName(AppliedPTransform<?, ?, ?> application) {
    return graph.getStepName(application);
  }

  /** Returns all of the steps in this {@link Pipeline}. */
  Collection<AppliedPTransform<?, ?, ?>> getSteps() {
    return graph.getPrimitiveTransforms();
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

  /** Returns the metrics container for this pipeline. */
  public DirectMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  void forceRefresh() {
    watermarkManager.refreshAll();
    fireAllAvailableCallbacks();
  }

  /**
   * Extracts all timers that have been fired and have not already been extracted.
   *
   * <p>This is a destructive operation. Timers will only appear in the result of this method once
   * for each time they are set.
   */
  public Collection<FiredTimers> extractFiredTimers() {
    forceRefresh();
    return watermarkManager.extractFiredTimers();
  }

  /**
   * Returns true if the step will not produce additional output.
   */
  public boolean isDone(AppliedPTransform<?, ?, ?> transform) {
    // the PTransform is done only if watermark is at the max value
    Instant stepWatermark = watermarkManager.getWatermarks(transform).getOutputWatermark();
    return !stepWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /**
   * Returns true if all steps are done.
   */
  public boolean isDone() {
    for (AppliedPTransform<?, ?, ?> transform : graph.getPrimitiveTransforms()) {
      if (!isDone(transform)) {
        return false;
      }
    }
    return true;
  }

  public Instant now() {
    return clock.now();
  }

  Clock getClock() {
    return clock;
  }
}
