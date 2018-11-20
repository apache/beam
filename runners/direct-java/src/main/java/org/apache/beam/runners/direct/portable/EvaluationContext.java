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
package org.apache.beam.runners.direct.portable;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.Clock;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.direct.WatermarkManager;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.runners.direct.portable.CommittedResult.OutputType;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * The evaluation context for a specific pipeline being executed by the {@code DirectRunner}.
 * Contains state shared within the execution across all transforms.
 *
 * <p>{@link EvaluationContext} contains shared state for an execution of the {@code DirectRunner}
 * that can be used while evaluating a {@link PTransform}. This consists of views into underlying
 * state and watermark implementations, access to read and write {@link PCollectionView
 * PCollectionViews}, and managing the {@link DirectStateAndTimers ExecutionContexts}. This includes
 * executing callbacks asynchronously when state changes to the appropriate point (e.g. when a
 * {@link PCollectionView} is requested and known to be empty).
 *
 * <p>{@link EvaluationContext} also handles results by committing finalizing bundles based on the
 * current global state and updating the global state appropriately. This includes updating the
 * per-{@link StepAndKey} state, updating global watermarks, and executing any callbacks that can be
 * executed.
 */
class EvaluationContext {
  /** The graph representing this {@link Pipeline}. */
  private final ExecutableGraph<PTransformNode, ? super PCollectionNode> graph;

  private final Clock clock;

  private final BundleFactory bundleFactory;

  /** The current processing time and event time watermarks and timers. */
  private final WatermarkManager<PTransformNode, ? super PCollectionNode> watermarkManager;

  /** Executes callbacks based on the progression of the watermark. */
  private final WatermarkCallbackExecutor callbackExecutor;

  /** The stateInternals of the world, by applied PTransform and key. */
  private final ConcurrentMap<StepAndKey, CopyOnAccessInMemoryStateInternals>
      applicationStateInternals;

  private final DirectMetrics metrics;

  private final Set<PCollectionNode> keyedPValues;

  public static EvaluationContext create(
      Clock clock,
      BundleFactory bundleFactory,
      ExecutableGraph<PTransformNode, ? super PCollectionNode> graph,
      Set<PCollectionNode> keyedPValues) {
    return new EvaluationContext(clock, bundleFactory, graph, keyedPValues);
  }

  private EvaluationContext(
      Clock clock,
      BundleFactory bundleFactory,
      ExecutableGraph<PTransformNode, ? super PCollectionNode> graph,
      Set<PCollectionNode> keyedPValues) {
    this.clock = clock;
    this.bundleFactory = checkNotNull(bundleFactory);
    this.graph = checkNotNull(graph);
    this.keyedPValues = keyedPValues;

    this.watermarkManager = WatermarkManager.create(clock, graph, PTransformNode::getId);

    this.applicationStateInternals = new ConcurrentHashMap<>();
    this.metrics = new DirectMetrics();

    this.callbackExecutor = WatermarkCallbackExecutor.create(MoreExecutors.directExecutor());
  }

  public void initialize(
      Map<PTransformNode, ? extends Iterable<CommittedBundle<?>>> initialInputs) {
    watermarkManager.initialize((Map) initialInputs);
  }

  /**
   * Handle the provided {@link TransformResult}, produced after evaluating the provided {@link
   * CommittedBundle} (potentially null, if the result of a root {@link PTransform}).
   *
   * <p>The result is the output of running the transform contained in the {@link TransformResult}
   * on the contents of the provided bundle.
   *
   * @param completedBundle the bundle that was processed to produce the result. Potentially {@code
   *     null} if the transform that produced the result is a root transform
   * @param completedTimers the timers that were delivered to produce the {@code completedBundle},
   *     or an empty iterable if no timers were delivered
   * @param result the result of evaluating the input bundle
   * @return the committed bundles contained within the handled {@code result}
   */
  public CommittedResult<PTransformNode> handleResult(
      CommittedBundle<?> completedBundle,
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
    CommittedResult<PTransformNode> committedResult =
        CommittedResult.create(
            result, getUnprocessedInput(completedBundle, result), committedBundles, outputTypes);
    // Update state internals
    CopyOnAccessInMemoryStateInternals theirState = result.getState();
    if (theirState != null) {
      CopyOnAccessInMemoryStateInternals committedState = theirState.commit();
      StepAndKey stepAndKey = StepAndKey.of(result.getTransform(), completedBundle.getKey());
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
        committedResult.getExecutable(),
        committedResult.getUnprocessedInputs().orNull(),
        committedResult.getOutputs(),
        result.getWatermarkHold());
    return committedResult;
  }

  /**
   * Returns an {@link Optional} containing a bundle which contains all of the unprocessed elements
   * that were not processed from the {@code completedBundle}. If all of the elements of the {@code
   * completedBundle} were processed, or if {@code completedBundle} is null, returns an absent
   * {@link Optional}.
   */
  private Optional<? extends CommittedBundle<?>> getUnprocessedInput(
      CommittedBundle<?> completedBundle, TransformResult<?> result) {
    if (completedBundle == null || Iterables.isEmpty(result.getUnprocessedElements())) {
      return Optional.absent();
    }
    CommittedBundle<?> residual =
        completedBundle.withElements((Iterable) result.getUnprocessedElements());
    return Optional.of(residual);
  }

  private Iterable<? extends CommittedBundle<?>> commitBundles(
      Iterable<? extends UncommittedBundle<?>> bundles) {
    ImmutableList.Builder<CommittedBundle<?>> completed = ImmutableList.builder();
    for (UncommittedBundle<?> inProgress : bundles) {
      PTransformNode producing = graph.getProducer(inProgress.getPCollection());
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
    for (PTransformNode transform : graph.getExecutables()) {
      fireAvailableCallbacks(transform);
    }
  }

  private void fireAvailableCallbacks(PTransformNode producingTransform) {
    TransformWatermarks watermarks = watermarkManager.getWatermarks(producingTransform);
    Instant outputWatermark = watermarks.getOutputWatermark();
    callbackExecutor.fireForWatermark(producingTransform, outputWatermark);
  }

  /** Create a {@link UncommittedBundle} for use by a source. */
  public <T> UncommittedBundle<T> createRootBundle() {
    return bundleFactory.createRootBundle();
  }

  /**
   * Create a {@link UncommittedBundle} whose elements belong to the specified {@link PCollection}.
   */
  public <T> UncommittedBundle<T> createBundle(PCollectionNode output) {
    return bundleFactory.createBundle(output);
  }

  /**
   * Create a {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@code DirectGroupByKeyOnly} {@link PTransform PTransforms}.
   */
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      StructuralKey<K> key, PCollectionNode output) {
    return bundleFactory.createKeyedBundle(key, output);
  }

  /** Indicate whether or not this {@link PCollection} has been determined to be keyed. */
  public <T> boolean isKeyed(PCollectionNode pValue) {
    return keyedPValues.contains(pValue);
  }

  /**
   * Schedule a callback to be executed after output would be produced for the given window if there
   * had been input.
   *
   * <p>Output would be produced when the watermark for a {@link PValue} passes the point at which
   * the trigger for the specified window (with the specified windowing strategy) must have fired
   * from the perspective of that {@link PValue}, as specified by the value of {@link
   * Trigger#getWatermarkThatGuaranteesFiring(BoundedWindow)} for the trigger of the {@link
   * WindowingStrategy}. When the callback has fired, either values will have been produced for a
   * key in that window, the window is empty, or all elements in the window are late. The callback
   * will be executed regardless of whether values have been produced.
   */
  public void scheduleAfterOutputWouldBeProduced(
      PCollectionNode value,
      BoundedWindow window,
      WindowingStrategy<?, ?> windowingStrategy,
      Runnable runnable) {
    PTransformNode producing = graph.getProducer(value);
    callbackExecutor.callOnWindowExpiration(producing, window, windowingStrategy, runnable);

    fireAvailableCallbacks(producing);
  }

  /** Get a {@link DirectStateAndTimers} for the provided {@link PTransformNode} and key. */
  public <K> StepStateAndTimers<K> getStateAndTimers(
      PTransformNode application, StructuralKey<K> key) {
    StepAndKey stepAndKey = StepAndKey.of(application, key);
    return new DirectStateAndTimers<>(
        key,
        applicationStateInternals.get(stepAndKey),
        clock,
        watermarkManager.getWatermarks(application));
  }

  /** Returns all of the steps in this {@link Pipeline}. */
  Collection<PTransformNode> getSteps() {
    return graph.getExecutables();
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
  public Collection<FiredTimers<PTransformNode>> extractFiredTimers() {
    forceRefresh();
    return watermarkManager.extractFiredTimers();
  }

  /** Returns true if the step will not produce additional output. */
  public boolean isDone(PTransformNode transform) {
    // the PTransform is done only if watermark is at the max value
    Instant stepWatermark = watermarkManager.getWatermarks(transform).getOutputWatermark();
    return !stepWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /** Returns true if all steps are done. */
  public boolean isDone() {
    for (PTransformNode transform : graph.getExecutables()) {
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
