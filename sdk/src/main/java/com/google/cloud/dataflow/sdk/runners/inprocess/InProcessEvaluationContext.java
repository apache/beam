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
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.joda.time.Instant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

/**
 * The evaluation context for the {@link InProcessPipelineRunner}. Contains state shared within
 * the current evaluation.
 */
class InProcessEvaluationContext {
  private final Set<AppliedPTransform<?, ?, ?>> allTransforms;
  private final Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers;
  private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

  private final InProcessPipelineOptions options;
  private final TransformEvaluatorRegistry registry;
  private final InMemoryWatermarkManager watermarkManager;
  private final ConcurrentMap<AppliedPTransform<?, ?, ?>, PriorityQueue<WatermarkCallback>>
      pendingCallbacks;

  // The stateInternals of the world, by applied PTransform and key.
  private final ConcurrentMap<StepAndKey, CopyOnAccessInMemoryStateInternals<?>>
      applicationStateInternals;

  private final InProcessSideInputContainer sideInputContainer;

  private final CounterSet mergedCounters;

  private final Executor callbackExecutor;

  public static InProcessEvaluationContext create(InProcessPipelineOptions options,
      TransformEvaluatorRegistry evaluatorRegistry,
      Collection<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames, Collection<PCollectionView<?>> views) {
    return new InProcessEvaluationContext(
        options, evaluatorRegistry, rootTransforms, valueToConsumers, stepNames, views);
  }

  private InProcessEvaluationContext(InProcessPipelineOptions options,
      TransformEvaluatorRegistry registry,
      Collection<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers,
      Map<AppliedPTransform<?, ?, ?>, String> stepNames, Collection<PCollectionView<?>> views) {
    this.options = checkNotNull(options);
    this.registry = registry;
    checkNotNull(rootTransforms);
    checkNotNull(valueToConsumers);
    checkNotNull(stepNames);
    checkNotNull(views);

    this.allTransforms =
        ImmutableSet.<AppliedPTransform<?, ?, ?>>builder()
            .addAll(rootTransforms)
            .addAll(Iterables.concat(valueToConsumers.values()))
            .build();
    this.valueToConsumers = valueToConsumers;
    this.stepNames = stepNames;

    this.pendingCallbacks = new ConcurrentHashMap<>();
    this.watermarkManager = InMemoryWatermarkManager.create(
        NanosOffsetClock.create(), rootTransforms, valueToConsumers);
    this.sideInputContainer = InProcessSideInputContainer.create(this, views);

    this.applicationStateInternals = new ConcurrentHashMap<>();
    this.mergedCounters = new CounterSet();

    this.callbackExecutor = Executors.newCachedThreadPool();
  }

  /**
   * Handle the provided {@link InProcessTransformResult}, produced after evaluating the provided
   * {@link CommittedBundle} (potentially null, if the result of a root {@link PTransform}).
   *
   * <p>The result is the output of running the transform contained in the
   * {@link InProcessTransformResult result} on the contents of the provided bundle.
   *
   * @param completedBundle the bundle that was processed to produce the result. Potentially
   *                        {@code null} if a root transform
   * @param completedTimers the timers that were delivered to produce the {@code completedBundle},
   *                        or an empty iterable if no timers were delivered
   * @param result the result of evaluating the input bundle
   */
  public synchronized Iterable<? extends CommittedBundle<?>> handleResult(
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
    watermarksUpdated();
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
    return committedBundles;
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
      if (committed.getElements().iterator().hasNext()) {
        // Empty bundles don't impact watermarks and shouldn't trigger downstream execution
        completed.add(committed);
      }
    }
    return completed.build();
  }

  private void watermarksUpdated() {
    for (
        Map.Entry<AppliedPTransform<?, ?, ?>, PriorityQueue<WatermarkCallback>> transformCallbacks :
        pendingCallbacks.entrySet()) {
      checkCallbacks(transformCallbacks.getKey(), transformCallbacks.getValue());
    }
  }

  private void checkCallbacks(
      AppliedPTransform<?, ?, ?> producingTransform,
      PriorityQueue<WatermarkCallback> pendingTransformCallbacks) {
    TransformWatermarks watermarks = watermarkManager.getWatermarks(producingTransform);
    synchronized (pendingTransformCallbacks) {
      while (!pendingTransformCallbacks.isEmpty()
          && pendingTransformCallbacks.peek().shouldFire(watermarks.getOutputWatermark())) {
        callbackExecutor.execute(pendingTransformCallbacks.poll().getCallback());
      }
    }
  }

  /**
   * Create a {@link UncommittedBundle} for use by a source.
   */
  public <T> UncommittedBundle<T> createRootBundle(PCollection<T> output) {
    return InProcessBundle.unkeyed(output);
  }

  /**
   * Create a {@link UncommittedBundle} whose elements belong to the specified {@link
   * PCollection}.
   */
  public <T> UncommittedBundle<T> createBundle(CommittedBundle<?> input, PCollection<T> output) {
    if (input.isKeyed()) {
      return InProcessBundle.keyed(output, input.getKey());
    } else {
      return InProcessBundle.unkeyed(output);
    }
  }

  /**
   * Create a {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@link InProcessGroupByKeyOnly} {@link PTransform PTransforms}.
   */
  public <T> UncommittedBundle<T> createKeyedBundle(
      CommittedBundle<?> input, Object key, PCollection<T> output) {
    return InProcessBundle.keyed(output, key);
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
   * Schedules a callback after the watermark for a {@link PValue} after the trigger for the
   * specified window (with the specified windowing strategy) must have fired from the perspective
   * of that {@link PValue}, as specified by the value of
   * {@link Trigger#getWatermarkThatGuaranteesFiring(BoundedWindow)} for the trigger of the
   * {@link WindowingStrategy}.
   */
  public void callAfterOutputMustHaveBeenProduced(
      PValue value,
      BoundedWindow window,
      WindowingStrategy<?, ?> windowingStrategy,
      Runnable runnable) {
    WatermarkCallback callback =
        WatermarkCallback.onWindowClose(window, windowingStrategy, runnable);
    AppliedPTransform<?, ?, ?> producing = getProducing(value);
    PriorityQueue<WatermarkCallback> callbacks = pendingCallbacks.get(producing);
    if (callbacks == null) {
      PriorityQueue<WatermarkCallback> newCallbacks =
          new PriorityQueue<>(10, new CallbackOrdering());
      pendingCallbacks.putIfAbsent(producing, newCallbacks);
      callbacks = pendingCallbacks.get(producing);
    }
    synchronized (callbacks) {
      callbacks.offer(callback);
    }
    checkCallbacks(producing, callbacks);
  }

  private AppliedPTransform<?, ?, ?> getProducing(PValue value) {
    if (value.getProducingTransformInternal() != null) {
      return value.getProducingTransformInternal();
    }
    return lookupProducing(value);
  }

  private AppliedPTransform<?, ?, ?> lookupProducing(PValue value) {
    for (AppliedPTransform<?, ?, ?> transform : allTransforms) {
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
    return new InProcessExecutionContext(options.getClock(), key,
        (CopyOnAccessInMemoryStateInternals<Object>) applicationStateInternals.get(stepAndKey),
        watermarkManager.getWatermarks(application));
  }

  /**
   * Get all of the steps used in this {@link Pipeline}.
   */
  public Collection<AppliedPTransform<?, ?, ?>> getSteps() {
    return allTransforms;
  }

  /**
   * Get the Step Name for the provided application.
   */
  public String getStepName(AppliedPTransform<?, ?, ?> application) {
    return stepNames.get(application);
  }

  /**
   * Returns a {@link SideInputReader} capable of reading the provided
   * {@link PCollectionView PCollectionViews}.
   * @param sideInputs the {@link PCollectionView PCollectionViews} the result should be able to
   *                   read
   * @return a {@link SideInputReader} that can read all of the provided
   *         {@link PCollectionView PCollectionViews}
   */
  public SideInputReader createSideInputReader(final List<PCollectionView<?>> sideInputs) {
    return sideInputContainer.withViews(sideInputs);
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
   * Gets all of the {@link AppliedPTransform transform applications} that consume the provided
   * {@link PValue} in this {@link Pipeline}.
   *
   * <p>Each returned {@link AppliedPTransform} will contain the provide {@link PValue} in its
   * expanded output (i.e., the result of
   * {@code application.getInput().expand().contains(pvalue) == true} for each returned value.)
   */
  public Collection<AppliedPTransform<?, ?, ?>> getConsumers(PValue pvalue) {
    return valueToConsumers.get(pvalue);
  }

  /**
   * Gets a transform evaluator capable of evaluating the provided {@link AppliedPTransform} on the
   * provided {@link CommittedBundle Bundle}.
   *
   * @throws Exception if constructing the evaluator throws an exception
   */
  public <T> TransformEvaluator<T> getTransformEvaluator(
      AppliedPTransform<?, ?, ?> transform, CommittedBundle<T> inputBundle) throws Exception {
    return registry.forApplication(transform, inputBundle, this);
  }

  private static class WatermarkCallback {
    public static <W extends BoundedWindow> WatermarkCallback onWindowClose(
        BoundedWindow window, WindowingStrategy<?, W> strategy, Runnable callback) {
      @SuppressWarnings("unchecked")
      Instant firingAfter =
          strategy.getTrigger().getSpec().getWatermarkThatGuaranteesFiring((W) window);
      return new WatermarkCallback(firingAfter, callback);
    }

    private final Instant fireAfter;
    private final Runnable callback;

    private WatermarkCallback(Instant fireAfter, Runnable callback) {
      this.fireAfter = fireAfter;
      this.callback = callback;
    }

    public boolean shouldFire(Instant currentWatermark) {
      return currentWatermark.isAfter(fireAfter)
          || currentWatermark.equals(BoundedWindow.TIMESTAMP_MAX_VALUE);
    }

    public Runnable getCallback() {
      return callback;
    }
  }

  private static class CallbackOrdering extends Ordering<WatermarkCallback> {
    @Override
    public int compare(WatermarkCallback left, WatermarkCallback right) {
      return ComparisonChain.start()
          .compare(left.fireAfter, right.fireAfter)
          .compare(left.callback, right.callback, Ordering.arbitrary())
          .result();
    }
  }

  /**
   * Extracts all timers that have been fired and have not already been extracted.
   *
   * <p>This is a destructive operation. Timers will only appear in the result of this method once.
   */
  public Map<AppliedPTransform<?, ?, ?>, Map<Object, FiredTimers>> extractFiredTimers() {
    return watermarkManager.extractFiredTimers();
  }

  /**
   * Returns true if all steps are done.
   */
  public boolean isDone() {
    return watermarkManager.isDone();
  }
}
