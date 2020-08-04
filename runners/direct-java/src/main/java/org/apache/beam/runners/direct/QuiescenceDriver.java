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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.runners.local.ExecutionDriver;
import org.apache.beam.runners.local.PipelineMessageReceiver;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pushes additional work onto a {@link BundleProcessor} based on the fact that a pipeline has
 * quiesced.
 */
class QuiescenceDriver implements ExecutionDriver {
  private static final Logger LOG = LoggerFactory.getLogger(QuiescenceDriver.class);

  public static ExecutionDriver create(
      EvaluationContext context,
      DirectGraph graph,
      BundleProcessor<PCollection<?>, CommittedBundle<?>, AppliedPTransform<?, ?, ?>>
          bundleProcessor,
      PipelineMessageReceiver messageReceiver,
      Map<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>> initialBundles) {
    return new QuiescenceDriver(context, graph, bundleProcessor, messageReceiver, initialBundles);
  }

  private final EvaluationContext evaluationContext;
  private final DirectGraph graph;
  private final BundleProcessor<PCollection<?>, CommittedBundle<?>, AppliedPTransform<?, ?, ?>>
      bundleProcessor;
  private final PipelineMessageReceiver pipelineMessageReceiver;

  private final CompletionCallback defaultCompletionCallback =
      new TimerIterableCompletionCallback(Collections.emptyList());

  private final Map<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>>
      pendingRootBundles;
  private final Queue<WorkUpdate> pendingWork = new ConcurrentLinkedQueue<>();
  // We collect here bundles and AppliedPTransforms that have started to process bundle, but have
  // not completed it yet. The reason for that is that the bundle processing might change output
  // watermark of a PTransform before enqueuing the resulting bundle to pendingUpdates of downstream
  // PTransform, which can lead to watermark being updated past the emitted elements.
  private final Map<AppliedPTransform<?, ?, ?>, Collection<CommittedBundle<?>>> inflightBundles =
      new ConcurrentHashMap<>();

  private final AtomicReference<ExecutorState> state =
      new AtomicReference<>(ExecutorState.QUIESCENT);
  private final AtomicLong outstandingWork = new AtomicLong(0L);
  private boolean exceptionThrown = false;

  private QuiescenceDriver(
      EvaluationContext evaluationContext,
      DirectGraph graph,
      BundleProcessor<PCollection<?>, CommittedBundle<?>, AppliedPTransform<?, ?, ?>>
          bundleProcessor,
      PipelineMessageReceiver pipelineMessageReceiver,
      Map<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>>
          pendingRootBundles) {
    this.evaluationContext = evaluationContext;
    this.graph = graph;
    this.bundleProcessor = bundleProcessor;
    this.pipelineMessageReceiver = pipelineMessageReceiver;
    this.pendingRootBundles = pendingRootBundles;
  }

  @Override
  public DriverState drive() {
    boolean noWorkOutstanding = outstandingWork.get() == 0L;
    ExecutorState startingState = state.get();
    if (startingState == ExecutorState.ACTIVE) {
      // The remainder of this call will add all available work to the Executor, and there will
      // be no new work available
      state.compareAndSet(ExecutorState.ACTIVE, ExecutorState.PROCESSING);
    } else if (startingState == ExecutorState.PROCESSING && noWorkOutstanding) {
      // The executor has consumed all new work and no new work was added
      state.compareAndSet(ExecutorState.PROCESSING, ExecutorState.QUIESCING);
    } else if (startingState == ExecutorState.QUIESCING && noWorkOutstanding) {
      // The executor re-ran all blocked work and nothing could make progress.
      state.compareAndSet(ExecutorState.QUIESCING, ExecutorState.QUIESCENT);
    }
    fireTimers();
    Collection<WorkUpdate> updates = new ArrayList<>();
    // Pull all available updates off of the queue before adding additional work. This ensures
    // both loops terminate.
    WorkUpdate pendingUpdate = pendingWork.poll();
    while (pendingUpdate != null) {
      updates.add(pendingUpdate);
      pendingUpdate = pendingWork.poll();
    }
    for (WorkUpdate update : updates) {
      applyUpdate(noWorkOutstanding, startingState, update);
    }
    addWorkIfNecessary();

    if (exceptionThrown) {
      return DriverState.FAILED;
    } else if (evaluationContext.isDone()) {
      return DriverState.SHUTDOWN;
    } else {
      return DriverState.CONTINUE;
    }
  }

  private void applyUpdate(
      boolean noWorkOutstanding, ExecutorState startingState, WorkUpdate update) {
    LOG.debug("Executor Update: {}", update);
    if (update.getBundle().isPresent()) {
      if (ExecutorState.ACTIVE == startingState
          || (ExecutorState.PROCESSING == startingState && noWorkOutstanding)) {
        CommittedBundle<?> bundle = update.getBundle().get();
        for (AppliedPTransform<?, ?, ?> consumer : update.getConsumers()) {
          processBundle(bundle, consumer);
        }
      } else {
        pendingWork.offer(update);
      }
    } else if (update.getException().isPresent()) {
      pipelineMessageReceiver.failed(update.getException().get());
      exceptionThrown = true;
    }
  }

  private void processBundle(CommittedBundle<?> bundle, AppliedPTransform<?, ?, ?> consumer) {
    processBundle(bundle, consumer, defaultCompletionCallback);
  }

  private void processBundle(
      CommittedBundle<?> bundle, AppliedPTransform<?, ?, ?> consumer, CompletionCallback callback) {
    inflightBundles.compute(
        consumer,
        (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(bundle);
          return v;
        });
    outstandingWork.incrementAndGet();
    bundleProcessor.process(bundle, consumer, callback);
  }

  /** Fires any available timers. */
  private void fireTimers() {
    try {
      for (FiredTimers<AppliedPTransform<?, ?, ?>> transformTimers :
          evaluationContext.extractFiredTimers(inflightBundles.keySet())) {
        Collection<TimerData> delivery = transformTimers.getTimers();
        KeyedWorkItem<?, Object> work =
            KeyedWorkItems.timersWorkItem(transformTimers.getKey().getKey(), delivery);
        @SuppressWarnings({"unchecked", "rawtypes"})
        CommittedBundle<?> bundle =
            evaluationContext
                .createKeyedBundle(
                    transformTimers.getKey(),
                    (PCollection)
                        Iterables.getOnlyElement(
                            transformTimers.getExecutable().getMainInputs().values()))
                .add(WindowedValue.valueInGlobalWindow(work))
                .commit(evaluationContext.now());
        processBundle(
            bundle, transformTimers.getExecutable(), new TimerIterableCompletionCallback(delivery));
        state.set(ExecutorState.ACTIVE);
      }
    } catch (Exception e) {
      LOG.error("Internal Error while delivering timers", e);
      pipelineMessageReceiver.failed(e);
      exceptionThrown = true;
    }
  }

  /**
   * If all active {@link DirectTransformExecutor TransformExecutors} are in a blocked state, add
   * more work from root nodes that may have additional work. This ensures that if a pipeline has
   * elements available from the root nodes it will add those elements when necessary.
   */
  private void addWorkIfNecessary() {
    // If any timers have fired, they will add more work; We don't need to add more
    if (state.get() == ExecutorState.QUIESCENT) {
      // All current TransformExecutors are blocked; add more work from the roots.
      for (Map.Entry<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>>
          pendingRootEntry : pendingRootBundles.entrySet()) {
        Collection<CommittedBundle<?>> bundles = new ArrayList<>();
        // Pull all available work off of the queue, then schedule it all, so this loop
        // terminates
        while (!pendingRootEntry.getValue().isEmpty()) {
          CommittedBundle<?> bundle = pendingRootEntry.getValue().poll();
          bundles.add(bundle);
        }
        for (CommittedBundle<?> bundle : bundles) {
          processBundle(bundle, pendingRootEntry.getKey());
          state.set(ExecutorState.ACTIVE);
        }
      }
    }
  }

  /**
   * The state of the executor. The state of the executor determines the behavior of the {@link
   * QuiescenceDriver} when it runs.
   */
  private enum ExecutorState {
    /**
     * Output has been produced since the last time the monitor ran. Work exists that has not yet
     * been evaluated, and all pending, including potentially blocked work, should be evaluated.
     *
     * <p>The executor becomes active whenever a timer fires, a {@link PCollectionView} is updated,
     * or output is produced by the evaluation of a {@link DirectTransformExecutor}.
     */
    ACTIVE,
    /**
     * The Executor does not have any unevaluated work available to it, but work is in progress.
     * Work should not be added until the Executor becomes active or no work is outstanding.
     *
     * <p>If all outstanding work completes without the executor becoming {@code ACTIVE}, the
     * Executor enters state {@code QUIESCING}. Previously evaluated work must be reevaluated, in
     * case a side input has made progress.
     */
    PROCESSING,
    /**
     * All outstanding work is work that may be blocked on a side input. When there is no
     * outstanding work, the executor becomes {@code QUIESCENT}.
     */
    QUIESCING,
    /**
     * All elements are either buffered in state or are blocked on a side input. There are no timers
     * that are permitted to fire but have not. There is no outstanding work.
     *
     * <p>The pipeline will not make progress without the progression of watermarks, the progression
     * of processing time, or the addition of elements.
     */
    QUIESCENT
  }

  /**
   * The base implementation of {@link CompletionCallback} that provides implementations for {@link
   * #handleResult(CommittedBundle, TransformResult)} and {@link #handleException(CommittedBundle,
   * Exception)}.
   */
  private class TimerIterableCompletionCallback implements CompletionCallback {

    private final Iterable<TimerData> timers;

    TimerIterableCompletionCallback(Iterable<TimerData> timers) {
      this.timers = timers;
    }

    @Override
    public final CommittedResult handleResult(
        CommittedBundle<?> inputBundle, TransformResult<?> result) {

      final CommittedResult<AppliedPTransform<?, ?, ?>> committedResult;
      committedResult = evaluationContext.handleResult(inputBundle, timers, result);
      for (CommittedBundle<?> outputBundle : committedResult.getOutputs()) {
        pendingWork.offer(
            WorkUpdate.fromBundle(
                outputBundle, graph.getPerElementConsumers(outputBundle.getPCollection())));
      }
      Optional<? extends CommittedBundle<?>> unprocessedInputs =
          committedResult.getUnprocessedInputs();
      if (unprocessedInputs.isPresent()) {
        if (inputBundle.getPCollection() == null) {
          // TODO: Split this logic out of an if statement
          pendingRootBundles.get(result.getTransform()).offer(unprocessedInputs.get());
        } else {
          pendingWork.offer(
              WorkUpdate.fromBundle(
                  unprocessedInputs.get(), Collections.singleton(committedResult.getExecutable())));
        }
      }
      if (!committedResult.getProducedOutputTypes().isEmpty()) {
        state.set(ExecutorState.ACTIVE);
      }
      outstandingWork.decrementAndGet();
      inflightBundles.compute(
          result.getTransform(),
          (k, v) -> {
            v.remove(inputBundle);
            return v.isEmpty() ? null : v;
          });
      return committedResult;
    }

    @Override
    public void handleEmpty(AppliedPTransform<?, ?, ?> transform) {
      outstandingWork.decrementAndGet();
    }

    @Override
    public final void handleException(CommittedBundle<?> inputBundle, Exception e) {
      pendingWork.offer(WorkUpdate.fromException(e));
      outstandingWork.decrementAndGet();
    }

    @Override
    public void handleError(Error err) {
      pipelineMessageReceiver.failed(err);
    }
  }

  /**
   * An internal status update on the state of the executor.
   *
   * <p>Used to signal when the executor should be shut down (due to an exception).
   */
  @AutoValue
  abstract static class WorkUpdate {
    private static WorkUpdate fromBundle(
        CommittedBundle<?> bundle, Collection<AppliedPTransform<?, ?, ?>> consumers) {
      return new AutoValue_QuiescenceDriver_WorkUpdate(
          Optional.of(bundle), consumers, Optional.empty());
    }

    private static WorkUpdate fromException(Exception e) {
      return new AutoValue_QuiescenceDriver_WorkUpdate(
          Optional.empty(), Collections.emptyList(), Optional.of(e));
    }

    /** Returns the bundle that produced this update. */
    public abstract Optional<? extends CommittedBundle<?>> getBundle();

    /**
     * Returns the transforms to process the bundle. If nonempty, {@link #getBundle()} will return a
     * present {@link Optional}.
     */
    public abstract Collection<AppliedPTransform<?, ?, ?>> getConsumers();

    public abstract Optional<? extends Exception> getException();
  }
}
