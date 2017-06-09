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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link PipelineExecutor} that uses an underlying {@link ExecutorService} and
 * {@link EvaluationContext} to execute a {@link Pipeline}.
 */
final class ExecutorServiceParallelExecutor implements PipelineExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceParallelExecutor.class);

  private final int targetParallelism;
  private final ExecutorService executorService;

  private final DirectGraph graph;
  private final RootProviderRegistry rootProviderRegistry;
  private final TransformEvaluatorRegistry registry;
  @SuppressWarnings("rawtypes")
  private final Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
      transformEnforcements;

  private final EvaluationContext evaluationContext;

  private final LoadingCache<StepAndKey, TransformExecutorService> executorServices;

  private final Queue<ExecutorUpdate> allUpdates;
  private final BlockingQueue<VisibleExecutorUpdate> visibleUpdates;

  private final TransformExecutorService parallelExecutorService;
  private final CompletionCallback defaultCompletionCallback;

  private final ConcurrentMap<AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<CommittedBundle<?>>>
      pendingRootBundles;

 private final AtomicReference<ExecutorState> state =
      new AtomicReference<>(ExecutorState.QUIESCENT);

  /**
   * Measures the number of {@link TransformExecutor TransformExecutors} that have been scheduled
   * but not yet completed.
   *
   * <p>Before a {@link TransformExecutor} is scheduled, this value is incremented. All methods in
   * {@link CompletionCallback} decrement this value.
   */
  private final AtomicLong outstandingWork = new AtomicLong();
  private AtomicReference<State> pipelineState = new AtomicReference<>(State.RUNNING);

  public static ExecutorServiceParallelExecutor create(
      int targetParallelism,
      DirectGraph graph,
      RootProviderRegistry rootProviderRegistry,
      TransformEvaluatorRegistry registry,
      @SuppressWarnings("rawtypes")
          Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
              transformEnforcements,
      EvaluationContext context) {
    return new ExecutorServiceParallelExecutor(
        targetParallelism,
        graph,
        rootProviderRegistry,
        registry,
        transformEnforcements,
        context);
  }

  private ExecutorServiceParallelExecutor(
      int targetParallelism,
      DirectGraph graph,
      RootProviderRegistry rootProviderRegistry,
      TransformEvaluatorRegistry registry,
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>> transformEnforcements,
      EvaluationContext context) {
    this.targetParallelism = targetParallelism;
    // Don't use Daemon threads for workers. The Pipeline should continue to execute even if there
    // are no other active threads (for example, because waitUntilFinish was not called)
    this.executorService =
        Executors.newFixedThreadPool(
            targetParallelism,
            new ThreadFactoryBuilder()
                .setThreadFactory(MoreExecutors.platformThreadFactory())
                .setNameFormat("direct-runner-worker")
                .build());
    this.graph = graph;
    this.rootProviderRegistry = rootProviderRegistry;
    this.registry = registry;
    this.transformEnforcements = transformEnforcements;
    this.evaluationContext = context;

    // Weak Values allows TransformExecutorServices that are no longer in use to be reclaimed.
    // Executing TransformExecutorServices have a strong reference to their TransformExecutorService
    // which stops the TransformExecutorServices from being prematurely garbage collected
    executorServices =
        CacheBuilder.newBuilder()
            .weakValues()
            .removalListener(shutdownExecutorServiceListener())
            .build(serialTransformExecutorServiceCacheLoader());

    this.allUpdates = new ConcurrentLinkedQueue<>();
    this.visibleUpdates = new LinkedBlockingQueue<>();

    parallelExecutorService = TransformExecutorServices.parallel(executorService);
    defaultCompletionCallback =
        new TimerIterableCompletionCallback(Collections.<TimerData>emptyList());
    this.pendingRootBundles = new ConcurrentHashMap<>();
  }

  private CacheLoader<StepAndKey, TransformExecutorService>
      serialTransformExecutorServiceCacheLoader() {
    return new CacheLoader<StepAndKey, TransformExecutorService>() {
      @Override
      public TransformExecutorService load(StepAndKey stepAndKey) throws Exception {
        return TransformExecutorServices.serial(executorService);
      }
    };
  }

  private RemovalListener<StepAndKey, TransformExecutorService> shutdownExecutorServiceListener() {
    return new RemovalListener<StepAndKey, TransformExecutorService>() {
      @Override
      public void onRemoval(
          RemovalNotification<StepAndKey, TransformExecutorService> notification) {
        TransformExecutorService service = notification.getValue();
        if (service != null) {
          service.shutdown();
        }
      }
    };
  }

  @Override
  public void start(Collection<AppliedPTransform<?, ?, ?>> roots) {
    int numTargetSplits = Math.max(3, targetParallelism);
    for (AppliedPTransform<?, ?, ?> root : roots) {
      ConcurrentLinkedQueue<CommittedBundle<?>> pending = new ConcurrentLinkedQueue<>();
      try {
        Collection<CommittedBundle<?>> initialInputs =
            rootProviderRegistry.getInitialInputs(root, numTargetSplits);
        pending.addAll(initialInputs);
      } catch (Exception e) {
        throw UserCodeException.wrap(e);
      }
      pendingRootBundles.put(root, pending);
    }
    evaluationContext.initialize(pendingRootBundles);
    Runnable monitorRunnable = new MonitorRunnable();
    executorService.submit(monitorRunnable);
  }

  @SuppressWarnings("unchecked")
  private void scheduleConsumption(
      AppliedPTransform<?, ?, ?> consumer,
      CommittedBundle<?> bundle,
      CompletionCallback onComplete) {
    evaluateBundle(consumer, bundle, onComplete);
  }

  private <T> void evaluateBundle(
      final AppliedPTransform<?, ?, ?> transform,
      final CommittedBundle<T> bundle,
      final CompletionCallback onComplete) {
    TransformExecutorService transformExecutor;

    if (isKeyed(bundle.getPCollection())) {
      final StepAndKey stepAndKey = StepAndKey.of(transform, bundle.getKey());
      // This executor will remain reachable until it has executed all scheduled transforms.
      // The TransformExecutors keep a strong reference to the Executor, the ExecutorService keeps
      // a reference to the scheduled TransformExecutor callable. Follow-up TransformExecutors
      // (scheduled due to the completion of another TransformExecutor) are provided to the
      // ExecutorService before the Earlier TransformExecutor callable completes.
      transformExecutor = executorServices.getUnchecked(stepAndKey);
    } else {
      transformExecutor = parallelExecutorService;
    }

    Collection<ModelEnforcementFactory> enforcements =
        MoreObjects.firstNonNull(
            transformEnforcements.get(transform.getTransform().getClass()),
            Collections.<ModelEnforcementFactory>emptyList());

    TransformExecutor<T> callable =
        TransformExecutor.create(
            evaluationContext,
            registry,
            enforcements,
            bundle,
            transform,
            onComplete,
            transformExecutor);
    outstandingWork.incrementAndGet();
    if (!pipelineState.get().isTerminal()) {
      transformExecutor.schedule(callable);
    }
  }

  private boolean isKeyed(PValue pvalue) {
    return evaluationContext.isKeyed(pvalue);
  }

  private void scheduleConsumers(ExecutorUpdate update) {
    CommittedBundle<?> bundle = update.getBundle().get();
    for (AppliedPTransform<?, ?, ?> consumer : update.getConsumers()) {
      scheduleConsumption(consumer, bundle, defaultCompletionCallback);
    }
  }

  @Override
  public State waitUntilFinish(Duration duration) throws Exception {
    Instant completionTime;
    if (duration.equals(Duration.ZERO)) {
      completionTime = new Instant(Long.MAX_VALUE);
    } else {
      completionTime = Instant.now().plus(duration);
    }

    VisibleExecutorUpdate update = null;
    while (Instant.now().isBefore(completionTime)
        && (update == null || isTerminalStateUpdate(update))) {
      // Get an update; don't block forever if another thread has handled it. The call to poll will
      // wait the entire timeout; this call primarily exists to relinquish any core.
      update = visibleUpdates.poll(25L, TimeUnit.MILLISECONDS);
      if (update == null && pipelineState.get().isTerminal()) {
        // there are no updates to process and no updates will ever be published because the
        // executor is shutdown
        return pipelineState.get();
      } else if (update != null && update.thrown.isPresent()) {
        Throwable thrown = update.thrown.get();
        if (thrown instanceof Exception) {
          throw (Exception) thrown;
        } else if (thrown instanceof Error) {
          throw (Error) thrown;
        } else {
          throw new Exception("Unknown Type of Throwable", thrown);
        }
      }
    }
    return pipelineState.get();
  }

  @Override
  public State getPipelineState() {
    return pipelineState.get();
  }

  private boolean isTerminalStateUpdate(VisibleExecutorUpdate update) {
    return !(update.getNewState() == null && update.getNewState().isTerminal());
  }

  @Override
  public void stop() {
    shutdownIfNecessary(State.CANCELLED);
    while (!visibleUpdates.offer(VisibleExecutorUpdate.cancelled())) {
      // Make sure "This Pipeline was Cancelled" notification arrives.
      visibleUpdates.poll();
    }
  }

  private void shutdownIfNecessary(State newState) {
    if (!newState.isTerminal()) {
      return;
    }
    LOG.debug("Pipeline has terminated. Shutting down.");
    pipelineState.compareAndSet(State.RUNNING, newState);
    // Stop accepting new work before shutting down the executor. This ensures that thread don't try
    // to add work to the shutdown executor.
    executorServices.invalidateAll();
    executorServices.cleanUp();
    parallelExecutorService.shutdown();
    executorService.shutdown();
    try {
      registry.cleanup();
    } catch (Exception e) {
      visibleUpdates.add(VisibleExecutorUpdate.fromException(e));
    }
  }

  /**
   * The base implementation of {@link CompletionCallback} that provides implementations for
   * {@link #handleResult(CommittedBundle, TransformResult)} and
   * {@link #handleException(CommittedBundle, Exception)}.
   */
  private class TimerIterableCompletionCallback implements CompletionCallback {
    private final Iterable<TimerData> timers;

    protected TimerIterableCompletionCallback(Iterable<TimerData> timers) {
      this.timers = timers;
    }

    @Override
    public final CommittedResult handleResult(
        CommittedBundle<?> inputBundle, TransformResult<?> result) {
      CommittedResult committedResult = evaluationContext.handleResult(inputBundle, timers, result);
      for (CommittedBundle<?> outputBundle : committedResult.getOutputs()) {
        allUpdates.offer(
            ExecutorUpdate.fromBundle(
                outputBundle, graph.getPerElementConsumers(outputBundle.getPCollection())));
      }
      CommittedBundle<?> unprocessedInputs = committedResult.getUnprocessedInputs();
      if (unprocessedInputs != null && !Iterables.isEmpty(unprocessedInputs.getElements())) {
        if (inputBundle.getPCollection() == null) {
          // TODO: Split this logic out of an if statement
          pendingRootBundles.get(result.getTransform()).offer(unprocessedInputs);
        } else {
          allUpdates.offer(
              ExecutorUpdate.fromBundle(
                  unprocessedInputs,
                  Collections.<AppliedPTransform<?, ?, ?>>singleton(
                      committedResult.getTransform())));
        }
      }
      if (!committedResult.getProducedOutputTypes().isEmpty()) {
        state.set(ExecutorState.ACTIVE);
      }
      outstandingWork.decrementAndGet();
      return committedResult;
    }

    @Override
    public void handleEmpty(AppliedPTransform<?, ?, ?> transform) {
      outstandingWork.decrementAndGet();
    }

    @Override
    public final void handleException(CommittedBundle<?> inputBundle, Exception e) {
      allUpdates.offer(ExecutorUpdate.fromException(e));
      outstandingWork.decrementAndGet();
    }

    @Override
    public void handleError(Error err) {
      visibleUpdates.add(VisibleExecutorUpdate.fromError(err));
    }
  }

  /**
   * An internal status update on the state of the executor.
   *
   * <p>Used to signal when the executor should be shut down (due to an exception).
   */
  @AutoValue
  abstract static class ExecutorUpdate {
    public static ExecutorUpdate fromBundle(
        CommittedBundle<?> bundle,
        Collection<AppliedPTransform<?, ?, ?>> consumers) {
      return new AutoValue_ExecutorServiceParallelExecutor_ExecutorUpdate(
          Optional.of(bundle),
          consumers,
          Optional.<Exception>absent());
    }

    public static ExecutorUpdate fromException(Exception e) {
      return new AutoValue_ExecutorServiceParallelExecutor_ExecutorUpdate(
          Optional.<CommittedBundle<?>>absent(),
          Collections.<AppliedPTransform<?, ?, ?>>emptyList(),
          Optional.of(e));
    }

    /**
     * Returns the bundle that produced this update.
     */
    public abstract Optional<? extends CommittedBundle<?>> getBundle();

    /**
     * Returns the transforms to process the bundle. If nonempty, {@link #getBundle()} will return
     * a present {@link Optional}.
     */
    public abstract Collection<AppliedPTransform<?, ?, ?>> getConsumers();

    public abstract Optional<? extends Exception> getException();
  }

  /**
   * An update of interest to the user. Used in {@link #waitUntilFinish} to decide whether to
   * return normally or throw an exception.
   */
  private static class VisibleExecutorUpdate {
    private final Optional<? extends Throwable> thrown;
    @Nullable
    private final State newState;

    public static VisibleExecutorUpdate fromException(Exception e) {
      return new VisibleExecutorUpdate(null, e);
    }

    public static VisibleExecutorUpdate fromError(Error err) {
      return new VisibleExecutorUpdate(State.FAILED, err);
    }

    public static VisibleExecutorUpdate finished() {
      return new VisibleExecutorUpdate(State.DONE, null);
    }

    public static VisibleExecutorUpdate cancelled() {
      return new VisibleExecutorUpdate(State.CANCELLED, null);
    }

    private VisibleExecutorUpdate(State newState, @Nullable Throwable exception) {
      this.thrown = Optional.fromNullable(exception);
      this.newState = newState;
    }

    public State getNewState() {
      return newState;
    }
  }

  private class MonitorRunnable implements Runnable {
    private final String runnableName = String.format("%s$%s-monitor",
        evaluationContext.getPipelineOptions().getAppName(),
        ExecutorServiceParallelExecutor.class.getSimpleName());

    private boolean exceptionThrown = false;

    @Override
    public void run() {
      String oldName = Thread.currentThread().getName();
      Thread.currentThread().setName(runnableName);
      try {
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
        Collection<ExecutorUpdate> updates = new ArrayList<>();
        // Pull all available updates off of the queue before adding additional work. This ensures
        // both loops terminate.
        ExecutorUpdate pendingUpdate = allUpdates.poll();
        while (pendingUpdate != null) {
          updates.add(pendingUpdate);
          pendingUpdate = allUpdates.poll();
        }
        for (ExecutorUpdate update : updates) {
          applyUpdate(noWorkOutstanding, startingState, update);
        }
        addWorkIfNecessary();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Monitor died due to being interrupted");
        while (!visibleUpdates.offer(VisibleExecutorUpdate.fromException(e))) {
          visibleUpdates.poll();
        }
      } catch (Exception t) {
        LOG.error("Monitor thread died due to exception", t);
        while (!visibleUpdates.offer(VisibleExecutorUpdate.fromException(t))) {
          visibleUpdates.poll();
        }
      } finally {
        if (!shouldShutdown()) {
          // The monitor thread should always be scheduled; but we only need to be scheduled once
          executorService.submit(this);
        }
        Thread.currentThread().setName(oldName);
      }
    }

    private void applyUpdate(
        boolean noWorkOutstanding, ExecutorState startingState, ExecutorUpdate update) {
      LOG.debug("Executor Update: {}", update);
      if (update.getBundle().isPresent()) {
        if (ExecutorState.ACTIVE == startingState
            || (ExecutorState.PROCESSING == startingState
                && noWorkOutstanding)) {
          scheduleConsumers(update);
        } else {
          allUpdates.offer(update);
        }
      } else if (update.getException().isPresent()) {
        checkState(
            visibleUpdates.offer(VisibleExecutorUpdate.fromException(update.getException().get())),
            "VisibleUpdates should always be able to receive an offered update");
        exceptionThrown = true;
      }
    }

    /**
     * Fires any available timers.
     */
    private void fireTimers() throws Exception {
      try {
        for (FiredTimers transformTimers : evaluationContext.extractFiredTimers()) {
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
                              transformTimers.getTransform().getInputs().values()))
                  .add(WindowedValue.valueInGlobalWindow(work))
                  .commit(evaluationContext.now());
          scheduleConsumption(
              transformTimers.getTransform(),
              bundle,
              new TimerIterableCompletionCallback(delivery));
          state.set(ExecutorState.ACTIVE);
        }
      } catch (Exception e) {
        LOG.error("Internal Error while delivering timers", e);
        throw e;
      }
    }

    private boolean shouldShutdown() {
      State nextState = State.UNKNOWN;
      if (exceptionThrown) {
        nextState = State.FAILED;
      } else if (evaluationContext.isDone()) {
        visibleUpdates.offer(VisibleExecutorUpdate.finished());
        nextState = State.DONE;
      }
      shutdownIfNecessary(nextState);
      return pipelineState.get().isTerminal();
    }

    /**
     * If all active {@link TransformExecutor TransformExecutors} are in a blocked state,
     * add more work from root nodes that may have additional work. This ensures that if a pipeline
     * has elements available from the root nodes it will add those elements when necessary.
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
            scheduleConsumption(pendingRootEntry.getKey(), bundle, defaultCompletionCallback);
            state.set(ExecutorState.ACTIVE);
          }
        }
      }
    }
  }


  /**
   * The state of the executor. The state of the executor determines the behavior of the
   * {@link MonitorRunnable} when it runs.
   */
  private enum ExecutorState {
    /**
     * Output has been produced since the last time the monitor ran. Work exists that has not yet
     * been evaluated, and all pending, including potentially blocked work, should be evaluated.
     *
     * <p>The executor becomes active whenever a timer fires, a {@link PCollectionView} is updated,
     * or output is produced by the evaluation of a {@link TransformExecutor}.
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
     * All elements are either buffered in state or are blocked on a side input. There are no
     * timers that are permitted to fire but have not. There is no outstanding work.
     *
     * <p>The pipeline will not make progress without the progression of watermarks, the progression
     * of processing time, or the addition of elements.
     */
    QUIESCENT
  }
}
