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

import org.apache.beam.runners.direct.InMemoryWatermarkManager.FiredTimers;
import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;

import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * An {@link InProcessExecutor} that uses an underlying {@link ExecutorService} and
 * {@link InProcessEvaluationContext} to execute a {@link Pipeline}.
 */
final class ExecutorServiceParallelExecutor implements InProcessExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceParallelExecutor.class);

  private final ExecutorService executorService;

  private final Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers;
  private final Set<PValue> keyedPValues;
  private final TransformEvaluatorRegistry registry;
  @SuppressWarnings("rawtypes")
  private final Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>>
      transformEnforcements;

  private final InProcessEvaluationContext evaluationContext;

  private final LoadingCache<StepAndKey, TransformExecutorService> executorServices;
  private final ConcurrentMap<TransformExecutor<?>, Boolean> scheduledExecutors;

  private final Queue<ExecutorUpdate> allUpdates;
  private final BlockingQueue<VisibleExecutorUpdate> visibleUpdates;

  private final TransformExecutorService parallelExecutorService;
  private final CompletionCallback defaultCompletionCallback;

  private Collection<AppliedPTransform<?, ?, ?>> rootNodes;

  public static ExecutorServiceParallelExecutor create(
      ExecutorService executorService,
      Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers,
      Set<PValue> keyedPValues,
      TransformEvaluatorRegistry registry,
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>> transformEnforcements,
      InProcessEvaluationContext context) {
    return new ExecutorServiceParallelExecutor(
        executorService, valueToConsumers, keyedPValues, registry, transformEnforcements, context);
  }

  private ExecutorServiceParallelExecutor(
      ExecutorService executorService,
      Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers,
      Set<PValue> keyedPValues,
      TransformEvaluatorRegistry registry,
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, Collection<ModelEnforcementFactory>> transformEnforcements,
      InProcessEvaluationContext context) {
    this.executorService = executorService;
    this.valueToConsumers = valueToConsumers;
    this.keyedPValues = keyedPValues;
    this.registry = registry;
    this.transformEnforcements = transformEnforcements;
    this.evaluationContext = context;

    scheduledExecutors = new ConcurrentHashMap<>();
    // Weak Values allows TransformExecutorServices that are no longer in use to be reclaimed.
    // Executing TransformExecutorServices have a strong reference to their TransformExecutorService
    // which stops the TransformExecutorServices from being prematurely garbage collected
    executorServices =
        CacheBuilder.newBuilder().weakValues().build(serialTransformExecutorServiceCacheLoader());

    this.allUpdates = new ConcurrentLinkedQueue<>();
    this.visibleUpdates = new ArrayBlockingQueue<>(20);

    parallelExecutorService =
        TransformExecutorServices.parallel(executorService, scheduledExecutors);
    defaultCompletionCallback = new DefaultCompletionCallback();
  }

  private CacheLoader<StepAndKey, TransformExecutorService>
      serialTransformExecutorServiceCacheLoader() {
    return new CacheLoader<StepAndKey, TransformExecutorService>() {
      @Override
      public TransformExecutorService load(StepAndKey stepAndKey) throws Exception {
        return TransformExecutorServices.serial(executorService, scheduledExecutors);
      }
    };
  }

  @Override
  public void start(Collection<AppliedPTransform<?, ?, ?>> roots) {
    rootNodes = ImmutableList.copyOf(roots);
    Runnable monitorRunnable = new MonitorRunnable();
    executorService.submit(monitorRunnable);
  }

  @SuppressWarnings("unchecked")
  public void scheduleConsumption(
      AppliedPTransform<?, ?, ?> consumer,
      @Nullable CommittedBundle<?> bundle,
      CompletionCallback onComplete) {
    evaluateBundle(consumer, bundle, onComplete);
  }

  private <T> void evaluateBundle(
      final AppliedPTransform<?, ?, ?> transform,
      @Nullable final CommittedBundle<T> bundle,
      final CompletionCallback onComplete) {
    TransformExecutorService transformExecutor;

    if (bundle != null && isKeyed(bundle.getPCollection())) {
      final StepAndKey stepAndKey =
          StepAndKey.of(transform, bundle == null ? null : bundle.getKey());
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
            registry,
            enforcements,
            evaluationContext,
            bundle,
            transform,
            onComplete,
            transformExecutor);
    transformExecutor.schedule(callable);
  }

  private boolean isKeyed(PValue pvalue) {
    return keyedPValues.contains(pvalue);
  }

  private void scheduleConsumers(ExecutorUpdate update) {
    CommittedBundle<?> bundle = update.getBundle().get();
    for (AppliedPTransform<?, ?, ?> consumer : update.getConsumers()) {
      scheduleConsumption(consumer, bundle, defaultCompletionCallback);
    }
  }

  @Override
  public void awaitCompletion() throws Throwable {
    VisibleExecutorUpdate update;
    do {
      update = visibleUpdates.take();
      if (update.throwable.isPresent()) {
        throw update.throwable.get();
      }
    } while (!update.isDone());
    executorService.shutdown();
  }

  /**
   * The base implementation of {@link CompletionCallback} that provides implementations for
   * {@link #handleResult(CommittedBundle, InProcessTransformResult)} and
   * {@link #handleThrowable(CommittedBundle, Throwable)}, given an implementation of
   * {@link #getCommittedResult(CommittedBundle, InProcessTransformResult)}.
   */
  private abstract class CompletionCallbackBase implements CompletionCallback {
    protected abstract CommittedResult getCommittedResult(
        CommittedBundle<?> inputBundle,
        InProcessTransformResult result);

    @Override
    public final CommittedResult handleResult(
        CommittedBundle<?> inputBundle, InProcessTransformResult result) {
      CommittedResult committedResult = getCommittedResult(inputBundle, result);
      for (CommittedBundle<?> outputBundle : committedResult.getOutputs()) {
        allUpdates.offer(ExecutorUpdate.fromBundle(outputBundle,
            valueToConsumers.get(outputBundle.getPCollection())));
      }
      CommittedBundle<?> unprocessedInputs = committedResult.getUnprocessedInputs();
      if (unprocessedInputs != null && !Iterables.isEmpty(unprocessedInputs.getElements())) {
        allUpdates.offer(ExecutorUpdate.fromBundle(unprocessedInputs,
            Collections.<AppliedPTransform<?, ?, ?>>singleton(committedResult.getTransform())));
      }
      return committedResult;
    }

    @Override
    public final void handleThrowable(CommittedBundle<?> inputBundle, Throwable t) {
      allUpdates.offer(ExecutorUpdate.fromThrowable(t));
    }
  }

  /**
   * The default {@link CompletionCallback}. The default completion callback is used to complete
   * transform evaluations that are triggered due to the arrival of elements from an upstream
   * transform, or for a source transform.
   */
  private class DefaultCompletionCallback extends CompletionCallbackBase {
    @Override
    public CommittedResult getCommittedResult(
        CommittedBundle<?> inputBundle, InProcessTransformResult result) {
      return evaluationContext.handleResult(inputBundle,
          Collections.<TimerData>emptyList(),
          result);
    }
  }

  /**
   * A {@link CompletionCallback} where the completed bundle was produced to deliver some collection
   * of {@link TimerData timers}. When the evaluator completes successfully, reports all of the
   * timers used to create the input to the {@link InProcessEvaluationContext evaluation context}
   * as part of the result.
   */
  private class TimerCompletionCallback extends CompletionCallbackBase {
    private final Iterable<TimerData> timers;

    private TimerCompletionCallback(Iterable<TimerData> timers) {
      this.timers = timers;
    }

    @Override
    public CommittedResult getCommittedResult(
        CommittedBundle<?> inputBundle, InProcessTransformResult result) {
          return evaluationContext.handleResult(inputBundle, timers, result);
    }
  }

  /**
   * An internal status update on the state of the executor.
   *
   * Used to signal when the executor should be shut down (due to an exception).
   */
  @AutoValue
  abstract static class ExecutorUpdate {
    public static ExecutorUpdate fromBundle(
        CommittedBundle<?> bundle,
        Collection<AppliedPTransform<?, ?, ?>> consumers) {
      return new AutoValue_ExecutorServiceParallelExecutor_ExecutorUpdate(
          Optional.of(bundle),
          consumers,
          Optional.<Throwable>absent());
    }

    public static ExecutorUpdate fromThrowable(Throwable t) {
      return new AutoValue_ExecutorServiceParallelExecutor_ExecutorUpdate(
          Optional.<CommittedBundle<?>>absent(),
          Collections.<AppliedPTransform<?, ?, ?>>emptyList(),
          Optional.of(t));
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

    public abstract Optional<? extends Throwable> getException();
  }

  /**
   * An update of interest to the user. Used in {@link #awaitCompletion} to decide whether to
   * return normally or throw an exception.
   */
  private static class VisibleExecutorUpdate {
    private final Optional<? extends Throwable> throwable;
    private final boolean done;

    public static VisibleExecutorUpdate fromThrowable(Throwable e) {
      return new VisibleExecutorUpdate(false, e);
    }

    public static VisibleExecutorUpdate finished() {
      return new VisibleExecutorUpdate(true, null);
    }

    private VisibleExecutorUpdate(boolean done, @Nullable Throwable exception) {
      this.throwable = Optional.fromNullable(exception);
      this.done = done;
    }

    public boolean isDone() {
      return done;
    }
  }

  private class MonitorRunnable implements Runnable {
    // arbitrary termination condition to ensure progress in the presence of pushback
    private final long maxTimeProcessingUpdatesNanos = TimeUnit.MILLISECONDS.toNanos(5L);
    private final String runnableName = String.format("%s$%s-monitor",
        evaluationContext.getPipelineOptions().getAppName(),
        ExecutorServiceParallelExecutor.class.getSimpleName());

    @Override
    public void run() {
      String oldName = Thread.currentThread().getName();
      Thread.currentThread().setName(runnableName);
      try {
        ExecutorUpdate update = allUpdates.poll();
        int numUpdates = 0;
        // pull all of the pending work off of the queue
        long updatesStart = System.nanoTime();
        while (update != null) {
          LOG.debug("Executor Update: {}", update);
          if (update.getBundle().isPresent()) {
            scheduleConsumers(update);
          } else if (update.getException().isPresent()) {
            visibleUpdates.offer(VisibleExecutorUpdate.fromThrowable(update.getException().get()));
          }
          if (System.nanoTime() - updatesStart > maxTimeProcessingUpdatesNanos) {
            break;
          } else {
            update = allUpdates.poll();
          }
        }
        boolean timersFired = fireTimers();
        addWorkIfNecessary(timersFired);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Monitor died due to being interrupted");
        while (!visibleUpdates.offer(VisibleExecutorUpdate.fromThrowable(e))) {
          visibleUpdates.poll();
        }
      } catch (Throwable t) {
        LOG.error("Monitor thread died due to throwable", t);
        while (!visibleUpdates.offer(VisibleExecutorUpdate.fromThrowable(t))) {
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

    /**
     * Fires any available timers. Returns true if at least one timer was fired.
     */
    private boolean fireTimers() throws Exception {
      try {
        boolean firedTimers = false;
        for (Map.Entry<AppliedPTransform<?, ?, ?>, Map<Object, FiredTimers>> transformTimers :
            evaluationContext.extractFiredTimers().entrySet()) {
          AppliedPTransform<?, ?, ?> transform = transformTimers.getKey();
          for (Map.Entry<Object, FiredTimers> keyTimers : transformTimers.getValue().entrySet()) {
            for (TimeDomain domain : TimeDomain.values()) {
              Collection<TimerData> delivery = keyTimers.getValue().getTimers(domain);
              if (delivery.isEmpty()) {
                continue;
              }
              KeyedWorkItem<Object, Object> work =
                  KeyedWorkItems.timersWorkItem(keyTimers.getKey(), delivery);
              @SuppressWarnings({"unchecked", "rawtypes"})
              CommittedBundle<?> bundle =
                  evaluationContext
                      .createKeyedBundle(
                          null, keyTimers.getKey(), (PCollection) transform.getInput())
                      .add(WindowedValue.valueInEmptyWindows(work))
                      .commit(Instant.now());
              scheduleConsumption(transform, bundle, new TimerCompletionCallback(delivery));
              firedTimers = true;
            }
          }
        }
        return firedTimers;
      } catch (Exception e) {
        LOG.error("Internal Error while delivering timers", e);
        throw e;
      }
    }

    private boolean shouldShutdown() {
      if (evaluationContext.isDone()) {
        LOG.debug("Pipeline is finished. Shutting down. {}");
        while (!visibleUpdates.offer(VisibleExecutorUpdate.finished())) {
          visibleUpdates.poll();
        }
        executorService.shutdown();
        return true;
      }
      return false;
    }

    /**
     * If all active {@link TransformExecutor TransformExecutors} are in a blocked state,
     * add more work from root nodes that may have additional work. This ensures that if a pipeline
     * has elements available from the root nodes it will add those elements when necessary.
     */
    private void addWorkIfNecessary(boolean firedTimers) {
      // If any timers have fired, they will add more work; We don't need to add more
      if (firedTimers) {
        return;
      }
      for (TransformExecutor<?> executor : scheduledExecutors.keySet()) {
        if (!isExecutorBlocked(executor)) {
          // We have at least one executor that can proceed without adding additional work
          return;
        }
      }
      // All current TransformExecutors are blocked; add more work from the roots.
      for (AppliedPTransform<?, ?, ?> root : rootNodes) {
        if (!evaluationContext.isDone(root)) {
          scheduleConsumption(root, null, defaultCompletionCallback);
        }
      }
    }

    /**
     * Return true if the provided executor might make more progress if no action is taken.
     *
     * <p>May return false even if all executor threads are currently blocked or cleaning up, as
     * these can cause more work to be scheduled. If this does not occur, after these calls
     * terminate, future calls will return true if all executors are waiting.
     */
    private boolean isExecutorBlocked(TransformExecutor<?> executor) {
      Thread thread = executor.getThread();
      if (thread == null) {
        return false;
      }
      switch (thread.getState()) {
        case TERMINATED:
          throw new IllegalStateException(String.format(
              "Unexpectedly encountered a Terminated TransformExecutor %s", executor));
        case WAITING:
        case TIMED_WAITING:
          // The thread is waiting for some external input. Adding more work may cause the thread
          // to stop waiting (e.g. the thread is waiting on an unbounded side input)
          return true;
        case BLOCKED:
          // The executor is blocked on acquisition of a java monitor. This usually means it is
          // making a call to the EvaluationContext, but not a model-blocking call - and will
          // eventually complete, at which point we may reevaluate.
        default:
          // NEW and RUNNABLE threads can make progress
          return false;
      }
    }
  }
}
