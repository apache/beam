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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.inprocess.InMemoryWatermarkManager.FiredTimers;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessExecutor;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.KeyedWorkItem;
import com.google.cloud.dataflow.sdk.util.KeyedWorkItems;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

/**
 * An {@link InProcessExecutor} that uses an underlying {@link ExecutorService} and
 * {@link InProcessEvaluationContext} to execute a {@link Pipeline}.
 */
final class ExecutorServiceParallelExecutor implements InProcessExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceParallelExecutor.class);

  private final ExecutorService executorService;
  private final InProcessEvaluationContext evaluationContext;
  private final Collection<AppliedPTransform<?, ?, ?>> incompleteRootNodes;

  private final ConcurrentMap<StepAndKey, TransformExecutorService> currentEvaluations;
  private final ConcurrentMap<TransformExecutor<?>, Boolean> scheduledExecutors;

  private final Queue<ExecutorUpdate> allUpdates;
  private final BlockingQueue<VisibleExecutorUpdate> visibleUpdates;

  private final CompletionCallback defaultCompletionCallback;

  public static ExecutorServiceParallelExecutor create(
      ExecutorService executorService, InProcessEvaluationContext context) {
    return new ExecutorServiceParallelExecutor(executorService, context);
  }

  private ExecutorServiceParallelExecutor(
      ExecutorService executorService, InProcessEvaluationContext context) {
    this.executorService = executorService;
    this.evaluationContext = context;
    this.incompleteRootNodes = new CopyOnWriteArrayList<>();

    currentEvaluations = new ConcurrentHashMap<>();
    scheduledExecutors = new ConcurrentHashMap<>();

    this.allUpdates = new ConcurrentLinkedQueue<>();
    this.visibleUpdates = new ArrayBlockingQueue<>(20);

    defaultCompletionCallback = new TimerlessCompletionCallback();
  }

  @Override
  public void start(Collection<AppliedPTransform<?, ?, ?>> roots) {
    incompleteRootNodes.addAll(roots);
    Runnable monitorRunnable = new MonitorRunnable();
    executorService.submit(monitorRunnable);
  }

  @SuppressWarnings("unchecked")
  public void scheduleConsumption(AppliedPTransform<?, ?, ?> consumer, CommittedBundle<?> bundle,
      CompletionCallback onComplete) {
    evaluateBundle(consumer, bundle, onComplete);
  }

  private <T> void evaluateBundle(final AppliedPTransform<?, ?, ?> transform,
      @Nullable final CommittedBundle<T> bundle, final CompletionCallback onComplete) {
    final StepAndKey stepAndKey = StepAndKey.of(transform, bundle == null ? null : bundle.getKey());
    TransformExecutorService state =
        getStepAndKeyExecutorService(stepAndKey, bundle == null ? true : !bundle.isKeyed());
    TransformExecutor<T> callable =
        TransformExecutor.create(evaluationContext, bundle, transform, onComplete, state);
    state.schedule(callable);
  }

  private void scheduleConsumers(CommittedBundle<?> bundle) {
    for (AppliedPTransform<?, ?, ?> consumer :
        evaluationContext.getConsumers(bundle.getPCollection())) {
      scheduleConsumption(consumer, bundle, defaultCompletionCallback);
    }
  }

  private TransformExecutorService getStepAndKeyExecutorService(
      StepAndKey stepAndKey, boolean parallelizable) {
    if (!currentEvaluations.containsKey(stepAndKey)) {
      TransformExecutorService evaluationState =
          parallelizable
              ? TransformExecutorServices.parallel(executorService, scheduledExecutors)
              : TransformExecutorServices.serial(executorService, scheduledExecutors);
      currentEvaluations.putIfAbsent(stepAndKey, evaluationState);
    }
    return currentEvaluations.get(stepAndKey);
  }

  @Override
  public void awaitCompletion() throws Throwable {
    VisibleExecutorUpdate update;
    do {
      update = visibleUpdates.take();
      if (update.throwable.isPresent()) {
        if (update.throwable.get() instanceof Exception) {
          throw update.throwable.get();
        } else {
          throw update.throwable.get();
        }
      }
    } while (!update.isDone());
    executorService.shutdown();
  }

  private class TimerlessCompletionCallback implements CompletionCallback {
    @Override
    public void handleResult(CommittedBundle<?> inputBundle, InProcessTransformResult result) {
      Iterable<? extends CommittedBundle<?>> resultBundles =
          evaluationContext.handleResult(inputBundle, Collections.<TimerData>emptyList(), result);
      for (CommittedBundle<?> outputBundle : resultBundles) {
        allUpdates.offer(ExecutorUpdate.fromBundle(outputBundle));
      }
    }

    @Override
    public void handleThrowable(CommittedBundle<?> inputBundle, Throwable t) {
      allUpdates.offer(ExecutorUpdate.fromThrowable(t));
    }
  }

  private class TimerCompletionCallback implements CompletionCallback {
    private final Iterable<TimerData> timers;

    private TimerCompletionCallback(Iterable<TimerData> timers) {
      this.timers = timers;
    }

    @Override
    public void handleResult(CommittedBundle<?> inputBundle, InProcessTransformResult result) {
      Iterable<? extends CommittedBundle<?>> resultBundles =
          evaluationContext.handleResult(inputBundle, timers, result);
      for (CommittedBundle<?> outputBundle : resultBundles) {
        allUpdates.offer(ExecutorUpdate.fromBundle(outputBundle));
      }
    }

    @Override
    public void handleThrowable(CommittedBundle<?> inputBundle, Throwable t) {
      allUpdates.offer(ExecutorUpdate.fromThrowable(t));
    }
  }

  /**
   * An internal status update on the state of the executor.
   *
   * Used to signal when the executor should be shut down (due to an exception).
   */
  private static class ExecutorUpdate {
    private final Optional<? extends CommittedBundle<?>> bundle;
    private final Optional<? extends Throwable> throwable;

    public static ExecutorUpdate fromBundle(CommittedBundle<?> bundle) {
      return new ExecutorUpdate(bundle, null);
    }

    public static ExecutorUpdate fromThrowable(Throwable t) {
      return new ExecutorUpdate(null, t);
    }

    private ExecutorUpdate(CommittedBundle<?> producedBundle, Throwable throwable) {
      this.bundle = Optional.fromNullable(producedBundle);
      this.throwable = Optional.fromNullable(throwable);
    }

    public Optional<? extends CommittedBundle<?>> getBundle() {
      return bundle;
    }

    public Optional<? extends Throwable> getException() {
      return throwable;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(ExecutorUpdate.class)
          .add("bundle", bundle).add("exception", throwable)
          .toString();
    }
  }

  /**
   * An update of interest to the user. Used in {@link #awaitCompletion} to decide weather to
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
    @Override
    public void run() {
      Thread.currentThread()
          .setName(
              String.format(
                  "%s$%s-monitor",
                  evaluationContext.getPipelineOptions().getAppName(),
                  ExecutorServiceParallelExecutor.class.getSimpleName()));
      try {
        while (true) {
          ExecutorUpdate update = allUpdates.poll();
          if (update != null) {
            LOG.debug("Executor Update: {}", update);
            if (update.getBundle().isPresent()) {
              scheduleConsumers(update.getBundle().get());
            } else if (update.getException().isPresent()) {
              visibleUpdates.offer(
                  VisibleExecutorUpdate.fromThrowable(update.getException().get()));
            }
          } else {
            Thread.sleep(50L);
          }
          mightNeedMoreWork();
          fireTimers();
          if (finishIfDone()) {
            break;
          }
        }
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
      }
    }

    private void addMoreWork(Collection<AppliedPTransform<?, ?, ?>> activeRoots) {
      for (AppliedPTransform<?, ?, ?> activeRoot : activeRoots) {
        scheduleConsumption(activeRoot, null, defaultCompletionCallback);
      }
    }

    public void fireTimers() throws Exception {
      try {
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
                  InProcessBundle
                      .<KeyedWorkItem<Object, Object>>keyed(
                          (PCollection) transform.getInput(), keyTimers.getKey())
                      .add(WindowedValue.valueInEmptyWindows(work))
                      .commit(Instant.now());
              scheduleConsumption(transform, bundle, new TimerCompletionCallback(delivery));
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Internal Error while delivering timers", e);
        throw e;
      }
    }

    private boolean finishIfDone() {
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

    private void mightNeedMoreWork() {
      synchronized (scheduledExecutors) {
        for (TransformExecutor<?> executor : scheduledExecutors.keySet()) {
          Thread thread = executor.getThread();
          if (thread != null) {
            switch (thread.getState()) {
              case BLOCKED:
              case WAITING:
              case TERMINATED:
              case TIMED_WAITING:
                break;
              default:
                return;
            }
          }
        }
      }
      addMoreWork(incompleteRootNodes);
    }
  }
}

