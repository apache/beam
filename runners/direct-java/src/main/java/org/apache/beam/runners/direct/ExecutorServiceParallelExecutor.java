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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.local.ExecutionDriver;
import org.apache.beam.runners.local.ExecutionDriver.DriverState;
import org.apache.beam.runners.local.PipelineMessageReceiver;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Queues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link PipelineExecutor} that uses an underlying {@link ExecutorService} and {@link
 * EvaluationContext} to execute a {@link Pipeline}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
final class ExecutorServiceParallelExecutor
    implements PipelineExecutor,
        BundleProcessor<PCollection<?>, CommittedBundle<?>, AppliedPTransform<?, ?, ?>> {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceParallelExecutor.class);

  private final int targetParallelism;
  private final ExecutorService executorService;

  private final TransformEvaluatorRegistry registry;

  private final EvaluationContext evaluationContext;

  private final TransformExecutorFactory executorFactory;
  private final TransformExecutorService parallelExecutorService;
  private final LoadingCache<StepAndKey, TransformExecutorService> serialExecutorServices;

  private final QueueMessageReceiver visibleUpdates;

  private final ExecutorService metricsExecutor;

  private AtomicReference<State> pipelineState = new AtomicReference<>(State.RUNNING);

  public static ExecutorServiceParallelExecutor create(
      int targetParallelism,
      TransformEvaluatorRegistry registry,
      Map<String, Collection<ModelEnforcementFactory>> transformEnforcements,
      EvaluationContext context,
      ExecutorService metricsExecutor) {
    return new ExecutorServiceParallelExecutor(
        targetParallelism, registry, transformEnforcements, context, metricsExecutor);
  }

  private ExecutorServiceParallelExecutor(
      int targetParallelism,
      TransformEvaluatorRegistry registry,
      Map<String, Collection<ModelEnforcementFactory>> transformEnforcements,
      EvaluationContext context,
      ExecutorService metricsExecutor) {
    this.targetParallelism = targetParallelism;
    this.metricsExecutor = metricsExecutor;
    // Don't use Daemon threads for workers. The Pipeline should continue to execute even if there
    // are no other active threads (for example, because waitUntilFinish was not called)
    this.executorService =
        Executors.newFixedThreadPool(
            targetParallelism,
            new ThreadFactoryBuilder()
                .setThreadFactory(MoreExecutors.platformThreadFactory())
                .setNameFormat("direct-runner-worker")
                .build());
    this.registry = registry;
    this.evaluationContext = context;

    // Weak Values allows TransformExecutorServices that are no longer in use to be reclaimed.
    // Executing TransformExecutorServices have a strong reference to their TransformExecutorService
    // which stops the TransformExecutorServices from being prematurely garbage collected
    serialExecutorServices =
        CacheBuilder.newBuilder()
            .weakValues()
            .removalListener(shutdownExecutorServiceListener())
            .build(serialTransformExecutorServiceCacheLoader());

    this.visibleUpdates = new QueueMessageReceiver();

    parallelExecutorService = TransformExecutorServices.parallel(executorService);
    executorFactory = new DirectTransformExecutor.Factory(context, registry, transformEnforcements);
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
    return notification -> {
      TransformExecutorService service = notification.getValue();
      if (service != null) {
        service.shutdown();
      }
    };
  }

  @Override
  // TODO: [https://github.com/apache/beam/issues/18968] Pass Future back to consumer to check for
  // async errors
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start(DirectGraph graph, RootProviderRegistry rootProviderRegistry) {
    int numTargetSplits = Math.max(3, targetParallelism);
    ImmutableMap.Builder<AppliedPTransform<?, ?, ?>, Queue<CommittedBundle<?>>> pendingRootBundles =
        ImmutableMap.builder();
    for (AppliedPTransform<?, ?, ?> root : graph.getRootTransforms()) {
      MetricsContainerImpl metricsContainer = new MetricsContainerImpl(root.getFullName());
      Queue<CommittedBundle<?>> pending = Queues.newArrayDeque();
      try (Closeable metricsScope = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
        Collection<CommittedBundle<?>> initialInputs =
            rootProviderRegistry.getInitialInputs(root, numTargetSplits);
        pending.addAll(initialInputs);
      } catch (Exception e) {
        throw UserCodeException.wrap(e);
      } finally {
        //  Metrics emitted initial split are reported along with the first bundle
        if (pending.peek() != null) {
          evaluationContext
              .getMetrics()
              .commitPhysical(pending.peek(), metricsContainer.getCumulative());
        }
      }
      pendingRootBundles.put(root, pending);
    }
    evaluationContext.initialize(pendingRootBundles.build());
    final ExecutionDriver executionDriver =
        QuiescenceDriver.create(
            evaluationContext, graph, this, visibleUpdates, pendingRootBundles.build());
    executorService.submit(
        new Runnable() {
          @Override
          public void run() {
            DriverState drive = executionDriver.drive();
            if (drive.isTerminal()) {
              State newPipelineState = State.UNKNOWN;
              switch (drive) {
                case FAILED:
                  newPipelineState = State.FAILED;
                  break;
                case SHUTDOWN:
                  newPipelineState = State.DONE;
                  break;
                case CONTINUE:
                  throw new IllegalStateException(
                      String.format("%s should not be a terminal state", DriverState.CONTINUE));
                default:
                  throw new IllegalArgumentException(
                      String.format("Unknown %s %s", DriverState.class.getSimpleName(), drive));
              }
              shutdownIfNecessary(newPipelineState);
            } else {
              executorService.submit(this);
            }
          }
        });
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(
      CommittedBundle<?> bundle,
      AppliedPTransform<?, ?, ?> consumer,
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
      // a reference to the scheduled DirectTransformExecutor callable. Follow-up TransformExecutors
      // (scheduled due to the completion of another DirectTransformExecutor) are provided to the
      // ExecutorService before the Earlier DirectTransformExecutor callable completes.
      transformExecutor = serialExecutorServices.getUnchecked(stepAndKey);
    } else {
      transformExecutor = parallelExecutorService;
    }

    TransformExecutor callable =
        executorFactory.create(bundle, transform, onComplete, transformExecutor);
    if (!pipelineState.get().isTerminal()) {
      transformExecutor.schedule(callable);
    }
  }

  private boolean isKeyed(PValue pvalue) {
    return evaluationContext.isKeyed(pvalue);
  }

  @Override
  public State waitUntilFinish(Duration duration) throws Exception {
    Instant completionTime;
    if (duration.equals(Duration.ZERO)) {
      completionTime = new Instant(Long.MAX_VALUE);
    } else {
      completionTime = Instant.now().plus(duration);
    }

    while (Instant.now().isBefore(completionTime)) {
      // Get an update; don't block forever if another thread has handled it. The call to poll will
      // wait the entire timeout; this call primarily exists to relinquish any core.
      VisibleExecutorUpdate update = visibleUpdates.tryNext(Duration.millis(25L));

      if (update == null && pipelineState.get().isTerminal()) {
        // state and updates have seperate locks so it is possible for an update
        // to be posted in a race. updates should arrive before the status is set
        // to a terminal state, so if there is one we should see it immediately.
        update = visibleUpdates.tryNext(Duration.millis(1L));
        if (update == null) {
          // there are no updates to process and no updates will ever be published because the
          // executor is shutdown
          return pipelineState.get();
        }
      }

      if (update != null) {
        if (isTerminalStateUpdate(update)) {
          return pipelineState.get();
        }

        if (update.thrown.isPresent()) {
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
    }

    return null;
  }

  @Override
  public State getPipelineState() {
    return pipelineState.get();
  }

  private boolean isTerminalStateUpdate(VisibleExecutorUpdate update) {
    return update.getNewState() != null && update.getNewState().isTerminal();
  }

  @Override
  public void stop() {
    shutdownIfNecessary(State.CANCELLED);
    visibleUpdates.cancelled();
  }

  private void shutdownIfNecessary(State newState) {
    if (!newState.isTerminal()) {
      return;
    }
    LOG.debug("Pipeline has terminated. Shutting down.");

    final Collection<Exception> errors = new ArrayList<>();
    // Stop accepting new work before shutting down the executor. This ensures that thread don't try
    // to add work to the shutdown executor.
    try {
      serialExecutorServices.invalidateAll();
    } catch (final RuntimeException re) {
      errors.add(re);
    }
    try {
      serialExecutorServices.cleanUp();
    } catch (final RuntimeException re) {
      errors.add(re);
    }
    try {
      parallelExecutorService.shutdown();
    } catch (final RuntimeException re) {
      errors.add(re);
    }
    try {
      executorService.shutdown();
    } catch (final RuntimeException re) {
      errors.add(re);
    }
    try {
      metricsExecutor.shutdown();
    } catch (final RuntimeException re) {
      errors.add(re);
    }
    try {
      registry.cleanup();
    } catch (final Exception e) {
      errors.add(e);
    }
    pipelineState.compareAndSet(State.RUNNING, newState); // ensure we hit a terminal node
    if (!errors.isEmpty()) {
      final IllegalStateException exception =
          new IllegalStateException(
              "Error"
                  + (errors.size() == 1 ? "" : "s")
                  + " during executor shutdown:\n"
                  + errors.stream()
                      .map(Exception::getMessage)
                      .collect(Collectors.joining("\n- ", "- ", "")));
      visibleUpdates.failed(exception);
      throw exception;
    }
  }

  /**
   * An update of interest to the user. Used in {@link #waitUntilFinish} to decide whether to return
   * normally or throw an exception.
   */
  private static class VisibleExecutorUpdate {
    private final Optional<? extends Throwable> thrown;
    private final @Nullable State newState;

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
      this.thrown = Optional.ofNullable(exception);
      this.newState = newState;
    }

    State getNewState() {
      return newState;
    }
  }

  private static class QueueMessageReceiver implements PipelineMessageReceiver {
    // If the type of BlockingQueue changes, ensure the findbugs filter is updated appropriately
    private final BlockingQueue<VisibleExecutorUpdate> updates = new LinkedBlockingQueue<>();

    @Override
    // updates is a non-capacity-limited LinkedBlockingQueue, which can never refuse an offered
    // update
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void failed(Exception e) {
      updates.offer(VisibleExecutorUpdate.fromException(e));
    }

    @Override
    // updates is a non-capacity-limited LinkedBlockingQueue, which can never refuse an offered
    // update
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void failed(Error e) {
      updates.offer(VisibleExecutorUpdate.fromError(e));
    }

    @Override
    // updates is a non-capacity-limited LinkedBlockingQueue, which can never refuse an offered
    // update
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void cancelled() {
      updates.offer(VisibleExecutorUpdate.cancelled());
    }

    @Override
    // updates is a non-capacity-limited LinkedBlockingQueue, which can never refuse an offered
    // update
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void completed() {
      updates.offer(VisibleExecutorUpdate.finished());
    }

    /** Try to get the next unconsumed message in this {@link QueueMessageReceiver}. */
    private @Nullable VisibleExecutorUpdate tryNext(Duration timeout) throws InterruptedException {
      return updates.poll(timeout.getMillis(), TimeUnit.MILLISECONDS);
    }
  }
}
