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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.util.Clock;
import com.google.api.services.dataflow.model.SideInputInfo;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingHandler;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementExecutionTracker;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ticker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Execution context for the Dataflow worker. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class DataflowExecutionContext<T extends DataflowStepContext> {

  private final CounterFactory counterFactory;
  private final MetricsContainerRegistry<?> metricsContainerRegistry;
  private final ExecutionStateTracker executionStateTracker;
  protected final DataflowExecutionStateRegistry executionStateRegistry;
  // Desired limit on amount of data sinked. Cumulative
  // across all the sinks, when there are more than one sinks.
  private final long sinkByteLimit;
  private long bytesSinked = 0;

  public DataflowExecutionContext(
      CounterFactory counterFactory,
      MetricsContainerRegistry<?> metricsRegistry,
      DataflowExecutionStateTracker executionStateTracker,
      DataflowExecutionStateRegistry executionStateRegistry,
      long sinkByteLimit) {
    this.counterFactory = counterFactory;
    this.metricsContainerRegistry = metricsRegistry;
    this.executionStateTracker = executionStateTracker;
    this.executionStateRegistry = executionStateRegistry;
    this.sinkByteLimit = sinkByteLimit;
  }

  // Created step contexts, keyed by step name
  private Map<String, T> cachedStepContexts = new LinkedHashMap<>();

  /**
   * Returns a {@link SideInputReader} based on {@link SideInputInfo} descriptors and {@link
   * PCollectionView PCollectionViews}.
   *
   * <p>If side input source metadata is provided by the service in {@link SideInputInfo
   * sideInputInfos}, we request a {@link SideInputReader} from the execution context using that
   * info. If no side input source metadata is provided but the DoFn expects side inputs, as a
   * fallback, we request a {@link SideInputReader} based only on the expected views.
   *
   * <p>These cases are not disjoint: Whenever a {@link GroupAlsoByWindowFn} takes side inputs,
   * {@code doFnInfo.getSideInputViews()} should be non-empty.
   *
   * <p>A note on the behavior of the Dataflow service: Today, the first case corresponds to batch
   * mode, while the fallback corresponds to streaming mode.
   */
  public SideInputReader getSideInputReader(
      @Nullable Iterable<? extends SideInputInfo> sideInputInfos,
      @Nullable Iterable<? extends PCollectionView<?>> views,
      DataflowOperationContext operationContext)
      throws Exception {
    if (sideInputInfos != null && !Iterables.isEmpty(sideInputInfos)) {
      return getSideInputReader(sideInputInfos, operationContext);
    } else if (views != null && !Iterables.isEmpty(views)) {
      return getSideInputReaderForViews(views);
    } else {
      return NullSideInputReader.empty();
    }
  }

  public CounterFactory getCounterFactory() {
    return counterFactory;
  }

  /** Returns a collection view of all of the {@link StepContext}s. */
  public Collection<? extends T> getAllStepContexts() {
    return Collections.unmodifiableCollection(cachedStepContexts.values());
  }

  /**
   * A hint for currently executing step if the context prefers stopping processing more input since
   * the sinks are 'full'. This is polled by readers to stop consuming more records, when they can.
   * Currently the hint is set only by the sinks in streaming.
   */
  boolean isSinkFullHintSet() {
    return bytesSinked >= sinkByteLimit;
    // In addition to hint from the sinks, we could consider other factors likes global memory
    // pressure.
    // TODO: We should have state bytes also to contribute to this hint, otherwise,
    //               the state size might grow unbounded.
  }

  protected final long getBytesSinked() {
    return bytesSinked;
  }

  /**
   * Sets a flag to indicate that a sink has enough data written to it. This hint is read by
   * upstream producers to stop producing if they can. Mainly used in streaming.
   */
  void reportBytesSinked(long bytes) {
    bytesSinked += bytes;
  }

  protected void clearSinkFullHint() {
    // Cleared in Streaming when the context is reused for new work item.
    bytesSinked = 0;
  }

  /**
   * Returns a {@link SideInputReader} for all the side inputs described in the given {@link
   * SideInputInfo} descriptors.
   */
  protected abstract SideInputReader getSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos, DataflowOperationContext operationContext)
      throws Exception;

  protected abstract T createStepContext(DataflowOperationContext operationContext);

  // TODO: Move StepContext creation to the OperationContext.
  public T getStepContext(DataflowOperationContext operationContext) {
    NameContext nameContext = operationContext.nameContext();
    T context = cachedStepContexts.get(nameContext.systemName());
    if (context == null) {
      context = createStepContext(operationContext);
      cachedStepContexts.put(nameContext.systemName(), context);
    }
    return context;
  }

  /**
   * Returns a {@link SideInputReader} for all the provided views, where the execution context
   * itself knows how to read data for the view.
   */
  protected abstract SideInputReader getSideInputReaderForViews(
      Iterable<? extends PCollectionView<?>> views) throws Exception;

  /** Dataflow specific {@link StepContext}. */
  public abstract static class DataflowStepContext implements StepContext {

    private final NameContext nameContext;

    public DataflowStepContext(NameContext nameContext) {
      this.nameContext = nameContext;
    }

    public NameContext getNameContext() {
      return nameContext;
    }

    /**
     * Returns the next fired timer for this step.
     *
     * <p>The {@code windowCoder} is passed here as it is more convenient than doing so when the
     * {@link DataflowStepContext} is created.
     */
    public abstract @Nullable <W extends BoundedWindow> TimerData getNextFiredTimer(
        Coder<W> windowCoder);

    public abstract <W extends BoundedWindow> void setStateCleanupTimer(
        String timerId,
        W window,
        Coder<W> windowCoder,
        Instant cleanupTime,
        Instant cleanupOutputTimestamp);

    public abstract DataflowStepContext namespacedToUser();
  }

  /**
   * Creates the context for an operation with the stage this execution context belongs to.
   *
   * @param nameContext the set of names identifying the operation
   */
  public DataflowOperationContext createOperationContext(NameContext nameContext) {
    MetricsContainer metricsContainer =
        metricsContainerRegistry.getContainer(
            checkNotNull(
                nameContext.originalName(),
                "All operations must have an original name, but %s doesn't.",
                nameContext));
    return new DataflowOperationContext(
        counterFactory,
        nameContext,
        metricsContainer,
        executionStateTracker,
        executionStateRegistry);
  }

  protected MetricsContainerRegistry<?> getMetricsContainerRegistry() {
    return metricsContainerRegistry;
  }

  protected DataflowExecutionStateRegistry getExecutionStateRegistry() {
    return executionStateRegistry;
  }

  public ExecutionStateTracker getExecutionStateTracker() {
    return executionStateTracker;
  }

  /**
   * An extension of {@link ExecutionStateTracker} that also installs a {@link MetricsContainer}
   * using the current state to locate the {@link MetricsEnvironment}.
   */
  public static class DataflowExecutionStateTracker extends ExecutionStateTracker {

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final StepProcessingStats[] EMPTY_STEP_STATS = new StepProcessingStats[0];

    private static final AtomicLongFieldUpdater<DataflowExecutionStateTracker> ACTIVE_SEQ_UPDATER =
        AtomicLongFieldUpdater.newUpdater(DataflowExecutionStateTracker.class, "activeSeq");
    private static final AtomicReferenceFieldUpdater<DataflowExecutionStateTracker, String>
        ACTIVE_STEP_NAME_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                DataflowExecutionStateTracker.class, String.class, "activeStepName");
    private static final AtomicLongFieldUpdater<DataflowExecutionStateTracker>
        ACTIVE_START_NANOS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(
                DataflowExecutionStateTracker.class, "activeStartNanos");

    @SuppressWarnings("ThreadPriorityCheck") // Seqlock reader backoff for a preempted writer.
    private static void seqlockBackoff(int attempt) {
      if (attempt < 16) {
        Thread.onSpinWait();
      } else {
        Thread.yield();
      }
    }

    /**
     * Lock-free per-step processing time summary written exclusively by the tracked execution
     * thread (via release stores) and snapshotted by concurrent heartbeat/lull/completion reader
     * threads via a sequence lock.
     */
    static final class StepProcessingStats {
      private static final AtomicLongFieldUpdater<StepProcessingStats> SEQ_UPDATER =
          AtomicLongFieldUpdater.newUpdater(StepProcessingStats.class, "seq");
      private static final AtomicLongFieldUpdater<StepProcessingStats> COUNT_UPDATER =
          AtomicLongFieldUpdater.newUpdater(StepProcessingStats.class, "count");
      private static final AtomicLongFieldUpdater<StepProcessingStats> SUM_UPDATER =
          AtomicLongFieldUpdater.newUpdater(StepProcessingStats.class, "sum");
      private static final AtomicIntegerFieldUpdater<StepProcessingStats> MIN_UPDATER =
          AtomicIntegerFieldUpdater.newUpdater(StepProcessingStats.class, "min");
      private static final AtomicIntegerFieldUpdater<StepProcessingStats> MAX_UPDATER =
          AtomicIntegerFieldUpdater.newUpdater(StepProcessingStats.class, "max");

      final String stepName;

      // Non-volatile shadows exclusively accessed by the execution thread (or under tracker lock
      // during reset).
      private long localSeq;
      private long localCount;
      private long localSum;
      private int localMin = Integer.MAX_VALUE;
      private int localMax = Integer.MIN_VALUE;

      // Volatile fields written via lazySet (store-release) by the execution thread and read by
      // concurrent reader threads. Even seq = stable, odd seq = multi-field update in progress.
      private volatile long seq;
      private volatile long count;
      private volatile long sum;
      private volatile int min = Integer.MAX_VALUE;
      private volatile int max = Integer.MIN_VALUE;

      StepProcessingStats(String stepName) {
        this.stepName = stepName;
      }

      void recordZeroMillis() {
        // Common fast path: once min <= 0 <= max is established, sum/min/max do not change for a
        // 0 ms element, so only count needs a single release store.
        if (localMin <= 0 && localMax >= 0) {
          COUNT_UPDATER.lazySet(this, ++localCount);
          return;
        }
        record(0);
      }

      void record(int processingTimeMillis) {
        long nextSeq = ++localSeq;
        SEQ_UPDATER.lazySet(this, nextSeq);
        long newCount = ++localCount;
        COUNT_UPDATER.lazySet(this, newCount);
        if (processingTimeMillis != 0) {
          long newSum = localSum + processingTimeMillis;
          localSum = newSum;
          SUM_UPDATER.lazySet(this, newSum);
        }
        if (processingTimeMillis < localMin) {
          localMin = processingTimeMillis;
          MIN_UPDATER.lazySet(this, processingTimeMillis);
        }
        if (processingTimeMillis > localMax) {
          localMax = processingTimeMillis;
          MAX_UPDATER.lazySet(this, processingTimeMillis);
        }
        SEQ_UPDATER.lazySet(this, ++localSeq);
      }

      void reset() {
        long nextSeq = ++localSeq;
        SEQ_UPDATER.lazySet(this, nextSeq);
        localCount = 0L;
        localSum = 0L;
        localMin = Integer.MAX_VALUE;
        localMax = Integer.MIN_VALUE;
        COUNT_UPDATER.lazySet(this, 0L);
        SUM_UPDATER.lazySet(this, 0L);
        MIN_UPDATER.lazySet(this, Integer.MAX_VALUE);
        MAX_UPDATER.lazySet(this, Integer.MIN_VALUE);
        SEQ_UPDATER.lazySet(this, ++localSeq);
      }

      @Nullable IntSummaryStatistics snapshot() {
        int attempt = 0;
        while (true) {
          long s1 = seq;
          if ((s1 & 1L) != 0L) {
            seqlockBackoff(attempt);
            if (attempt < 16) {
              attempt++;
            }
            continue;
          }
          long c = count;
          if (c == 0L) {
            if (seq == s1) {
              return null;
            }
            seqlockBackoff(attempt);
            if (attempt < 16) {
              attempt++;
            }
            continue;
          }
          long total = sum;
          int lo = min;
          int hi = max;
          if (seq == s1 && lo <= hi) {
            return new IntSummaryStatistics(c, lo, hi, total);
          }
          seqlockBackoff(attempt);
          if (attempt < 16) {
            attempt++;
          }
        }
      }
    }

    private final ElementExecutionTracker elementExecutionTracker;
    private final DataflowOperationContext.DataflowExecutionState otherState;
    private final ContextActivationObserverRegistry contextActivationObserverRegistry;
    private final String workItemId;

    private final boolean isStreaming;
    private final boolean isSimpleStateTransition;
    private final boolean supportsFastProcessTransition;

    // Sequence lock guarding (activeStepName, activeStartNanos). Even activeSeq = stable,
    // odd activeSeq = update in progress. Strictly monotonic across updates and reset() so
    // concurrent readers never observe torn pairs or ABA across fused steps / repeated elements /
    // resets.
    private volatile long activeSeq = 0L;
    private volatile @Nullable String activeStepName = null;
    private volatile long activeStartNanos = 0L;

    // Non-volatile shadows exclusively accessed by the execution thread (or under lock in reset()).
    private long localActiveSeq = 0L;
    private @Nullable StepProcessingStats localActiveStepStats = null;
    private long localActiveStartNanos = 0L;
    private @Nullable String lastLookupStepName = null;
    private @Nullable StepProcessingStats lastLookupStepStats = null;

    /** Clock used to either provide real system time or mocked to virtualize time for testing. */
    private final Clock clock;

    // Registered step stats: map guarded by this only on one-time registration; copy-on-write
    // array enables lock-free iteration on reads and lock-free lookup on enterState().
    @GuardedBy("this")
    private final Map<String, StepProcessingStats> stepStatsByName = new HashMap<>();

    private volatile StepProcessingStats[] stepStatsArray = EMPTY_STEP_STATS;

    private static final Logger LOG = LoggerFactory.getLogger(DataflowExecutionStateTracker.class);

    private static final PeriodFormatter DURATION_FORMATTER =
        new PeriodFormatterBuilder()
            .appendDays()
            .appendSuffix("d")
            .minimumPrintedDigits(2)
            .appendHours()
            .appendSuffix("h")
            .printZeroAlways()
            .appendMinutes()
            .appendSuffix("m")
            .appendSeconds()
            .appendSuffix("s")
            .toFormatter();

    public DataflowExecutionStateTracker(
        ExecutionStateSampler sampler,
        DataflowOperationContext.DataflowExecutionState otherState,
        CounterFactory counterFactory,
        PipelineOptions options,
        String workItemId) {
      this(sampler, otherState, counterFactory, options, workItemId, Clock.SYSTEM);
    }

    @VisibleForTesting
    public DataflowExecutionStateTracker(
        ExecutionStateSampler sampler,
        DataflowOperationContext.DataflowExecutionState otherState,
        CounterFactory counterFactory,
        PipelineOptions options,
        String workItemId,
        Clock clock) {
      super(sampler);
      this.elementExecutionTracker =
          DataflowElementExecutionTracker.create(counterFactory, options);
      this.otherState = otherState;
      this.workItemId = workItemId;
      this.contextActivationObserverRegistry = ContextActivationObserverRegistry.createDefault();
      this.clock = clock;
      DataflowWorkerLoggingInitializer.initialize();
      if (options instanceof DataflowPipelineOptions) {
        this.isStreaming = ((DataflowPipelineOptions) options).isStreaming();
      } else {
        this.isStreaming = false;
      }
      boolean noOpElementTracker =
          this.elementExecutionTracker == ElementExecutionTracker.NO_OP_INSTANCE;
      this.isSimpleStateTransition = !this.isStreaming && noOpElementTracker;
      this.supportsFastProcessTransition = noOpElementTracker;
    }

    @Override
    public boolean supportsDirectTransition() {
      return getClass() == DataflowExecutionStateTracker.class && isSimpleStateTransition;
    }

    boolean supportsFastProcessElementTransition() {
      return getClass() == DataflowExecutionStateTracker.class && supportsFastProcessTransition;
    }

    boolean isStreaming() {
      return isStreaming;
    }

    @Override
    public synchronized void reset() {
      super.reset();
      localActiveStepStats = null;
      localActiveStartNanos = 0L;
      ACTIVE_SEQ_UPDATER.lazySet(this, ++localActiveSeq);
      ACTIVE_STEP_NAME_UPDATER.lazySet(this, null);
      ACTIVE_START_NANOS_UPDATER.lazySet(this, 0L);
      ACTIVE_SEQ_UPDATER.lazySet(this, ++localActiveSeq);
      lastLookupStepName = null;
      lastLookupStepStats = null;
      StepProcessingStats[] statsArr = this.stepStatsArray;
      for (int i = 0; i < statsArr.length; i++) {
        statsArr[i].reset();
      }
    }

    synchronized StepProcessingStats getOrCreateStepStats(String userStepName) {
      StepProcessingStats existing = stepStatsByName.get(userStepName);
      if (existing != null) {
        return existing;
      }
      StepProcessingStats created = new StepProcessingStats(userStepName);
      stepStatsByName.put(userStepName, created);
      StepProcessingStats[] oldArr = this.stepStatsArray;
      StepProcessingStats[] newArr = Arrays.copyOf(oldArr, oldArr.length + 1);
      newArr[oldArr.length] = created;
      this.stepStatsArray = newArr;
      return created;
    }

    @SuppressWarnings("ReferenceEquality")
    private StepProcessingStats getStepStatsFast(String userStepName) {
      if (userStepName == lastLookupStepName && lastLookupStepStats != null) {
        return lastLookupStepStats;
      }
      StepProcessingStats[] arr = this.stepStatsArray;
      for (int i = 0; i < arr.length; i++) {
        StepProcessingStats candidate = arr[i];
        if (candidate.stepName == userStepName || candidate.stepName.equals(userStepName)) {
          lastLookupStepName = userStepName;
          lastLookupStepStats = candidate;
          return candidate;
        }
      }
      StepProcessingStats stats = getOrCreateStepStats(userStepName);
      lastLookupStepName = userStepName;
      lastLookupStepStats = stats;
      return stats;
    }

    private static void recordElapsedNanos(StepProcessingStats stats, long elapsedNanos) {
      if (elapsedNanos < NANOS_PER_MILLI) {
        stats.recordZeroMillis();
      } else {
        stats.record((int) Math.min(elapsedNanos / NANOS_PER_MILLI, Integer.MAX_VALUE));
      }
    }

    private void setActiveMessage(StepProcessingStats stepStats, long startNanos) {
      String stepName = stepStats.stepName;
      localActiveStepStats = stepStats;
      localActiveStartNanos = startNanos;
      ACTIVE_SEQ_UPDATER.lazySet(this, ++localActiveSeq);
      ACTIVE_STEP_NAME_UPDATER.lazySet(this, stepName);
      ACTIVE_START_NANOS_UPDATER.lazySet(this, startNanos);
      ACTIVE_SEQ_UPDATER.lazySet(this, ++localActiveSeq);
    }

    private void clearActiveMessage() {
      localActiveStepStats = null;
      ACTIVE_SEQ_UPDATER.lazySet(this, ++localActiveSeq);
      ACTIVE_STEP_NAME_UPDATER.lazySet(this, null);
      ACTIVE_SEQ_UPDATER.lazySet(this, ++localActiveSeq);
    }

    /**
     * Allocation-free, lock-free streaming counterpart to {@link #enterState(ExecutionState)} for
     * {@code process} states with a pre-registered {@link StepProcessingStats}. Must be paired with
     * {@link #exitStreamingProcessDirect(ExecutionState)}.
     */
    final @Nullable ExecutionState enterStreamingProcessDirect(
        ExecutionState newState, StepProcessingStats stepStats) {
      final ExecutionState previous = enterStateDirect(newState);
      long nowNanos = System.nanoTime();
      StepProcessingStats prevActive = localActiveStepStats;
      if (prevActive != null) {
        recordElapsedNanos(prevActive, nowNanos - localActiveStartNanos);
      }
      setActiveMessage(stepStats, nowNanos);
      return previous;
    }

    /**
     * Allocation-free, lock-free streaming counterpart to closing the {@link Closeable} returned by
     * {@link #enterState(ExecutionState)} for {@code process} states.
     */
    final void exitStreamingProcessDirect(@Nullable ExecutionState previous) {
      StepProcessingStats active = localActiveStepStats;
      if (active != null) {
        recordElapsedNanos(active, System.nanoTime() - localActiveStartNanos);
        clearActiveMessage();
      }
      exitStateDirect(previous);
    }

    @Override
    public Closeable activate() {
      Closer closer = Closer.create();
      try {
        closer.register(super.activate());
        for (ContextActivationObserver p :
            contextActivationObserverRegistry.getContextActivationObservers()) {
          closer.register(p.activate(this));
        }
        closer.register(enterState(otherState));
        return closer;
      } catch (Exception e) {
        try {
          closer.close();
        } catch (IOException suppressed) {
          // Shouldn't happen -- none of the things being closed actually throw.
          e.addSuppressed(suppressed);
        }
        throw e;
      }
    }

    private String getBundleLullMessage(Thread trackedThread, Duration lullDuration) {
      StringBuilder message = new StringBuilder();
      message
          .append("Operation ongoing in bundle for at least ")
          .append(DURATION_FORMATTER.print(lullDuration.toPeriod()))
          .append(" without completing")
          .append("\n");
      Optional<ActiveMessageMetadata> activeMetadata = getActiveMessageMetadata();
      if (activeMetadata.isPresent()) {
        message
            .append("Current user step name: ")
            .append(activeMetadata.get().userStepName())
            .append("\n");
        message
            .append("Time spent in this step(millis): ")
            .append(
                clock.currentTimeMillis()
                    - activeMetadata.get().stopwatch().elapsed(TimeUnit.MILLISECONDS))
            .append("\n");
      }
      message.append("Processing times in each step(millis)\n");
      for (Map.Entry<String, IntSummaryStatistics> entry :
          getProcessingTimesByStepCopy().entrySet()) {
        message.append("Step name: ").append(entry.getKey()).append("\n");
        message
            .append("Time spent in this step: ")
            .append(entry.getValue().toString())
            .append("\n");
      }

      if (trackedThread != null) {
        message.append(StackTraceUtil.getStackTraceForLullMessage(trackedThread.getStackTrace()));
      }
      return message.toString();
    }

    @Override
    protected void takeSampleOnce(long millisSinceLastSample) {
      elementExecutionTracker.takeSample(millisSinceLastSample);
      super.takeSampleOnce(millisSinceLastSample);
    }

    @Override
    protected void reportBundleLull(Thread trackedThread, long millisElapsedSinceBundleStart) {
      // If we're not logging warnings, nothing to report.
      if (!LOG.isWarnEnabled()) {
        return;
      }

      Duration lullDuration = Duration.millis(millisElapsedSinceBundleStart);

      // Since the lull reporting executes in the sampler thread, it won't automatically inherit the
      // context of the current step. To ensure things are logged correctly, we get the currently
      // registered DataflowWorkerLoggingHandler and log directly in the desired context.
      LogRecord logRecord =
          new LogRecord(Level.WARNING, getBundleLullMessage(trackedThread, lullDuration));
      logRecord.setLoggerName(DataflowExecutionStateTracker.LOG.getName());

      // Publish directly in the context of this specific ExecutionState.
      DataflowWorkerLoggingHandler dataflowLoggingHandler =
          DataflowWorkerLoggingInitializer.getLoggingHandler();
      dataflowLoggingHandler.publish(logRecord);
    }

    /**
     * Enter a new state on the tracker. If the new state is a Dataflow processing state, tracks the
     * activeMessageMetadata with the start time of the new state.
     */
    @Override
    public Closeable enterState(ExecutionState newState) {
      if (isSimpleStateTransition) {
        return super.enterState(newState);
      }
      final boolean isDataflowProcessElementState =
          newState instanceof DataflowExecutionState && newState.isProcessElementState;
      if (!isDataflowProcessElementState) {
        return super.enterState(newState);
      }
      DataflowExecutionState newDFState = (DataflowExecutionState) newState;
      final ExecutionState previous = enterStateDirect(newState);
      if (isStreaming) {
        NameContext stepName = newDFState.getStepName();
        String userName = stepName != null ? stepName.userName() : null;
        if (userName != null) {
          StepProcessingStats stepStats = getStepStatsFast(userName);
          long nowNanos = System.nanoTime();
          StepProcessingStats prevActive = localActiveStepStats;
          if (prevActive != null) {
            recordElapsedNanos(prevActive, nowNanos - localActiveStartNanos);
          }
          setActiveMessage(stepStats, nowNanos);
        }
      }
      final boolean trackElementExecution =
          elementExecutionTracker != ElementExecutionTracker.NO_OP_INSTANCE;
      if (trackElementExecution) {
        elementExecutionTracker.enter(newDFState.getStepName());
      }

      return () -> {
        if (isStreaming) {
          StepProcessingStats active = localActiveStepStats;
          if (active != null) {
            recordElapsedNanos(active, System.nanoTime() - localActiveStartNanos);
            clearActiveMessage();
          }
        }
        if (trackElementExecution) {
          elementExecutionTracker.exit();
        }
        exitStateDirect(previous);
      };
    }

    public String getWorkItemId() {
      return this.workItemId;
    }

    private static Stopwatch createStopwatchStartedAt(long startNanos) {
      Ticker ticker =
          new Ticker() {
            private boolean firstRead = true;

            @Override
            public long read() {
              if (firstRead) {
                firstRead = false;
                return startNanos;
              }
              return System.nanoTime();
            }
          };
      return Stopwatch.createStarted(ticker);
    }

    public Optional<ActiveMessageMetadata> getActiveMessageMetadata() {
      int attempt = 0;
      while (true) {
        long s1 = activeSeq;
        if ((s1 & 1L) != 0L) {
          seqlockBackoff(attempt);
          if (attempt < 16) {
            attempt++;
          }
          continue;
        }
        String stepName = activeStepName;
        if (stepName == null) {
          if (activeSeq == s1) {
            return Optional.empty();
          }
          seqlockBackoff(attempt);
          if (attempt < 16) {
            attempt++;
          }
          continue;
        }
        long startNanos = activeStartNanos;
        if (activeSeq == s1) {
          return Optional.of(
              ActiveMessageMetadata.create(stepName, createStopwatchStartedAt(startNanos)));
        }
        seqlockBackoff(attempt);
        if (attempt < 16) {
          attempt++;
        }
      }
    }

    public Map<String, IntSummaryStatistics> getProcessingTimesByStepCopy() {
      StepProcessingStats[] statsArray = this.stepStatsArray;
      if (statsArray.length == 0) {
        return new HashMap<>();
      }
      Map<String, IntSummaryStatistics> processingTimesCopy =
          new HashMap<>(Math.max(4, statsArray.length * 2));
      for (int i = 0; i < statsArray.length; i++) {
        StepProcessingStats stats = statsArray[i];
        IntSummaryStatistics snapshot = stats.snapshot();
        if (snapshot != null) {
          processingTimesCopy.put(stats.stepName, snapshot);
        }
      }
      return processingTimesCopy;
    }
  }
}
