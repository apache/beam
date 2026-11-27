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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.LogRecord;
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
   * using the current state to locate the {@link MetricsEnvironment}, and tracks streaming
   * active-element latency and per-step processing-time distributions on the hot execution path.
   *
   * <p>To avoid locks and per-element object allocations (such as {@link Stopwatch} / {@link
   * ActiveMessageMetadata} instances), this tracker exposes mutable streaming state to concurrent
   * heartbeat, lull-reporting, and work-completion reader threads using two single-writer,
   * multi-reader sequence locks (seqlocks) implemented with {@link VarHandle}:
   *
   * <ul>
   *   <li>{@code activeSeq} guards the currently executing {@code (activeStepName,
   *       activeStartNanos)} pair read by {@link #getActiveMessageMetadata()}.
   *   <li>{@code StepProcessingStats.seq} guards each step's {@code (count, sum, min, max)}
   *       statistics read by {@link #getProcessingTimesByStepCopy()}.
   * </ul>
   *
   * <p>Registered {@code StepProcessingStats} instances are also published without locks via a
   * monotonically growing {@code stepStatsBuffer} + volatile {@code stepStatsCount} pair.
   *
   * <p>All mutations on the hot path are performed by the single tracked execution thread; cold
   * lifecycle transitions ({@link #activate()}, {@link #deactivate()}, {@link #reset()}) hold the
   * tracker's monitor lock, establishing happens-before across thread handoffs between bundles.
   */
  public static class DataflowExecutionStateTracker extends ExecutionStateTracker {

    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final StepProcessingStats[] EMPTY_STEP_STATS = new StepProcessingStats[0];
    private static final int MAX_ACTIVE_METADATA_ATTEMPTS = 64;
    private static final long SEQLOCK_PARK_NANOS = 10_000L;

    private static final VarHandle ACTIVE_SEQ;
    private static final VarHandle ACTIVE_STEP_NAME;
    private static final VarHandle ACTIVE_START_NANOS;

    static {
      try {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ACTIVE_SEQ =
            lookup.findVarHandle(DataflowExecutionStateTracker.class, "activeSeq", long.class);
        ACTIVE_STEP_NAME =
            lookup.findVarHandle(
                DataflowExecutionStateTracker.class, "activeStepName", String.class);
        ACTIVE_START_NANOS =
            lookup.findVarHandle(
                DataflowExecutionStateTracker.class, "activeStartNanos", long.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    @SuppressWarnings("ThreadPriorityCheck") // Seqlock reader backoff for a preempted writer.
    private static void seqlockBackoff(int attempt) {
      if (attempt < 16) {
        Thread.onSpinWait();
      } else if (attempt < 32 || Thread.currentThread().isInterrupted()) {
        // LockSupport.parkNanos returns immediately while the interrupt flag is set; yield
        // instead to avoid busy-spinning without clearing the interrupt status.
        Thread.yield();
      } else {
        LockSupport.parkNanos(SEQLOCK_PARK_NANOS);
      }
    }

    /**
     * Lock-free per-step processing time summary written exclusively by the tracked execution
     * thread via {@link VarHandle} stores and read concurrently by heartbeat, lull-reporting, and
     * completion threads using a sequence lock ({@code seq}).
     *
     * <p><b>Seqlock protocol ({@code seq}):</b> {@code seq} is a strictly monotonic version counter
     * whose parity encodes whether a multi-field update is in progress:
     *
     * <ul>
     *   <li><b>Even {@code seq}:</b> {@code (count, sum, min, max)} is consistent and readable.
     *   <li><b>Odd {@code seq}:</b> the writer is updating one or more of those fields; readers
     *       must back off and retry rather than trusting the payload.
     * </ul>
     *
     * <p>Every non-trivial update ({@link #record(int)}) and {@link #reset()} advances {@code seq}
     * {@code s → s+1 (odd) → s+2 (even)} without ever resetting it to zero, preventing ABA races
     * across sequential writes or cross-bundle resets.
     *
     * <p><b>Barrier pairing (x86_64/TSO and ARM64/weak memory models):</b>
     *
     * <pre>{@code
     * Writer (record):                       Reader (snapshot):
     *   SEQ.setOpaque(s + 1)   // odd          s1 = SEQ.getAcquire()   // must be even
     *   storeStoreFence()      // dmb ishst    c/sum/min/max = *.getOpaque()
     *   count/sum/min/max = *.setOpaque(...)   loadLoadFence()         // dmb ishld
     *   SEQ.setRelease(s + 2)  // even         s2 = SEQ.getOpaque()    // verify s2 == s1
     * }</pre>
     *
     * {@link VarHandle#storeStoreFence()} (rather than {@code setRelease(s + 1)}) is required on
     * entry to the write window because a release store only prevents <em>earlier</em> stores from
     * drifting after it, not <em>later</em> payload stores from hoisting ahead of it. On the reader
     * side, {@code getAcquire()} orders {@code s1} before payload loads (and synchronizes with the
     * writer's prior {@code setRelease(s1)}), and {@link VarHandle#loadLoadFence()} stops the
     * closing {@code s2 = SEQ.getOpaque()} validation load from hoisting ahead of the payload
     * loads. Both fences are free compiler barriers on x86_64.
     *
     * <p><b>Fast path for 0 ms elements ({@link #recordZeroMillis()}):</b> once at least one sample
     * with {@code 0} has been recorded (so {@code min <= 0 <= max}), another 0 ms element changes
     * only {@code count} — {@code sum}, {@code min}, and {@code max} stay invariant. In that case
     * the writer executes a single {@code COUNT.setRelease(count + 1)} <em>without</em> bumping
     * {@code seq}. Because {@code s1 = SEQ.getAcquire()} in {@link #snapshot()} already makes the
     * published {@code sum/min/max} visible, observing either {@code c} or {@code c + 1} produces a
     * valid, linearizable snapshot while keeping {@code seq} completely quiet for sub-millisecond
     * elements (avoiding both writer barriers and reader retry churn).
     *
     * <p><b>Plain single-writer reads:</b> the execution thread reads its own fields with plain
     * {@link VarHandle#get} instead of maintaining duplicate shadow fields; cross-thread {@link
     * #reset()} and {@link DataflowExecutionStateTracker#activate()} are both {@code synchronized}
     * on the enclosing tracker so happens-before holds across bundle handoffs. Without duplicate
     * shadows this object is ~48 bytes (fitting in a single 64-byte cache line under compressed
     * OOPs).
     */
    static final class StepProcessingStats {
      private static final VarHandle SEQ;
      private static final VarHandle COUNT;
      private static final VarHandle SUM;
      private static final VarHandle MIN;
      private static final VarHandle MAX;

      static {
        try {
          MethodHandles.Lookup lookup = MethodHandles.lookup();
          SEQ = lookup.findVarHandle(StepProcessingStats.class, "seq", long.class);
          COUNT = lookup.findVarHandle(StepProcessingStats.class, "count", long.class);
          SUM = lookup.findVarHandle(StepProcessingStats.class, "sum", long.class);
          MIN = lookup.findVarHandle(StepProcessingStats.class, "min", int.class);
          MAX = lookup.findVarHandle(StepProcessingStats.class, "max", int.class);
        } catch (ReflectiveOperationException e) {
          throw new ExceptionInInitializerError(e);
        }
      }

      final String stepName;

      // Single-writer fields read via plain VarHandle.get() by the execution thread and via
      // acquire/opaque loads by concurrent reader threads. Declared volatile so Java's memory
      // model guarantees 64-bit atomicity even if accessed directly; all hot-path reads/writes
      // route through the VarHandles above to avoid unnecessary fences.
      @SuppressWarnings("UnusedVariable")
      private volatile long seq;

      @SuppressWarnings("UnusedVariable")
      private volatile long count;

      @SuppressWarnings("UnusedVariable")
      private volatile long sum;

      @SuppressWarnings("UnusedVariable")
      private volatile int min = Integer.MAX_VALUE;

      @SuppressWarnings("UnusedVariable")
      private volatile int max = Integer.MIN_VALUE;

      StepProcessingStats(String stepName) {
        this.stepName = stepName;
      }

      void recordZeroMillis() {
        // Common fast path: once min <= 0 <= max is already published, a 0 ms sample leaves
        // sum/min/max unchanged, so only count needs a single release store (no seqlock bump).
        // First 0 ms sample falls through to record(0) so seq bumps once and establishes min = 0.
        if ((int) MIN.get(this) <= 0 && (int) MAX.get(this) >= 0) {
          COUNT.setRelease(this, (long) COUNT.get(this) + 1L);
          return;
        }
        record(0);
      }

      void record(int processingTimeMillis) {
        // Only primitive ops occur inside the odd window, so no exception can leave seq stuck odd.
        long s = (long) SEQ.get(this);
        // 1. Open write window (even -> odd). storeStoreFence ensures readers observing any new
        //    payload value below will also see seq >= s + 1 on their closing seq check.
        SEQ.setOpaque(this, s + 1L);
        VarHandle.storeStoreFence();
        // 2. Opaque payload writes prevent 64-bit tearing and dead-store elimination without
        // fences.
        COUNT.setOpaque(this, (long) COUNT.get(this) + 1L);
        if (processingTimeMillis != 0) {
          SUM.setOpaque(this, (long) SUM.get(this) + processingTimeMillis);
        }
        if (processingTimeMillis < (int) MIN.get(this)) {
          MIN.setOpaque(this, processingTimeMillis);
        }
        if (processingTimeMillis > (int) MAX.get(this)) {
          MAX.setOpaque(this, processingTimeMillis);
        }
        // 3. Close write window (odd -> even). Release semantics guarantee all payload writes
        //    above are visible to any reader whose SEQ.getAcquire() observes s + 2.
        SEQ.setRelease(this, s + 2L);
      }

      void reset() {
        // Cold path called under the tracker's monitor lock between bundles. Seed from volatile seq
        // (never reset to 0) so a cross-thread reset() strictly advances past any sequence a
        // concurrent reader may be validating, prevents ABA, and heals odd parity if ever skewed.
        long s = (long) SEQ.getVolatile(this);
        long odd = (s & 1L) != 0L ? s + 2L : s + 1L;
        SEQ.setVolatile(this, odd);
        COUNT.setVolatile(this, 0L);
        SUM.setVolatile(this, 0L);
        MIN.setVolatile(this, Integer.MAX_VALUE);
        MAX.setVolatile(this, Integer.MIN_VALUE);
        SEQ.setVolatile(this, odd + 1L);
      }

      @VisibleForTesting
      long getSeqForTesting() {
        return (long) SEQ.getVolatile(this);
      }

      /**
       * Returns a consistent snapshot of this step's statistics, or {@code null} if {@code count ==
       * 0}. Retries are unbounded because completion/lull snapshots require exact per-step counts
       * and {@code seq} only advances on {@code >= 1 ms} elements (sub-ms completions take the
       * seq-free {@link #recordZeroMillis()} fast path).
       */
      @Nullable IntSummaryStatistics snapshot() {
        int attempt = 0;
        while (true) {
          // 1. Read starting seq with acquire semantics, synchronizing with the writer's closing
          //    SEQ.setRelease(s1) so all fields from version s1 are visible below.
          long s1 = (long) SEQ.getAcquire(this);
          if ((s1 & 1L) != 0L) {
            // Writer is inside record() or reset(); wait for an even sequence number.
            seqlockBackoff(attempt);
            if (attempt < 32) {
              attempt++;
            }
            continue;
          }
          // 2. Read payload optimistically with opaque loads (atomic, un-hoisted).
          long c = (long) COUNT.getOpaque(this);
          if (c == 0L) {
            // Confirm count == 0 wasn't caught mid-reset or mid-first-record before returning null.
            VarHandle.loadLoadFence();
            if ((long) SEQ.getOpaque(this) == s1) {
              return null;
            }
            seqlockBackoff(attempt);
            if (attempt < 32) {
              attempt++;
            }
            continue;
          }
          long total = (long) SUM.getOpaque(this);
          int lo = (int) MIN.getOpaque(this);
          int hi = (int) MAX.getOpaque(this);
          // 3. loadLoadFence keeps all payload loads ordered before the closing seq check.
          //    If seq is still s1, no record() or reset() overlapped our read window (at most
          //    seq-free recordZeroMillis() increments of count ran, keeping sum/min/max valid).
          //    lo <= hi guards IntSummaryStatistics' constructor precondition defensively.
          VarHandle.loadLoadFence();
          if ((long) SEQ.getOpaque(this) == s1 && lo <= hi) {
            return new IntSummaryStatistics(c, lo, hi, total);
          }
          seqlockBackoff(attempt);
          if (attempt < 32) {
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

    // Sequence lock guarding (activeStepName, activeStartNanos). Uses the same even/odd protocol
    // and VarHandle fence pairing as StepProcessingStats.seq:
    //   - Even activeSeq: stable, readable (activeStepName, activeStartNanos) snapshot.
    //   - Odd activeSeq: writer is in setActiveMessage() or reset(); readers must retry.
    // Strictly monotonic (never reset to 0) so readers cannot confuse a reused pair across fused
    // steps, repeated elements, or bundle resets (no ABA). clearActiveMessage() does not bump
    // activeSeq: it only sets activeStepName to null via setRelease(), leaving activeStartNanos
    // unchanged so a concurrent reader still observes either empty or the prior matching pair.
    @SuppressWarnings("UnusedVariable")
    private volatile long activeSeq = 0L;

    @SuppressWarnings("UnusedVariable")
    private volatile @Nullable String activeStepName = null;

    @SuppressWarnings("UnusedVariable")
    private volatile long activeStartNanos = 0L;

    // Execution-thread-only references (reset under tracker monitor lock in reset()).
    private @Nullable StepProcessingStats localActiveStepStats = null;
    private @Nullable String lastLookupStepName = null;
    private @Nullable StepProcessingStats lastLookupStepStats = null;

    /** Clock used to either provide real system time or mocked to virtualize time for testing. */
    private final Clock clock;

    // Registered step stats (append-only across a worker tracker's lifetime):
    //   - Registrations synchronize in getOrCreateStepStats().
    //   - ConcurrentHashMap provides lock-free O(1) lookup by name on enterState().
    //   - stepStatsBuffer grows geometrically via Arrays.copyOf (existing indices [0, count) are
    //     never mutated or removed) and is published BEFORE incrementing volatile stepStatsCount.
    //     Readers in getProcessingTimesByStepCopy() read stepStatsCount FIRST, then
    //     stepStatsBuffer, so the volatile happens-before guarantees statsBuf.length >= count and
    //     every slot in [0, count) is non-null.
    private final ConcurrentHashMap<String, StepProcessingStats> stepStatsByName =
        new ConcurrentHashMap<>();
    private volatile StepProcessingStats[] stepStatsBuffer = EMPTY_STEP_STATS;
    private volatile int stepStatsCount = 0;

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
      // Seed from volatile activeSeq so cross-thread reset() without deactivate() strictly
      // advances past any previously published sequence and heals odd parity.
      long s = (long) ACTIVE_SEQ.getVolatile(this);
      long odd = (s & 1L) != 0L ? s + 2L : s + 1L;
      ACTIVE_SEQ.setVolatile(this, odd);
      ACTIVE_STEP_NAME.setVolatile(this, (String) null);
      ACTIVE_START_NANOS.setVolatile(this, 0L);
      ACTIVE_SEQ.setVolatile(this, odd + 1L);
      lastLookupStepName = null;
      lastLookupStepStats = null;
      int count = this.stepStatsCount;
      StepProcessingStats[] statsBuf = this.stepStatsBuffer;
      for (int i = 0; i < count; i++) {
        statsBuf[i].reset();
      }
    }

    @VisibleForTesting
    long getActiveSeqForTesting() {
      return (long) ACTIVE_SEQ.getVolatile(this);
    }

    @VisibleForTesting
    void setActiveSeqForTesting(long seq) {
      ACTIVE_SEQ.setVolatile(this, seq);
    }

    synchronized StepProcessingStats getOrCreateStepStats(String userStepName) {
      StepProcessingStats existing = stepStatsByName.get(userStepName);
      if (existing != null) {
        return existing;
      }
      StepProcessingStats created = new StepProcessingStats(userStepName);
      int idx = this.stepStatsCount;
      StepProcessingStats[] buf = this.stepStatsBuffer;
      if (idx == buf.length) {
        buf = Arrays.copyOf(buf, Math.max(4, buf.length << 1));
        buf[idx] = created;
        this.stepStatsBuffer = buf;
      } else {
        buf[idx] = created;
      }
      // Publish count after populating slot (and replacing buffer when resizing) so lock-free
      // readers observing i < stepStatsCount always see a non-null entry in stepStatsBuffer.
      this.stepStatsCount = idx + 1;
      stepStatsByName.put(userStepName, created);
      return created;
    }

    @SuppressWarnings("ReferenceEquality")
    private StepProcessingStats getStepStatsFast(String userStepName) {
      if (userStepName == lastLookupStepName && lastLookupStepStats != null) {
        return lastLookupStepStats;
      }
      StepProcessingStats stats = stepStatsByName.get(userStepName);
      if (stats == null) {
        stats = getOrCreateStepStats(userStepName);
      }
      lastLookupStepName = userStepName;
      lastLookupStepStats = stats;
      return stats;
    }

    @VisibleForTesting
    static void recordElapsedNanos(StepProcessingStats stats, long elapsedNanos) {
      if (elapsedNanos < NANOS_PER_MILLI) {
        stats.recordZeroMillis();
      } else {
        stats.record((int) Math.min(elapsedNanos / NANOS_PER_MILLI, Integer.MAX_VALUE));
      }
    }

    private void setActiveMessage(StepProcessingStats stepStats, long startNanos) {
      // Dereference stepName before opening the odd window so nothing inside the window can throw
      // and leave activeSeq stuck odd.
      String stepName = stepStats.stepName;
      localActiveStepStats = stepStats;
      long s = (long) ACTIVE_SEQ.get(this);
      // 1. Open write window (even -> odd). storeStoreFence ensures the odd marker precedes both
      //    payload stores (unlike setRelease, which only orders prior stores before itself).
      ACTIVE_SEQ.setOpaque(this, s + 1L);
      VarHandle.storeStoreFence();
      // 2. Write the new (stepName, startNanos) pair with opaque semantics (atomic, un-hoisted).
      ACTIVE_STEP_NAME.setOpaque(this, stepName);
      ACTIVE_START_NANOS.setOpaque(this, startNanos);
      // 3. Close write window (odd -> even). Release semantics publish both fields together to
      //    any reader whose ACTIVE_SEQ.getAcquire() observes s + 2.
      ACTIVE_SEQ.setRelease(this, s + 2L);
    }

    private void clearActiveMessage() {
      // Single release store clearing activeStepName without bumping activeSeq, eliminating 2
      // sequence updates/fences per element exit:
      //   - Leaving activeStartNanos untouched guarantees a concurrent reader in
      //     getActiveMessageMetadata() sees either null (returning empty, matching post-exit) or
      //     the complete prior (stepName, activeStartNanos) pair (before clear). No tearing or ABA
      //     can happen because the next setActiveMessage() or reset() bumps activeSeq odd before
      //     modifying either field.
      //   - setRelease() orders the caller's preceding recordElapsedNanos() before nulling out the
      //     active step.
      //   - The explicit (String) null cast is intentional: it gives VarHandle.setRelease an exact
      //     (DataflowExecutionStateTracker, String)V polymorphic descriptor instead of (...,
      // Void)V.
      localActiveStepStats = null;
      ACTIVE_STEP_NAME.setRelease(this, (String) null);
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
        recordElapsedNanos(prevActive, nowNanos - (long) ACTIVE_START_NANOS.get(this));
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
        recordElapsedNanos(active, System.nanoTime() - (long) ACTIVE_START_NANOS.get(this));
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
      final boolean trackElementExecution =
          elementExecutionTracker != ElementExecutionTracker.NO_OP_INSTANCE;
      final ExecutionState previous = enterStateDirect(newState);
      boolean activatedStreaming = false;
      try {
        if (isStreaming) {
          NameContext stepName = newDFState.getStepName();
          String userName = stepName != null ? stepName.userName() : null;
          if (userName != null) {
            StepProcessingStats stepStats = getStepStatsFast(userName);
            long nowNanos = System.nanoTime();
            StepProcessingStats prevActive = localActiveStepStats;
            if (prevActive != null) {
              recordElapsedNanos(prevActive, nowNanos - (long) ACTIVE_START_NANOS.get(this));
            }
            setActiveMessage(stepStats, nowNanos);
            activatedStreaming = true;
          }
        }
        if (trackElementExecution) {
          elementExecutionTracker.enter(newDFState.getStepName());
        }
      } catch (Throwable t) {
        if (activatedStreaming) {
          clearActiveMessage();
        }
        try {
          exitStateDirect(previous);
        } catch (Throwable suppressed) {
          t.addSuppressed(suppressed);
        }
        throw t;
      }

      return () -> {
        if (isStreaming) {
          StepProcessingStats active = localActiveStepStats;
          if (active != null) {
            recordElapsedNanos(active, System.nanoTime() - (long) ACTIVE_START_NANOS.get(this));
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

    /**
     * Reads a consistent snapshot of the currently active streaming step and its start time using
     * the {@code activeSeq} seqlock (allocating a {@link Stopwatch} only here on the cold reader
     * path, never on the hot execution path).
     *
     * <p>Retries are bounded to {@code MAX_ACTIVE_METADATA_ATTEMPTS} (64) so a fast-transitioning
     * stage or preempted writer cannot starve Windmill heartbeat threads; if {@code activeSeq}
     * keeps advancing across every attempt, no single element is stalled, so returning {@link
     * Optional#empty()} is safe.
     */
    public Optional<ActiveMessageMetadata> getActiveMessageMetadata() {
      for (int attempt = 0; attempt < MAX_ACTIVE_METADATA_ATTEMPTS; attempt++) {
        // 1. Read starting activeSeq with acquire semantics, synchronizing with the writer's
        //    closing ACTIVE_SEQ.setRelease(s1). Odd means setActiveMessage()/reset() is active.
        long s1 = (long) ACTIVE_SEQ.getAcquire(this);
        if ((s1 & 1L) != 0L) {
          seqlockBackoff(attempt);
          continue;
        }
        // 2. Read activeStepName. If clearActiveMessage() nulled it out without advancing
        //    activeSeq, loadLoadFence + matching s2 == s1 confirms the clear and returns empty.
        String stepName = (String) ACTIVE_STEP_NAME.getOpaque(this);
        if (stepName == null) {
          VarHandle.loadLoadFence();
          if ((long) ACTIVE_SEQ.getOpaque(this) == s1) {
            return Optional.empty();
          }
          seqlockBackoff(attempt);
          continue;
        }
        // 3. Read activeStartNanos, then fence before checking the closing activeSeq. Because
        //    clearActiveMessage() leaves activeStartNanos untouched, if s2 == s1 no subsequent
        //    setActiveMessage() or reset() intervened, so (stepName, startNanos) is an exact pair.
        long startNanos = (long) ACTIVE_START_NANOS.getOpaque(this);
        VarHandle.loadLoadFence();
        if ((long) ACTIVE_SEQ.getOpaque(this) == s1) {
          return Optional.of(
              ActiveMessageMetadata.create(stepName, createStopwatchStartedAt(startNanos)));
        }
        seqlockBackoff(attempt);
      }
      return Optional.empty();
    }

    /**
     * Returns a point-in-time snapshot of processing times for every step that has completed at
     * least one element since the last {@link #reset()}.
     */
    public Map<String, IntSummaryStatistics> getProcessingTimesByStepCopy() {
      // Load volatile stepStatsCount BEFORE volatile stepStatsBuffer to pair with
      // getOrCreateStepStats(), which writes the slot (and publishes any resized buffer) BEFORE
      // incrementing stepStatsCount. This guarantees statsBuf.length >= count and non-null slots.
      int count = this.stepStatsCount;
      if (count == 0) {
        return new HashMap<>();
      }
      StepProcessingStats[] statsBuf = this.stepStatsBuffer;
      Map<String, IntSummaryStatistics> processingTimesCopy = new HashMap<>(Math.max(4, count * 2));
      for (int i = 0; i < count; i++) {
        StepProcessingStats stats = statsBuf[i];
        IntSummaryStatistics snapshot = stats.snapshot();
        if (snapshot != null) {
          processingTimesCopy.put(stats.stepName, snapshot);
        }
      }
      return processingTimesCopy;
    }
  }
}
