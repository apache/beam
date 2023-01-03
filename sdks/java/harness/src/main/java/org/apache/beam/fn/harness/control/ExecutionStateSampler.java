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
package org.apache.beam.fn.harness.control;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.runners.core.metrics.MonitoringInfoEncodings;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Monitors the execution of one or more execution threads. */
public class ExecutionStateSampler {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionStateSampler.class);
  private static final int DEFAULT_SAMPLING_PERIOD_MS = 200;
  private static final long MAX_LULL_TIME_MS = TimeUnit.MINUTES.toMillis(5);
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
  private final int periodMs;
  private final MillisProvider clock;

  @GuardedBy("activeStateTrackers")
  private final Set<ExecutionStateTracker> activeStateTrackers;

  private final Future<Void> stateSamplingThread;

  @SuppressWarnings("methodref.receiver.bound" /* Synchronization ensures proper initialization */)
  public ExecutionStateSampler(PipelineOptions options, MillisProvider clock) {
    String samplingPeriodMills =
        ExperimentalOptions.getExperimentValue(
            options, ExperimentalOptions.STATE_SAMPLING_PERIOD_MILLIS);
    this.periodMs =
        samplingPeriodMills == null
            ? DEFAULT_SAMPLING_PERIOD_MS
            : Integer.parseInt(samplingPeriodMills);
    this.clock = clock;
    this.activeStateTrackers = new HashSet<>();
    // We specifically synchronize to ensure that this object can complete
    // being published before the state sampler thread starts.
    synchronized (this) {
      this.stateSamplingThread =
          options
              .as(ExecutorOptions.class)
              .getScheduledExecutorService()
              .submit(this::stateSampler);
    }
  }

  /** An {@link ExecutionState} represents the current state of an execution thread. */
  public interface ExecutionState {

    /**
     * Activates this execution state within the {@link ExecutionStateTracker}.
     *
     * <p>Must only be invoked by the bundle processing thread.
     */
    void activate();

    /**
     * Restores the previously active execution state within the {@link ExecutionStateTracker}.
     *
     * <p>Must only be invoked by the bundle processing thread.
     */
    void deactivate();
  }

  /** Stops the execution of the state sampler. */
  public void stop() {
    stateSamplingThread.cancel(true);
    try {
      stateSamplingThread.get(5L * periodMs, TimeUnit.MILLISECONDS);
    } catch (CancellationException e) {
      // This was expected -- we were cancelling the thread.
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(
          "Failed to stop state sampling after waiting 5 sampling periods.", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception in state sampler", e);
    }
  }

  /** Entry point for the state sampling thread. */
  private Void stateSampler() throws Exception {
    // Ensure the object finishes being published safely.
    synchronized (this) {
      if (stateSamplingThread == null) {
        throw new IllegalStateException("Underinitialized ExecutionStateSampler instance");
      }
    }

    long lastSampleTimeMillis = clock.getMillis();
    long targetTimeMillis = lastSampleTimeMillis + periodMs;
    while (!Thread.interrupted()) {
      long currentTimeMillis = clock.getMillis();
      long difference = targetTimeMillis - currentTimeMillis;
      if (difference > 0) {
        Thread.sleep(difference);
      } else {
        long millisSinceLastSample = currentTimeMillis - lastSampleTimeMillis;
        synchronized (activeStateTrackers) {
          for (ExecutionStateTracker activeTracker : activeStateTrackers) {
            activeTracker.takeSample(currentTimeMillis, millisSinceLastSample);
          }
        }
        lastSampleTimeMillis = currentTimeMillis;
        targetTimeMillis = lastSampleTimeMillis + periodMs;
      }
    }
    return null;
  }

  /** Returns a new {@link ExecutionStateTracker} associated with this state sampler. */
  public ExecutionStateTracker create() {
    return new ExecutionStateTracker();
  }

  /** Tracks the current state of a single execution thread. */
  public class ExecutionStateTracker implements BundleProgressReporter {

    // The set of execution states that this tracker is responsible for. Effectively
    // final since create() should not be invoked once any bundle starts processing.
    private final List<ExecutionStateImpl> executionStates;
    // Read by multiple threads, written by the bundle processing thread lazily.
    private final AtomicReference<@Nullable String> processBundleId;
    // Read by multiple threads, written by the bundle processing thread lazily.
    private final AtomicReference<@Nullable Thread> trackedThread;
    // Read by multiple threads, read and written by the ExecutionStateSampler thread lazily.
    private final AtomicLong lastTransitionTime;
    // Read and written by the bundle processing thread frequently.
    private long numTransitions;
    // Read by the ExecutionStateSampler, written by the bundle processing thread lazily and
    // frequently.
    private final AtomicLong numTransitionsLazy;
    // Read and written by the bundle processing thread frequently.
    private @Nullable ExecutionStateImpl currentState;
    // Read by multiple threads, written by the bundle processing thread lazily.
    private final AtomicReference<@Nullable ExecutionStateImpl> currentStateLazy;
    // Read and written by the ExecutionStateSampler thread
    private long transitionsAtLastSample;

    private ExecutionStateTracker() {
      this.executionStates = new ArrayList<>();
      this.trackedThread = new AtomicReference<>();
      this.lastTransitionTime = new AtomicLong();
      this.numTransitionsLazy = new AtomicLong();
      this.currentStateLazy = new AtomicReference<>();
      this.processBundleId = new AtomicReference<>();
    }

    /**
     * Returns an {@link ExecutionState} bound to this tracker for the specified transform and
     * processing state.
     */
    public ExecutionState create(
        String shortId, String ptransformId, String ptransformUniqueName, String stateName) {
      ExecutionStateImpl newState =
          new ExecutionStateImpl(shortId, ptransformId, ptransformUniqueName, stateName);
      executionStates.add(newState);
      return newState;
    }

    /**
     * Called periodically by the {@link ExecutionStateSampler} to report time spent in this state.
     *
     * @param currentTimeMillis the current time.
     * @param millisSinceLastSample the time since the last sample was reported. As an
     *     approximation, all of that time should be associated with this state.
     */
    private void takeSample(long currentTimeMillis, long millisSinceLastSample) {
      ExecutionStateImpl currentExecutionState = currentStateLazy.get();
      if (currentExecutionState != null) {
        currentExecutionState.takeSample(millisSinceLastSample);
      }

      long transitionsAtThisSample = numTransitionsLazy.get();

      if (transitionsAtThisSample != transitionsAtLastSample) {
        lastTransitionTime.lazySet(currentTimeMillis);
        transitionsAtLastSample = transitionsAtThisSample;
      } else {
        long lullTimeMs = currentTimeMillis - lastTransitionTime.get();
        Thread thread = trackedThread.get();
        if (lullTimeMs > MAX_LULL_TIME_MS) {
          if (thread == null) {
            LOG.warn(
                String.format(
                    "Operation ongoing in bundle %s for at least %s without outputting or completing (stack trace unable to be generated).",
                    processBundleId.get(),
                    DURATION_FORMATTER.print(Duration.millis(lullTimeMs).toPeriod())));
          } else if (currentExecutionState == null) {
            LOG.warn(
                String.format(
                    "Operation ongoing in bundle %s for at least %s without outputting or completing:%n  at %s",
                    processBundleId.get(),
                    DURATION_FORMATTER.print(Duration.millis(lullTimeMs).toPeriod()),
                    Joiner.on("\n  at ").join(thread.getStackTrace())));
          } else {
            LOG.warn(
                String.format(
                    "Operation ongoing in bundle %s for PTransform{id=%s, name=%s, state=%s} for at least %s without outputting or completing:%n  at %s",
                    processBundleId.get(),
                    currentExecutionState.ptransformId,
                    currentExecutionState.ptransformUniqueName,
                    currentExecutionState.stateName,
                    DURATION_FORMATTER.print(Duration.millis(lullTimeMs).toPeriod()),
                    Joiner.on("\n  at ").join(thread.getStackTrace())));
          }
        }
      }
    }

    /** Returns status information related to this tracker or null if not tracking a bundle. */
    public @Nullable ExecutionStateTrackerStatus getStatus() {
      Thread thread = trackedThread.get();
      if (thread == null) {
        return null;
      }
      long lastTransitionTimeMs = lastTransitionTime.get();
      // We are actively processing a bundle but may have not yet entered into a state.
      ExecutionStateImpl current = currentStateLazy.get();
      if (current != null) {
        return ExecutionStateTrackerStatus.create(
            current.ptransformId, current.ptransformUniqueName, thread, lastTransitionTimeMs);
      } else {
        return ExecutionStateTrackerStatus.create(null, null, thread, lastTransitionTimeMs);
      }
    }

    /** Returns the ptransform id of the currently executing thread. */
    public @Nullable String getCurrentThreadsPTransformId() {
      if (currentState == null) {
        return null;
      }
      return currentState.ptransformId;
    }

    /** {@link ExecutionState} represents the current state of an execution thread. */
    private class ExecutionStateImpl implements ExecutionState {
      private final String shortId;
      private final String ptransformId;
      private final String ptransformUniqueName;
      private final String stateName;
      // Read and written by the bundle processing thread frequently.
      private long msecs;
      // Read by the ExecutionStateSampler, written by the bundle processing thread frequently.
      private final AtomicLong lazyMsecs;
      /** Guarded by {@link BundleProcessor#getProgressRequestLock}. */
      private boolean hasReportedValue;
      /** Guarded by {@link BundleProcessor#getProgressRequestLock}. */
      private long lastReportedValue;
      // Read and written by the bundle processing thread frequently.
      private @Nullable ExecutionStateImpl previousState;

      private ExecutionStateImpl(
          String shortId, String ptransformId, String ptransformName, String stateName) {
        this.shortId = shortId;
        this.ptransformId = ptransformId;
        this.ptransformUniqueName = ptransformName;
        this.stateName = stateName;
        this.lazyMsecs = new AtomicLong();
      }

      /**
       * Called periodically by the {@link ExecutionStateTracker} to report time spent in this
       * state.
       *
       * @param millisSinceLastSample the time since the last sample was reported. As an
       *     approximation, all of that time should be associated with this state.
       */
      public void takeSample(long millisSinceLastSample) {
        msecs += millisSinceLastSample;
        lazyMsecs.set(msecs);
      }

      /** Updates the monitoring data for this {@link ExecutionState}. */
      public void updateMonitoringData(Map<String, ByteString> monitoringData) {
        long msecsReads = lazyMsecs.get();
        if (hasReportedValue && lastReportedValue == msecsReads) {
          return;
        }
        monitoringData.put(shortId, MonitoringInfoEncodings.encodeInt64Counter(msecsReads));
        lastReportedValue = msecsReads;
        hasReportedValue = true;
      }

      public void reset() {
        if (hasReportedValue) {
          msecs = 0;
          lazyMsecs.set(0);
          lastReportedValue = 0;
        }
      }

      @Override
      public void activate() {
        previousState = currentState;
        currentState = this;
        currentStateLazy.lazySet(this);
        numTransitions += 1;
        numTransitionsLazy.lazySet(numTransitions);
      }

      @Override
      public void deactivate() {
        currentState = previousState;
        currentStateLazy.lazySet(previousState);
        previousState = null;

        numTransitions += 1;
        numTransitionsLazy.lazySet(numTransitions);
      }
    }

    /**
     * Starts tracking execution states for specified {@code processBundleId}.
     *
     * <p>Only invoked by the bundle processing thread.
     */
    public void start(String processBundleId) {
      this.processBundleId.lazySet(processBundleId);
      this.lastTransitionTime.lazySet(clock.getMillis());
      this.trackedThread.lazySet(Thread.currentThread());
      synchronized (activeStateTrackers) {
        activeStateTrackers.add(this);
      }
    }

    @Override
    public void updateIntermediateMonitoringData(Map<String, ByteString> monitoringData) {
      for (ExecutionStateImpl executionState : executionStates) {
        executionState.updateMonitoringData(monitoringData);
      }
    }

    @Override
    public void updateFinalMonitoringData(Map<String, ByteString> monitoringData) {
      for (ExecutionStateImpl executionState : executionStates) {
        executionState.updateMonitoringData(monitoringData);
      }
    }

    /**
     * Stops tracking execution states allowing for the {@link ExecutionStateTracker} to be re-used
     * for another bundle.
     */
    @Override
    public void reset() {
      synchronized (activeStateTrackers) {
        activeStateTrackers.remove(this);
        for (ExecutionStateImpl executionState : executionStates) {
          executionState.reset();
        }
        this.transitionsAtLastSample = 0;
      }
      this.processBundleId.lazySet(null);
      this.trackedThread.lazySet(null);
      this.numTransitions = 0;
      this.numTransitionsLazy.lazySet(0);
      this.lastTransitionTime.lazySet(0);
    }
  }

  @AutoValue
  public abstract static class ExecutionStateTrackerStatus {
    public static ExecutionStateTrackerStatus create(
        @Nullable String ptransformId,
        @Nullable String ptransformUniqueName,
        Thread trackedThread,
        long lastTransitionTimeMs) {
      return new AutoValue_ExecutionStateSampler_ExecutionStateTrackerStatus(
          ptransformId, ptransformUniqueName, trackedThread, lastTransitionTimeMs);
    }

    public abstract @Nullable String getPTransformId();

    public abstract @Nullable String getPTransformUniqueName();

    public abstract Thread getTrackedThread();

    public abstract long getLastTransitionTimeMillis();
  }
}
