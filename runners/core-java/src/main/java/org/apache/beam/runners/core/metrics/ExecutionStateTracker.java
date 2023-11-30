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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Tracks the current state of a single execution thread. */
@SuppressFBWarnings(value = "IS2_INCONSISTENT_SYNC", justification = "Intentional for performance.")
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ExecutionStateTracker implements Comparable<ExecutionStateTracker> {

  /**
   * This allows determining which {@link ExecutionStateTracker} is managing a specific thread. We
   * don't use a ThreadLocal to allow testing the implementation of this class without having to run
   * from multiple threads.
   */
  private static final Map<Long, ExecutionStateTracker> CURRENT_TRACKERS =
      new ConcurrentHashMap<>();

  private static final long LULL_REPORT_MS = TimeUnit.MINUTES.toMillis(5);
  private static final AtomicIntegerFieldUpdater<ExecutionStateTracker> SAMPLING_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(ExecutionStateTracker.class, "sampling");

  public static final String START_STATE_NAME = "start";
  public static final String PROCESS_STATE_NAME = "process";
  public static final String PROCESS_TIMERS_STATE_NAME = "process-timers";
  public static final String FINISH_STATE_NAME = "finish";
  public static final String ABORT_STATE_NAME = "abort";

  /** An {@link ExecutionState} represents the current state of an execution thread. */
  public abstract static class ExecutionState {
    private final String stateName;

    /** Whether the current state represents the element processing state. */
    public final boolean isProcessElementState;

    public ExecutionState(String stateName) {
      this.stateName = stateName;
      this.isProcessElementState = Objects.equals(stateName, PROCESS_STATE_NAME);
    }

    /**
     * Called periodically by the {@link ExecutionStateSampler} to report time spent in this state.
     *
     * @param millisSinceLastSample the time since the last sample was reported. As an
     *     approximation, all of that time should be associated with this state.
     */
    public abstract void takeSample(long millisSinceLastSample);

    /** Returns the name of this state within the executing step. */
    public String getStateName() {
      return stateName;
    }

    /**
     * Called when this state becomes the active state (top of the stack). This method will be
     * called once when initially activated, and again any time a state it transitioned to is
     * deactivated, causing execution to return to this state. This may occur many times along the
     * hottest path, so it should it should be as efficient as possible.
     */
    public void onActivate(boolean pushing) {}

    /**
     * Called when a lull has been detected in the given state. This indicates that more than {@link
     * #LULL_REPORT_MS} has been spent in the same state without any transitions.
     *
     * @param trackedThread The execution thread that is in a lull.
     * @param millis The milliseconds since the state was most recently entered.
     */
    public abstract void reportLull(Thread trackedThread, long millis);

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass()).add("stateName", stateName).toString();
    }

    public String getDescription() {
      return stateName;
    }
  }

  private final ExecutionStateSampler sampler;

  /** The thread being managed by this {@link ExecutionStateTracker}. */
  private @Nullable Thread trackedThread = null;

  /**
   * The current state of the thread managed by this {@link ExecutionStateTracker}.
   *
   * <p>This variable is written by the Execution thread, and read by the sampling and progress
   * reporting threads, thus it being marked volatile.
   */
  private volatile @Nullable ExecutionState currentState;

  @SuppressWarnings("UnusedVariable")
  private volatile int sampling = 0;

  /**
   * The current number of times that this {@link ExecutionStateTracker} has transitioned state.
   *
   * <p>This variable is written by the Execution thread, and read by the sampling and progress
   * reporting threads, thus it being marked volatile.
   */
  private volatile long numTransitions = 0;

  /**
   * The number of milliseconds since the {@link ExecutionStateTracker} transitioned state.
   *
   * <p>This variable is updated by the Sampling thread, and read by the Progress Reporting thread,
   * thus it being marked volatile.
   */
  private volatile long millisSinceLastTransition = 0;

  private long transitionsAtLastSample = 0;
  private long nextLullReportMs = LULL_REPORT_MS;

  public ExecutionStateTracker(ExecutionStateSampler sampler) {
    this.sampler = sampler;
  }

  /** Reset the execution status. */
  public synchronized void reset() {
    if (trackedThread != null) {
      CURRENT_TRACKERS.remove(trackedThread.getId());
      trackedThread = null;
    }
    currentState = null;
    numTransitions = 0;
    millisSinceLastTransition = 0;
    transitionsAtLastSample = 0;
    nextLullReportMs = LULL_REPORT_MS;
  }

  @VisibleForTesting
  public static ExecutionStateTracker newForTest() {
    return new ExecutionStateTracker(ExecutionStateSampler.newForTest());
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  // Findbugs warns about use of pointer equality in the `then` clause
  // below, because it is critical that the `else` clause below can never
  // return 0. Our use of identityHashCode is a special case where this
  // holds.
  @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
  @Override
  public int compareTo(ExecutionStateTracker o) {
    if (this.equals(o)) {
      return 0;
    } else {
      return System.identityHashCode(this) - System.identityHashCode(o);
    }
  }

  /**
   * Return the current {@link ExecutionState} of the current thread, or {@code null} if there
   * either is no current state or if the current thread is not currently tracking the state.
   */
  public static @Nullable ExecutionState getCurrentExecutionState() {
    ExecutionStateTracker tracker = CURRENT_TRACKERS.get(Thread.currentThread().getId());
    return tracker == null ? null : tracker.currentState;
  }

  /**
   * Return the current {@link ExecutionState} of the thread with thread id, or {@code null} if
   * there either is no current state or if the corresponding thread is not currently tracking the
   * state.
   */
  public static @Nullable ExecutionState getCurrentExecutionState(long threadId) {
    ExecutionStateTracker tracker = CURRENT_TRACKERS.get(threadId);
    return tracker == null ? null : tracker.currentState;
  }

  /**
   * Activates state sampling using this {@link ExecutionStateTracker} to track the current thread.
   *
   * @return A {@link Closeable} to deactivate state sampling.
   */
  public Closeable activate() {
    return activate(Thread.currentThread());
  }

  /**
   * Activates state sampling using this {@link ExecutionStateTracker} to track the given thread.
   *
   * @return A {@link Closeable} to deactivate state sampling.
   */
  @VisibleForTesting
  public synchronized Closeable activate(Thread thread) {
    checkState(
        trackedThread == null, "Cannot activate an ExecutionStateTracker that is already in use.");

    ExecutionStateTracker other = CURRENT_TRACKERS.put(thread.getId(), this);
    checkState(
        other == null,
        "Execution state of thread %s was already being tracked by %s",
        thread.getId(),
        other);

    this.trackedThread = thread;
    sampler.addTracker(this);

    return this::deactivate;
  }

  /** Return the execution thread that is being tracked by this {@link ExecutionStateTracker}. */
  public Thread getTrackedThread() {
    return trackedThread;
  }

  private synchronized void deactivate() {
    sampler.removeTracker(this);
    Thread thread = this.trackedThread;
    if (thread != null) {
      CURRENT_TRACKERS.remove(thread.getId());
    }
    this.trackedThread = null;
  }

  public ExecutionState getCurrentState() {
    return currentState;
  }

  /**
   * Indicates that the execution thread has entered the {@code newState}. Returns a {@link
   * Closeable} that should be called when that state is completed.
   *
   * <p>This must be the only place where incTransitions is called, and always called from the
   * execution thread.
   */
  public Closeable enterState(ExecutionState newState) {
    // WARNING: This method is called in the hottest path, and must be kept as efficient as
    // possible. Avoid blocking, synchronizing, etc.
    final ExecutionState previous = currentState;
    currentState = newState;
    newState.onActivate(true);
    incTransitions();
    return () -> {
      currentState = previous;
      incTransitions();
      if (previous != null) {
        previous.onActivate(false);
      }
    };
  }

  @SuppressWarnings("NonAtomicVolatileUpdate")
  // Helper method necessary due to https://github.com/spotbugs/spotbugs/issues/724
  @SuppressFBWarnings(
      value = "VO_VOLATILE_INCREMENT",
      justification = "Intentional for performance.")
  private void incTransitions() {
    numTransitions++;
  }

  /** Return the number of transitions that have been observed by this state tracker. */
  public long getNumTransitions() {
    return numTransitions;
  }

  /** Return the time since the last transition. */
  public long getMillisSinceLastTransition() {
    return millisSinceLastTransition;
  }

  /** Return the number of transitions since the last sample. */
  public long getTransitionsAtLastSample() {
    return transitionsAtLastSample;
  }

  /** Return the time of the next lull report. */
  public long getNextLullReportMs() {
    return nextLullReportMs;
  }

  public void takeSample(long millisSinceLastSample) {
    if (SAMPLING_UPDATER.compareAndSet(this, 0, 1)) {
      try {
        takeSampleOnce(millisSinceLastSample);
      } finally {
        SAMPLING_UPDATER.set(this, 0);
      }
    }
  }

  protected void takeSampleOnce(long millisSinceLastSample) {
    // These variables are read by Sampler thread, and written by Execution and Progress Reporting
    // threads.
    // Because there is no read/modify/write cycle in the Sampler thread, making them volatile
    // provides enough thread security - but one should be very careful when working in this code.
    ExecutionState state = currentState;
    long transitionsAtThisSample = numTransitions;

    if (transitionsAtThisSample != transitionsAtLastSample) {
      millisSinceLastTransition = 0;
      nextLullReportMs = LULL_REPORT_MS;
      transitionsAtLastSample = transitionsAtThisSample;
    }
    updateMillisSinceLastTransition(millisSinceLastSample, state);
  }

  @SuppressWarnings("NonAtomicVolatileUpdate")
  private void updateMillisSinceLastTransition(long millisSinceLastSample, ExecutionState state) {
    // This variable is written by the Sampler thread, and read by the Progress Reporting thread.
    // Because only one thread modifies it, volatile provides enough synchronization.
    millisSinceLastTransition += millisSinceLastSample;
    if (state != null) {
      if (millisSinceLastTransition > nextLullReportMs) {
        state.reportLull(trackedThread, millisSinceLastTransition);
        nextLullReportMs += LULL_REPORT_MS;
      }

      state.takeSample(millisSinceLastSample);
    }
  }
}
