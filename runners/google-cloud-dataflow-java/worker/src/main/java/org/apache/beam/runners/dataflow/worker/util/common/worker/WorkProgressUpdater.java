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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import com.google.api.client.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkProgressUpdater allows a work executor to send work progress updates to the worker service.
 * The life-cycle of the WorkProgressUpdater is controlled externally through its {@link
 * #startReportingProgress()} and {@link #stopReportingProgress()} methods. The updater queries the
 * worker for progress updates and sends the updates to the worker service. The interval between two
 * consecutive updates is controlled by the worker service through reporting interval hints sent
 * back in the update response messages. To avoid update storms and monitoring staleness, the
 * interval between two consecutive updates is also bound by {@link #getMinReportingInterval} and
 * {@link #getMaxReportingInterval}.
 */
// Very likely real potential for bugs - https://github.com/apache/beam/issues/19274
@SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
@NotThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class WorkProgressUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(WorkProgressUpdater.class);

  /** The default lease duration to request from the external worker service (3 minutes). */
  public static final long DEFAULT_LEASE_DURATION_MILLIS = 3 * 60 * 1000;

  /** The lease renewal RPC latency margin (5 seconds). */
  private static final long DEFAULT_LEASE_RENEWAL_LATENCY_MARGIN = 5000;

  /**
   * The minimum period between two consecutive progress updates. Ensures the {@link
   * WorkProgressUpdater} does not generate update storms (5 seconds).
   */
  private static final long DEFAULT_MIN_REPORTING_INTERVAL_MILLIS = 5000;

  /**
   * The maximum period between two consecutive progress updates. Ensures the {@link
   * WorkProgressUpdater} does not cause monitoring staleness (10 minutes).
   */
  private static final long DEFAULT_MAX_REPORTING_INTERVAL_MILLIS = 10 * 60 * 1000;

  /**
   * Worker providing the work progress updates. This is a volatile variable because the worker
   * thread sets it while the progress updater thread reads it.
   */
  protected volatile WorkExecutor worker = null;

  /** Requested periodic checkpoint period. */
  private final int checkpointPeriodSec;

  /**
   * The time when the next periodic checkpoint should occur. In the same units as {@code
   * Clock.currentTimeMillis()}.
   */
  private long nextPeriodicCheckpointTimeMs;

  /** Executor used to schedule work progress updates. */
  private final ScheduledExecutorService executor;

  /** Clock used to either provide real system time or mocked to virtualize time for testing. */
  protected final Clock clock;

  /** The lease duration to request from the external worker service. */
  protected long requestedLeaseDurationMs;

  /** The time period until the next work progress update. */
  protected long progressReportIntervalMs;

  /** The state of worker checkpointing. */
  protected enum CheckpointState {
    /** No checkpoint has yet been requested. */
    CHECKPOINT_NOT_REQUESTED,
    /** A checkpoint has been requested but not yet done successfully. */
    CHECKPOINT_REQUESTED,
    /** A successful checkpoint has been done. */
    CHECKPOINT_SUCCESSFUL
  }

  @GuardedBy("executor")
  protected CheckpointState checkpointState = CheckpointState.CHECKPOINT_NOT_REQUESTED;

  /**
   * The {@link NativeReader.DynamicSplitResult} to report to the service in the next progress
   * update, or {@code null} if there is nothing to report (if no dynamic split happened since the
   * last progress update).
   */
  protected NativeReader.DynamicSplitResult dynamicSplitResultToReport;

  /**
   * @param checkpointPeriodSec the desired amount of time in seconds between periodic checkpoints;
   *     if no periodic checkpoints are desired then pass {@link Integer#MAX_VALUE}
   */
  public WorkProgressUpdater(WorkExecutor worker, int checkpointPeriodSec) {
    this(
        worker,
        checkpointPeriodSec,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("WorkProgressUpdater-%d")
                .build()),
        Clock.SYSTEM);
  }

  /**
   * @param checkpointPeriodSec the desired amount of time in seconds between periodic checkpoints;
   *     if no periodic checkpoints are desired then pass {@link Integer#MAX_VALUE}
   * @param executor the desired executor, can be used to inject a executor for testing
   * @param clock the desired clock, can be used to inject a mock clock for testing
   */
  @VisibleForTesting
  protected WorkProgressUpdater(
      WorkExecutor worker,
      int checkpointPeriodSec,
      ScheduledExecutorService executor,
      Clock clock) {
    this.worker = worker;
    this.checkpointPeriodSec = checkpointPeriodSec;
    this.executor = executor;
    this.clock = clock;
  }

  /** @param worker workexecutor for the updater. */
  public void setWorker(WorkExecutor worker) {
    this.worker = worker;
  }

  /** Starts sending work progress updates to the worker service. */
  public void startReportingProgress() {
    // The initial work progress report is sent according to hints from the service if any.
    // Otherwise the default is half-way through the lease.
    long leaseRemainingTime = leaseRemainingTime(getWorkUnitLeaseExpirationTimestamp());
    progressReportIntervalMs =
        nextProgressReportInterval(getWorkUnitSuggestedReportingInterval(), leaseRemainingTime);
    requestedLeaseDurationMs = DEFAULT_LEASE_DURATION_MILLIS;

    nextPeriodicCheckpointTimeMs = clock.currentTimeMillis() + ((long) checkpointPeriodSec) * 1000;

    LOG.debug("Started reporting progress for work item: {}", workString());
    scheduleNextUpdate();
  }

  /** Requests that a checkpoint be done. */
  public void requestCheckpoint() {
    synchronized (executor) {
      LOG.debug("Asynchronous checkpoint for work item {}.", workString());
      if (checkpointState == CheckpointState.CHECKPOINT_NOT_REQUESTED) {
        checkpointState = CheckpointState.CHECKPOINT_REQUESTED;
      }
      if (tryCheckpointIfNeeded()) {
        reportProgress();
      }
    }
  }

  /**
   * Stops sending work progress updates to the worker service. It may throw an exception if the
   * final progress report fails to be sent for some reason.
   */
  public void stopReportingProgress() throws Exception {
    // Wait until there are no more progress updates in progress, then shut down.
    synchronized (executor) {
      executor.shutdownNow();

      // We send a final progress report in case there was an unreported dynamic split.
      if (dynamicSplitResultToReport != null) {
        LOG.debug(
            "Sending final progress update with unreported split: {} " + "for work item: {}",
            dynamicSplitResultToReport,
            workString());
        reportProgressHelper(); // This call can fail with an exception
      }
    }

    LOG.debug("Stopped reporting progress for work item: {}", workString());
  }

  /**
   * Computes the time before sending the next work progress update making sure that it falls
   * between the [{@link #getMinReportingInterval}, {@link #getMaxReportingInterval}] interval.
   * Makes an attempt to bound the result by the remaining lease time, with an RPC latency margin of
   * {@link #getLeaseRenewalLatencyMargin}.
   *
   * @param suggestedInterval the suggested progress report interval
   * @param leaseRemainingTime milliseconds left before the work lease expires
   * @return the time in milliseconds before sending the next progress update
   */
  protected final long nextProgressReportInterval(long suggestedInterval, long leaseRemainingTime) {
    // Try to send the next progress update before the next lease expiration
    // allowing some RPC latency margin.
    suggestedInterval =
        Math.min(suggestedInterval, leaseRemainingTime - getLeaseRenewalLatencyMargin());

    // Bound reporting interval to avoid staleness and progress update storms.
    return Math.min(
        Math.max(getMinReportingInterval(), suggestedInterval), getMaxReportingInterval());
  }

  /** Schedules the next work progress update or periodic checkpoint. */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleNextUpdate() {
    if (executor.isShutdown()) {
      return;
    }
    long delay =
        Math.min(
            progressReportIntervalMs, nextPeriodicCheckpointTimeMs - clock.currentTimeMillis());
    executor.schedule(
        new Runnable() {
          @Override
          public void run() {
            doNextUpdate();
          }
        },
        delay,
        TimeUnit.MILLISECONDS);
    LOG.debug(
        "Next work progress update for work item {} scheduled to occur in {} ms.",
        workString(),
        progressReportIntervalMs);
  }

  /** Does the next work progress update or periodic checkpoint. */
  private void doNextUpdate() {
    // Don't shut down while reporting progress.
    synchronized (executor) {
      if (executor.isShutdown()) {
        return;
      }
      try {
        checkForPeriodicCheckpoint();
        tryCheckpointIfNeeded();
        reportProgress();
      } finally {
        scheduleNextUpdate();
      }
    }
  }

  /** If it is time for a periodic checkpoint then requests it. */
  @GuardedBy("executor")
  private void checkForPeriodicCheckpoint() {
    if (clock.currentTimeMillis() >= nextPeriodicCheckpointTimeMs) {
      LOG.debug("Periodic checkpoint for work item {}.", workString());
      if (checkpointState == CheckpointState.CHECKPOINT_NOT_REQUESTED) {
        checkpointState = CheckpointState.CHECKPOINT_REQUESTED;
      }
      nextPeriodicCheckpointTimeMs = Long.MAX_VALUE;
    }
  }

  /**
   * If a checkpoint has been requested but not yet done, tries to do it. Returns whether a
   * successful checkpoint was done.
   */
  @GuardedBy("executor")
  protected boolean tryCheckpointIfNeeded() {
    if (checkpointState == CheckpointState.CHECKPOINT_REQUESTED && worker != null) {
      LOG.debug("Trying to checkpoint for work item {}.", workString());
      try {
        NativeReader.DynamicSplitResult checkpointPos = worker.requestCheckpoint();
        if (checkpointPos != null) {
          LOG.debug("Successful checkpoint for work item {} at {}.", workString(), checkpointPos);
          dynamicSplitResultToReport = checkpointPos;
          checkpointState = CheckpointState.CHECKPOINT_SUCCESSFUL;
          return true;
        }
      } catch (Throwable e) {
        LOG.warn("Error trying to checkpoint the worker: ", e);
      }
    }
    return false;
  }

  /** Reports the current work progress to the worker service. */
  @GuardedBy("executor")
  private void reportProgress() {
    LOG.debug("Updating progress on work item {}", workString());
    try {
      reportProgressHelper();
    } catch (InterruptedException e) {
      LOG.info("Cancelling workitem execution: {}", workString(), e);
      worker.abort();
    } catch (Throwable e) {
      LOG.warn("Error reporting workitem progress update to Dataflow service: ", e);
    }
  }

  /**
   * Computes the amount of time left, in milliseconds, before a lease with the specified expiration
   * timestamp expires. Returns zero if the lease has already expired.
   */
  protected long leaseRemainingTime(long leaseExpirationTimestamp) {
    long now = clock.currentTimeMillis();
    if (leaseExpirationTimestamp < now) {
      LOG.debug("Lease remaining time for {} is 0 ms.", workString());
      return 0;
    }
    LOG.debug(
        "Lease remaining time for {} is {} ms.", workString(), leaseExpirationTimestamp - now);
    return leaseExpirationTimestamp - now;
  }

  @VisibleForTesting
  public NativeReader.DynamicSplitResult getDynamicSplitResultToReport() {
    return dynamicSplitResultToReport;
  }

  /**
   * Reports the current work progress to the worker service. Holds lock on executor during call so
   * that checkpointState can be accessed.
   *
   * @throws an InterruptedException to indicate that the WorkItem has been aborted.
   */
  @GuardedBy("executor")
  protected abstract void reportProgressHelper() throws Exception;

  /** Returns the current work item's lease expiration timestamp. */
  protected abstract long getWorkUnitLeaseExpirationTimestamp();

  /** Returns the current work item's suggested progress reporting interval. */
  protected long getWorkUnitSuggestedReportingInterval() {
    return leaseRemainingTime(getWorkUnitLeaseExpirationTimestamp()) / 2;
  }

  /** Returns the minimum allowed time between two periodic progress updates. */
  protected long getMinReportingInterval() {
    return DEFAULT_MIN_REPORTING_INTERVAL_MILLIS;
  }

  /** Returns the maximum allowed time between two periodic progress updates. */
  protected long getMaxReportingInterval() {
    return DEFAULT_MAX_REPORTING_INTERVAL_MILLIS;
  }

  /**
   * Returns the maximum allowed time between a periodic progress update and the moment the current
   * lease expires.
   */
  protected long getLeaseRenewalLatencyMargin() {
    return DEFAULT_LEASE_RENEWAL_LATENCY_MARGIN;
  }

  /**
   * Returns a string representation of the work item whose progress is being updated, for use in
   * logging messages.
   */
  protected abstract String workString();
}
