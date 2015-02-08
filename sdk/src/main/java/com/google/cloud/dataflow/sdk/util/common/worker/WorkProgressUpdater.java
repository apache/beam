/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * WorkProgressUpdater allows a work executor to send work progress
 * updates to the worker service. The life-cycle of the
 * WorkProgressUpdater is controlled externally through its
 * {@link #startReportingProgress()} and
 * {@link #stopReportingProgress()} methods. The updater queries the
 * worker for progress updates and sends the updates to the worker
 * service.  The interval between two consecutive updates is
 * controlled by the worker service through reporting interval hints
 * sent back in the update response messages.  To avoid update storms
 * and monitoring staleness, the interval between two consecutive
 * updates is also bound by {@link #MIN_REPORTING_INTERVAL_MILLIS} and
 * {@link #MAX_REPORTING_INTERVAL_MILLIS}.
 */
@NotThreadSafe
public abstract class WorkProgressUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(WorkProgressUpdater.class);

  /** The default lease duration to request from the external worker service. */
  private static final long DEFAULT_LEASE_DURATION_MILLIS = 3 * 60 * 1000;

  /** The lease renewal RPC latency margin. */
  private static final long LEASE_RENEWAL_LATENCY_MARGIN =
      Long.valueOf(System.getProperty("worker_lease_renewal_latency_margin", "5000"));

  /**
   * The minimum period between two consecutive progress updates. Ensures the
   * {@link WorkProgressUpdater} does not generate update storms.
   */
  private static final long MIN_REPORTING_INTERVAL_MILLIS =
      Long.valueOf(System.getProperty("minimum_worker_update_interval_millis", "5000"));

  /**
   * The maximum period between two consecutive progress updates. Ensures the
   * {@link WorkProgressUpdater} does not cause monitoring staleness.
   */
  private static final long MAX_REPORTING_INTERVAL_MILLIS = 10 * 60 * 1000;

  /** Worker providing the work progress updates. */
  protected final WorkExecutor worker;

  /** Executor used to schedule work progress updates. */
  private final ScheduledExecutorService executor;

  /** The lease duration to request from the external worker service. */
  protected long requestedLeaseDurationMs;

  /** The time period until the next work progress update. */
  protected long progressReportIntervalMs;

  /**
   * The {@link Reader.ForkResult} to report to the service in the next progress update,
   * or {@code null} if there is nothing to report (if no fork happened since the last progress
   * update).
   */
  protected Reader.ForkResult forkResultToReport;

  public WorkProgressUpdater(WorkExecutor worker) {
    this.worker = worker;
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("WorkProgressUpdater-%d").build());
  }

  /**
   * Starts sending work progress updates to the worker service.
   */
  public void startReportingProgress() {
    // Send the initial work progress report half-way through the lease
    // expiration. Subsequent intervals adapt to hints from the service.
    long leaseRemainingTime = leaseRemainingTime(getWorkUnitLeaseExpirationTimestamp());
    progressReportIntervalMs =
        nextProgressReportInterval(leaseRemainingTime / 2, leaseRemainingTime);
    requestedLeaseDurationMs = DEFAULT_LEASE_DURATION_MILLIS;

    LOG.info("Started reporting progress for work item: {}", workString());
    scheduleNextUpdate();
  }

  /**
   * Stops sending work progress updates to the worker service.
   * It may throw an exception if the final progress report fails to be sent for some reason.
   */
  public void stopReportingProgress() throws Exception {
    // TODO: Redesign to get rid of the executor and use a dedicated
    // thread with a sleeper.  Also unify with success/failure reporting.

    // Wait until there are no more progress updates in progress, then
    // shut down.
    synchronized (executor) {
      executor.shutdownNow();
    }

    // We send a final progress report in case there was an unreported fork.
    if (forkResultToReport != null) {
      LOG.info("Sending final progress update with unreported fork: {} "
          + "for work item: {}", forkResultToReport, workString());
      reportProgressHelper(); // This call can fail with an exception
    }

    LOG.info("Stopped reporting progress for work item: {}", workString());
  }

  /**
   * Computes the time before sending the next work progress update making sure
   * that it falls between the [{@link #MIN_REPORTING_INTERVAL_MILLIS},
   * {@link #MAX_REPORTING_INTERVAL_MILLIS}) interval. Makes an attempt to bound
   * the result by the remaining lease time, with an RPC latency margin of
   * {@link #LEASE_RENEWAL_LATENCY_MARGIN}.
   *
   * @param suggestedInterval the suggested progress report interval
   * @param leaseRemainingTime milliseconds left before the work lease expires
   * @return the time in milliseconds before sending the next progress update
   */
  protected static long nextProgressReportInterval(
      long suggestedInterval, long leaseRemainingTime) {
    // Sanitize input in case we get a negative suggested time interval.
    suggestedInterval = Math.max(0, suggestedInterval);

    // Try to send the next progress update before the next lease expiration
    // allowing some RPC latency margin.
    suggestedInterval =
        Math.min(suggestedInterval, leaseRemainingTime - LEASE_RENEWAL_LATENCY_MARGIN);

    // Bound reporting interval to avoid staleness and progress update storms.
    return Math.min(
        Math.max(MIN_REPORTING_INTERVAL_MILLIS, suggestedInterval), MAX_REPORTING_INTERVAL_MILLIS);
  }

  /**
   * Schedules the next work progress update.
   */
  private void scheduleNextUpdate() {
    if (executor.isShutdown()) {
      return;
    }
    executor.schedule(new Runnable() {
      @Override
      public void run() {
        // Don't shut down while reporting progress.
        synchronized (executor) {
          if (executor.isShutdown()) {
            return;
          }
          reportProgress();
        }
      }
    },
        progressReportIntervalMs, TimeUnit.MILLISECONDS);
    LOG.debug("Next work progress update for work item {} scheduled to occur in {} ms.",
        workString(), progressReportIntervalMs);
  }

  /**
   * Reports the current work progress to the worker service.
   */
  private void reportProgress() {
    LOG.info("Updating progress on work item {}", workString());
    try {
      reportProgressHelper();
    } catch (Throwable e) {
      LOG.warn("Error reporting work progress update: ", e);
    } finally {
      scheduleNextUpdate();
    }
  }

  /**
   * Computes the amount of time left, in milliseconds, before a lease
   * with the specified expiration timestamp expires.  Returns zero if
   * the lease has already expired.
   */
  protected long leaseRemainingTime(long leaseExpirationTimestamp) {
    long now = System.currentTimeMillis();
    if (leaseExpirationTimestamp < now) {
      LOG.debug("Lease remaining time for {} is 0 ms.", workString());
      return 0;
    }
    LOG.debug(
        "Lease remaining time for {} is {} ms.", workString(), leaseExpirationTimestamp - now);
    return leaseExpirationTimestamp - now;
  }

  // Visible for testing.
  public Reader.ForkResult getForkResultToReport() {
    return forkResultToReport;
  }

  /**
   * Reports the current work progress to the worker service.
   */
  protected abstract void reportProgressHelper() throws Exception;

  /**
   * Returns the current work item's lease expiration timestamp.
   */
  protected abstract long getWorkUnitLeaseExpirationTimestamp();

  /**
   * Returns a string representation of the work item whose progress
   * is being updated, for use in logging messages.
   */
  protected abstract String workString();
}
