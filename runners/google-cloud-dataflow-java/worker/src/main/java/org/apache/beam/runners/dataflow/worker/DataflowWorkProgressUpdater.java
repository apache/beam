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

import static org.apache.beam.runners.dataflow.util.TimeUtil.fromCloudDuration;
import static org.apache.beam.runners.dataflow.util.TimeUtil.fromCloudTime;
import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;

import com.google.api.client.util.Clock;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.HotKeyDetection;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkProgressUpdater;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataflowWorkProgressUpdater implements the WorkProgressUpdater interface for the Cloud Dataflow
 * system.
 */
@NotThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DataflowWorkProgressUpdater extends WorkProgressUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkProgressUpdater.class);

  /**
   * How long executor creation may run before the first "still creating" warning. Matches the point
   * where {@code DoFnInstanceManagers} calls a single DoFn.setup slow (3 minutes).
   */
  private static final long SLOW_SETUP_FIRST_WARNING_MILLIS = 3 * 60 * 1000;

  /** Gap between repeated "still creating" warnings, so a stuck setup keeps showing in the log. */
  private static final long SLOW_SETUP_WARNING_INTERVAL_MILLIS = 5 * 60 * 1000;

  private final WorkItemStatusClient workItemStatusClient;

  /** The WorkItem for which work progress updates are sent. */
  private final WorkItem workItem;

  private final DataflowPipelineOptions options;

  private HotKeyLogger hotKeyLogger;

  private volatile boolean wasAskedToAbort = false;

  /**
   * Guards the hand-off between an abort that arrives while the executor is still being created and
   * {@link #setWorker}, so the abort reaches the executor whichever thread runs first.
   */
  private final Object abortLock = new Object();

  /** When this updater was created, which for the batch worker is when executor creation starts. */
  private final long createdAtMillis;

  /** Only read and written by the progress reporting thread. */
  private long nextSlowSetupWarningAtMillis;

  public DataflowWorkProgressUpdater(
      WorkItemStatusClient workItemStatusClient,
      WorkItem workItem,
      WorkExecutor worker,
      DataflowPipelineOptions options) {
    super(worker, Integer.MAX_VALUE);
    this.workItemStatusClient = workItemStatusClient;
    this.workItem = workItem;
    this.hotKeyLogger = new HotKeyLogger();
    this.options = options;
    this.createdAtMillis = clock.currentTimeMillis();
    this.nextSlowSetupWarningAtMillis = createdAtMillis + SLOW_SETUP_FIRST_WARNING_MILLIS;
  }

  /**
   * The {@link ScheduledExecutorService} parameter is used to inject a stubbed executor that uses
   * virtual time for testing, and the {@link Clock} parameter is used to inject a mock clock that
   * provides virtual time.
   */
  @VisibleForTesting
  DataflowWorkProgressUpdater(
      WorkItemStatusClient workItemStatusClient,
      WorkItem workItem,
      WorkExecutor worker,
      ScheduledExecutorService executor,
      Clock clock,
      HotKeyLogger hotKeyLogger,
      DataflowPipelineOptions options) {
    super(worker, Integer.MAX_VALUE, executor, clock);
    this.workItemStatusClient = workItemStatusClient;
    this.workItem = workItem;
    this.hotKeyLogger = hotKeyLogger;
    this.options = options;
    this.createdAtMillis = clock.currentTimeMillis();
    this.nextSlowSetupWarningAtMillis = createdAtMillis + SLOW_SETUP_FIRST_WARNING_MILLIS;
  }

  @Override
  public void setWorker(WorkExecutor worker) {
    boolean abortRequested;
    synchronized (abortLock) {
      super.setWorker(worker);
      abortRequested = wasAskedToAbort;
    }
    // a map task executor remembers the abort, so its read loop stops as soon as execute() starts
    if (abortRequested) {
      worker.abort();
    }
  }

  @Override
  protected String workString() {
    return workItemStatusClient.uniqueWorkId();
  }

  @Override
  protected long getWorkUnitLeaseExpirationTimestamp() {
    return getLeaseExpirationTimestamp(workItem);
  }

  @Override
  protected long getWorkUnitSuggestedReportingInterval() {
    return fromCloudDuration(workItem.getReportStatusInterval()).getMillis();
  }

  @Override
  protected void reportProgressHelper() throws Exception {
    if (wasAskedToAbort) {
      LOG.info("Service already asked to abort work item, not reporting ignored progress.");
      return;
    }
    if (Objects.isNull(worker)) {
      warnIfSetupIsSlow();
    }
    WorkItemServiceState result =
        workItemStatusClient.reportUpdate(
            dynamicSplitResultToReport, Duration.millis(requestedLeaseDurationMs));

    if (result != null) {
      if (result.getCompleteWorkStatus() != null
          && result.getCompleteWorkStatus().getCode() != com.google.rpc.Code.OK.getNumber()) {
        LOG.info("Service asked worker to abort with status: {}", result.getCompleteWorkStatus());
        WorkExecutor currentWorker;
        synchronized (abortLock) {
          wasAskedToAbort = true;
          currentWorker = worker;
        }
        // the executor may still be under construction, in which case setWorker aborts it
        if (Objects.nonNull(currentWorker)) {
          currentWorker.abort();
        }
        return;
      }

      if (result.getHotKeyDetection() != null
          && result.getHotKeyDetection().getUserStepName() != null) {
        HotKeyDetection hotKeyDetection = result.getHotKeyDetection();

        // The key set the in BatchModeExecutionContext is only set in the GroupingShuffleReader
        // which is the correct key. The key is also translated into a Java object in the reader.
        if (options.isHotKeyLoggingEnabled() || hasExperiment(options, "enable_hot_key_logging")) {
          hotKeyLogger.logHotKeyDetection(
              hotKeyDetection.getUserStepName(),
              TimeUtil.fromCloudDuration(hotKeyDetection.getHotKeyAge()),
              workItemStatusClient.getExecutionContext().getKey());
        } else {
          hotKeyLogger.logHotKeyDetection(
              hotKeyDetection.getUserStepName(),
              TimeUtil.fromCloudDuration(hotKeyDetection.getHotKeyAge()));
        }
      }

      // Resets state after a successful progress report.
      dynamicSplitResultToReport = null;

      progressReportIntervalMs =
          nextProgressReportInterval(
              fromCloudDuration(result.getReportStatusInterval()).getMillis(),
              leaseRemainingTime(getLeaseExpirationTimestamp(result)));

      ApproximateSplitRequest suggestedStopPoint = result.getSplitRequest();
      // there is nothing to split until the executor exists, the service asks again later
      if (suggestedStopPoint != null && Objects.nonNull(worker)) {
        LOG.info("Proposing dynamic split of work unit {} at {}", workString(), suggestedStopPoint);
        dynamicSplitResultToReport =
            worker.requestDynamicSplit(
                SourceTranslationUtils.toDynamicSplitRequest(suggestedStopPoint));
      }
    }
  }

  private void warnIfSetupIsSlow() {
    long now = clock.currentTimeMillis();
    if (now < nextSlowSetupWarningAtMillis) {
      return;
    }

    // the lease keeps being extended while setup runs, so a stuck setup never fails the work item
    // on its own. Repeating the warning is what makes it visible in the worker log
    nextSlowSetupWarningAtMillis = now + SLOW_SETUP_WARNING_INTERVAL_MILLIS;
    LOG.warn(
        "Work item {} is still creating its executor after {} seconds. Creating the executor runs"
            + " DoFn.setup, so a slow or stuck setup keeps this work item leased without"
            + " processing any elements.",
        workString(),
        (now - createdAtMillis) / 1000);
  }

  /** Returns the given work unit's lease expiration timestamp. */
  private long getLeaseExpirationTimestamp(WorkItem workItem) {
    return fromCloudTime(workItem.getLeaseExpireTime()).getMillis();
  }

  /** Returns the given work unit service state lease expiration timestamp. */
  private long getLeaseExpirationTimestamp(WorkItemServiceState workItemServiceState) {
    return fromCloudTime(workItemServiceState.getLeaseExpireTime()).getMillis();
  }
}
