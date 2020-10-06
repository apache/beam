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

import com.google.api.client.util.Clock;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.HotKeyDetection;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkProgressUpdater;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataflowWorkProgressUpdater implements the WorkProgressUpdater interface for the Cloud Dataflow
 * system.
 */
@NotThreadSafe
public class DataflowWorkProgressUpdater extends WorkProgressUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkProgressUpdater.class);

  private final WorkItemStatusClient workItemStatusClient;

  /** The WorkItem for which work progress updates are sent. */
  private final WorkItem workItem;

  private HotKeyLogger hotKeyLogger;

  private boolean wasAskedToAbort = false;

  public DataflowWorkProgressUpdater(
      WorkItemStatusClient workItemStatusClient, WorkItem workItem, WorkExecutor worker) {
    super(worker, Integer.MAX_VALUE);
    this.workItemStatusClient = workItemStatusClient;
    this.workItem = workItem;
    this.hotKeyLogger = new HotKeyLogger();
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
      HotKeyLogger hotKeyLogger) {
    super(worker, Integer.MAX_VALUE, executor, clock);
    this.workItemStatusClient = workItemStatusClient;
    this.workItem = workItem;
    this.hotKeyLogger = hotKeyLogger;
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
    WorkItemServiceState result =
        workItemStatusClient.reportUpdate(
            dynamicSplitResultToReport, Duration.millis(requestedLeaseDurationMs));

    if (result != null) {
      if (result.getCompleteWorkStatus() != null
          && result.getCompleteWorkStatus().getCode() != com.google.rpc.Code.OK.getNumber()) {
        LOG.info(
            "Service asked worker to abort with message "
                + result.getCompleteWorkStatus().toString());
        wasAskedToAbort = true;
        worker.abort();
        return;
      }

      if (result.getHotKeyDetection() != null
          && result.getHotKeyDetection().getUserStepName() != null) {
        HotKeyDetection hotKeyDetection = result.getHotKeyDetection();
        hotKeyLogger.logHotKeyDetection(
            hotKeyDetection.getUserStepName(),
            TimeUtil.fromCloudDuration(hotKeyDetection.getHotKeyAge()));
      }

      // Resets state after a successful progress report.
      dynamicSplitResultToReport = null;

      progressReportIntervalMs =
          nextProgressReportInterval(
              fromCloudDuration(result.getReportStatusInterval()).getMillis(),
              leaseRemainingTime(getLeaseExpirationTimestamp(result)));

      ApproximateSplitRequest suggestedStopPoint = result.getSplitRequest();
      if (suggestedStopPoint != null) {
        LOG.info("Proposing dynamic split of work unit {} at {}", workString(), suggestedStopPoint);
        dynamicSplitResultToReport =
            worker.requestDynamicSplit(
                SourceTranslationUtils.toDynamicSplitRequest(suggestedStopPoint));
      }
    }
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
