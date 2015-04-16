/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.DataflowWorker.buildStatus;
import static com.google.cloud.dataflow.sdk.runners.worker.DataflowWorker.uniqueId;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toDynamicSplitRequest;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.fromCloudDuration;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.fromCloudTime;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkProgressUpdater;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * DataflowWorkProgressUpdater implements the WorkProgressUpdater
 * interface for the Cloud Dataflow system.
 */
@NotThreadSafe
public class DataflowWorkProgressUpdater extends WorkProgressUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkProgressUpdater.class);

  /** The Dataflow Worker WorkItem client. */
  private final DataflowWorker.WorkUnitClient workUnitClient;

  /** The WorkItem for which work progress updates are sent. */
  private final WorkItem workItem;

  /** Options specifying information about the pipeline run by the worker.*/
  private final DataflowWorkerHarnessOptions options;

  /** The index to use for the next report sent for the updater's work item. */
  private long nextReportIndex;

  public DataflowWorkProgressUpdater(WorkItem workItem, WorkExecutor worker,
      DataflowWorker.WorkUnitClient workUnitClient, DataflowWorkerHarnessOptions options) {
    super(worker);
    this.workItem = workItem;
    this.workUnitClient = workUnitClient;
    this.options = options;
    this.nextReportIndex = workItem.getInitialReportIndex();
  }

  @Override
  protected String workString() {
    return uniqueId(workItem);
  }

  @Override
  protected long getWorkUnitLeaseExpirationTimestamp() {
    return getLeaseExpirationTimestamp(workItem);
  }

  @Override
  protected long getWorkUnitSuggestedReportingInterval(){
    return fromCloudDuration(workItem.getReportStatusInterval()).getMillis();
  }

  @Override
  protected void reportProgressHelper() throws Exception {
    WorkItemStatus status = buildStatus(workItem, false/*completed*/, worker.getOutputCounters(),
        worker.getOutputMetrics(), options, worker.getWorkerProgress(), dynamicSplitResultToReport,
        null/*sourceOperationResponse*/, null/*errors*/,
        getNextReportIndex());
    status.setRequestedLeaseDuration(toCloudDuration(Duration.millis(requestedLeaseDurationMs)));

    WorkItemServiceState result = workUnitClient.reportWorkItemStatus(status);
    if (result != null) {
      // Resets state after a successful progress report.
      dynamicSplitResultToReport = null;
      nextReportIndex = result.getNextReportIndex();

      progressReportIntervalMs = nextProgressReportInterval(
          fromCloudDuration(result.getReportStatusInterval()).getMillis(),
          leaseRemainingTime(getLeaseExpirationTimestamp(result)));

      ApproximateProgress suggestedStopPoint = result.getSuggestedStopPoint();
      if (suggestedStopPoint != null) {
        LOG.info("Proposing dynamic split of work unit {} at {}", workString(), suggestedStopPoint);
        dynamicSplitResultToReport = worker.requestDynamicSplit(
            toDynamicSplitRequest(suggestedStopPoint));
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

  /**
   * Returns the index to use for the next work item report for the work
   * progress updater's work item.
   */
  long getNextReportIndex() {
    return nextReportIndex;
  }
}
