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

import static org.apache.beam.runners.dataflow.util.TimeUtil.toCloudDuration;
import static org.apache.beam.runners.dataflow.util.TimeUtil.toCloudTime;
import static org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames.CAPABILITY_REMOTE_SOURCE;
import static org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames.WORK_ITEM_TYPE_MAP_TASK;
import static org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames.WORK_ITEM_TYPE_REMOTE_SOURCE_TASK;
import static org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames.WORK_ITEM_TYPE_SEQ_MAP_TASK;
import static org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames.WORK_ITEM_TYPE_STREAMING_CONFIG_TASK;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LeaseWorkItemRequest;
import com.google.api.services.dataflow.model.LeaseWorkItemResponse;
import com.google.api.services.dataflow.model.ReportWorkItemStatusRequest;
import com.google.api.services.dataflow.model.ReportWorkItemStatusResponse;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkProgressUpdater;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;

/** A Dataflow WorkUnit client that fetches WorkItems from the Dataflow service. */
@ThreadSafe
class DataflowWorkUnitClient implements WorkUnitClient {
  private final Logger logger;

  /**
   * Work items are reported as complete using this class's reportWorkItemStatus() method on the
   * same thread that requested the item using getWorkItem(). This thread local variable is used to
   * tag the current thread with the stage start time during getWorkItem() so that the elapsed
   * execution time can be easily determined in reportWorkItemStatus(). A similar thread-local
   * mechanism is used in DataflowWorkerLoggingMDC to track other metadata about the current
   * operation being executed.
   */
  private static final ThreadLocal<DateTime> stageStartTime = new ThreadLocal<>();

  private final CounterShortIdCache shortIdCache;
  private final Dataflow dataflow;
  private final DataflowWorkerHarnessOptions options;

  /**
   * Creates a client that fetches WorkItems from the Dataflow service.
   *
   * @param options The pipeline options.
   */
  DataflowWorkUnitClient(DataflowWorkerHarnessOptions options, Logger logger) {
    this.dataflow = options.getDataflowClient();
    this.options = options;
    this.logger = logger;
    this.shortIdCache = new CounterShortIdCache();
  }

  /**
   * Gets a {@link WorkItem} from the Dataflow service, or returns {@link Optional#absent()} if no
   * work was found.
   *
   * <p>If work is returned, the calling thread should call reportWorkItemStatus after completing it
   * and before requesting another work item.
   */
  @Override
  public Optional<WorkItem> getWorkItem() throws IOException {
    List<String> workItemTypes =
        ImmutableList.of(
            WORK_ITEM_TYPE_MAP_TASK,
            WORK_ITEM_TYPE_SEQ_MAP_TASK,
            WORK_ITEM_TYPE_REMOTE_SOURCE_TASK);
    // All remote sources require the "remote_source" capability. Dataflow's
    // custom sources are further tagged with the format "custom_source".
    List<String> capabilities =
        ImmutableList.<String>of(
            options.getWorkerId(), CAPABILITY_REMOTE_SOURCE, PropertyNames.CUSTOM_SOURCE_FORMAT);

    Optional<WorkItem> workItem = getWorkItemInternal(workItemTypes, capabilities);
    if (!workItem.isPresent()) {
      // Normal case, this means that the response contained no work, i.e. no work is available
      // at this time.
      return Optional.absent();
    }
    if (workItem.isPresent() && workItem.get().getId() == null) {
      logger.debug("Discarding invalid work item {}", workItem.orNull());
      return Optional.absent();
    }

    WorkItem work = workItem.get();

    final String stage;
    if (work.getMapTask() != null) {
      stage = work.getMapTask().getStageName();
      logger.info("Starting MapTask stage {}", stage);
    } else if (work.getSeqMapTask() != null) {
      stage = work.getSeqMapTask().getStageName();
      logger.info("Starting SeqMapTask stage {}", stage);
    } else if (work.getSourceOperationTask() != null) {
      stage = work.getSourceOperationTask().getStageName();
      logger.info("Starting SourceOperationTask stage {}", stage);
    } else {
      stage = null;
    }
    DataflowWorkerLoggingMDC.setStageName(stage);

    stageStartTime.set(DateTime.now());
    DataflowWorkerLoggingMDC.setWorkId(Long.toString(work.getId()));

    return workItem;
  }

  /**
   * Gets a global streaming config {@link WorkItem} from the Dataflow service, or returns {@link
   * Optional#absent()} if no work was found.
   */
  @Override
  public Optional<WorkItem> getGlobalStreamingConfigWorkItem() throws IOException {
    return getWorkItemInternal(
        ImmutableList.of(WORK_ITEM_TYPE_STREAMING_CONFIG_TASK), ImmutableList.of());
  }

  /**
   * Gets a streaming config {@link WorkItem} for the given computation from the Dataflow service,
   * or returns {@link Optional#absent()} if no work was found.
   */
  @Override
  public Optional<WorkItem> getStreamingConfigWorkItem(String computationId) throws IOException {
    Preconditions.checkNotNull(computationId);
    return getWorkItemInternal(
        ImmutableList.of("streaming_config_task:" + computationId), ImmutableList.of());
  }

  private Optional<WorkItem> getWorkItemInternal(
      List<String> workItemTypes, List<String> capabilities) throws IOException {
    LeaseWorkItemRequest request = new LeaseWorkItemRequest();
    request.setFactory(Transport.getJsonFactory());
    request.setWorkItemTypes(workItemTypes);
    request.setWorkerCapabilities(capabilities);
    request.setWorkerId(options.getWorkerId());
    request.setCurrentWorkerTime(toCloudTime(DateTime.now()));

    // This shouldn't be necessary, but a valid cloud duration string is
    // required by the Google API parsing framework.  TODO: Fix the framework
    // so that an empty or not-present string can be used as a default value.
    request.setRequestedLeaseDuration(
        toCloudDuration(Duration.millis(WorkProgressUpdater.DEFAULT_LEASE_DURATION_MILLIS)));

    logger.debug("Leasing work: {}", request);

    LeaseWorkItemResponse response =
        dataflow
            .projects()
            .locations()
            .jobs()
            .workItems()
            .lease(options.getProject(), options.getRegion(), options.getJobId(), request)
            .execute();
    logger.debug("Lease work response: {}", response);

    List<WorkItem> workItems = response.getWorkItems();
    if (workItems == null || workItems.isEmpty()) {
      // We didn't lease any work.
      return Optional.absent();
    } else if (workItems.size() > 1) {
      throw new IOException(
          "This version of the SDK expects no more than one work item from the service: "
              + response);
    }
    WorkItem work = response.getWorkItems().get(0);

    // Looks like the work's a'ight.
    return Optional.of(work);
  }

  /** Reports the status of the most recently requested work item. */
  @Override
  public WorkItemServiceState reportWorkItemStatus(WorkItemStatus workItemStatus)
      throws IOException {
    DateTime endTime = DateTime.now();
    workItemStatus.setFactory(Transport.getJsonFactory());
    logger.debug("Reporting work status: {}", workItemStatus);
    // Log the stage execution time of finished stages that have a stage name.  This will not be set
    // in the event this status is associated with a dummy work item.
    if (firstNonNull(workItemStatus.getCompleted(), Boolean.FALSE)
        && DataflowWorkerLoggingMDC.getStageName() != null) {
      DateTime startTime = stageStartTime.get();
      if (startTime != null) {
        // This thread should have been tagged with the stage start time during getWorkItem(),
        Interval elapsed = new Interval(startTime, endTime);
        int numErrors = workItemStatus.getErrors() == null ? 0 : workItemStatus.getErrors().size();
        logger.info(
            "Finished processing stage {} with {} errors in {} seconds ",
            DataflowWorkerLoggingMDC.getStageName(),
            numErrors,
            (double) elapsed.toDurationMillis() / 1000);
      }
    }
    shortIdCache.shortenIdsIfAvailable(workItemStatus.getCounterUpdates());
    ReportWorkItemStatusRequest request =
        new ReportWorkItemStatusRequest()
            .setWorkerId(options.getWorkerId())
            .setWorkItemStatuses(Collections.singletonList(workItemStatus))
            .setCurrentWorkerTime(toCloudTime(endTime));
    ReportWorkItemStatusResponse result =
        dataflow
            .projects()
            .locations()
            .jobs()
            .workItems()
            .reportStatus(options.getProject(), options.getRegion(), options.getJobId(), request)
            .execute();
    if (result == null) {
      logger.warn("Report work item status response: null");
      throw new IOException("Got null work item status response");
    }

    if (result.getWorkItemServiceStates() == null) {
      logger.warn("Report work item status response: {}", result);
      throw new IOException("Report work item status contained no work item service states");
    }
    if (result.getWorkItemServiceStates().size() != 1) {
      logger.warn("Report work item status response: {}", result);
      throw new IOException(
          "This version of the SDK expects exactly one work item service state from the service "
              + "but got "
              + result.getWorkItemServiceStates().size()
              + " states");
    }
    shortIdCache.storeNewShortIds(request, result);
    WorkItemServiceState state = result.getWorkItemServiceStates().get(0);
    logger.debug("ReportWorkItemStatus result: {}", state);
    return state;
  }
}
