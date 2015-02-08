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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudSourceOperationResponseToSourceOperationResponse;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceOperationResponseToCloudSourceOperationResponse;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;

import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.Status;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudCounterUtils;
import com.google.cloud.dataflow.sdk.util.CloudMetricUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.Metric;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.SourceFormat;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkProgressUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

/**
 * This is a semi-abstract harness for executing WorkItem tasks in
 * Java workers. Concrete implementations need to implement a
 * WorkUnitClient.
 *
 * <p>DataflowWorker presents one public interface,
 * getAndPerformWork(), which uses the WorkUnitClient to get work,
 * execute it, and update the work.
 */
public class DataflowWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorker.class);

  /**
   * A client to get and update work items.
   */
  private final WorkUnitClient workUnitClient;

  /**
   * Pipeline options, initially provided via the constructor and
   * partially provided via each work work unit.
   */
  private final DataflowWorkerHarnessOptions options;

  public DataflowWorker(WorkUnitClient workUnitClient, DataflowWorkerHarnessOptions options) {
    this.workUnitClient = workUnitClient;
    this.options = options;
  }

  /**
   * Gets WorkItem and performs it; returns true if work was
   * successfully completed.
   *
   * getAndPerformWork may throw if there is a failure of the
   * WorkUnitClient.
   */
  public boolean getAndPerformWork() throws IOException {
    WorkItem work = workUnitClient.getWorkItem();
    if (work == null) {
      return false;
    }
    return doWork(work);
  }

  /**
   * Performs the given work; returns true if successful.
   *
   * @throws IOException Only if the WorkUnitClient fails.
   */
  private boolean doWork(WorkItem workItem) throws IOException {
    LOG.info("Executing: {}", workItem);

    WorkExecutor worker = null;
    try {
      // Populate PipelineOptions with data from work unit.
      options.setProject(workItem.getProjectId());

      ExecutionContext executionContext = new BatchModeExecutionContext();

      if (workItem.getMapTask() != null) {
        worker = MapTaskExecutorFactory.create(options, workItem.getMapTask(), executionContext);

      } else if (workItem.getSourceOperationTask() != null) {
        worker = SourceOperationExecutorFactory.create(options, workItem.getSourceOperationTask());

      } else {
        throw new RuntimeException("unknown kind of work item: " + workItem.toString());
      }

      WorkProgressUpdater progressUpdater =
          new DataflowWorkProgressUpdater(workItem, worker, workUnitClient, options);
      progressUpdater.startReportingProgress();

      // Blocks while executing the work.
      // TODO: refactor to allow multiple work unit
      // processing threads.
      worker.execute();

      // Log all counter values for debugging purposes.
      CounterSet counters = worker.getOutputCounters();
      for (Counter counter : counters) {
        LOG.info("COUNTER {}.", counter);
      }

      // Log all metrics for debugging purposes.
      Collection<Metric<?>> metrics = worker.getOutputMetrics();
      for (Metric<?> metric : metrics) {
        LOG.info("METRIC {}: {}", metric.getName(), metric.getValue());
      }

      // stopReportingProgress can throw an exception if the final progress
      // update fails. For correctness, the task must then be marked as failed.
      progressUpdater.stopReportingProgress();

      // Report job success.

      // TODO: Find out a generic way for the WorkExecutor to report work-specific results
      // into the work update.
      SourceFormat.OperationResponse operationResponse =
          (worker instanceof SourceOperationExecutor)
              ? cloudSourceOperationResponseToSourceOperationResponse(
                  ((SourceOperationExecutor) worker).getResponse())
              : null;
      reportStatus(
          options, "Success", workItem, counters, metrics, operationResponse, null/*errors*/);

      return true;

    } catch (Throwable e) {
      handleWorkError(workItem, worker, e);
      return false;

    } finally {
      if (worker != null) {
        try {
          worker.close();
        } catch (Exception exn) {
          LOG.warn("Uncaught exception occurred during work unit shutdown:", exn);
        }
      }
    }
  }

  /** Handles the exception thrown when reading and executing the work. */
  private void handleWorkError(WorkItem workItem, WorkExecutor worker, Throwable e)
      throws IOException {
    LOG.warn("Uncaught exception occurred during work unit execution:", e);

    // TODO: Look into moving the stack trace thinning
    // into the client.
    Throwable t = e instanceof UserCodeException ? e.getCause() : e;
    Status error = new Status();
    error.setCode(2); // Code.UNKNOWN.  TODO: Replace with a generated definition.
    // TODO: Attach the stack trace as exception details, not to the message.
    error.setMessage(buildCloudStackTrace(t));

    reportStatus(options, "Failure", workItem, worker == null ? null : worker.getOutputCounters(),
        worker == null ? null : worker.getOutputMetrics(), null/*sourceOperationResponse*/,
        error == null ? null : Collections.singletonList(error));
  }

  /**
   * Recursively goes through an exception, pulling out the stack trace. If the
   * exception is a chained exception, it recursively goes through any causes
   * and appends them to the stack trace.
   */
  private static String buildCloudStackTrace(Throwable t) {
    StringWriter result = new StringWriter();
    PrintWriter printResult = new PrintWriter(result);

    printResult.print("Exception: ");
    for (;;) {
      printResult.println(t.toString());
      for (StackTraceElement frame : t.getStackTrace()) {
        printResult.println(frame.toString());
      }
      t = t.getCause();
      if (t == null) {
        break;
      }
      printResult.print("Caused by: ");
    }
    return result.toString();
  }

  private void reportStatus(DataflowWorkerHarnessOptions options, String status, WorkItem workItem,
      @Nullable CounterSet counters, @Nullable Collection<Metric<?>> metrics,
      @Nullable SourceFormat.OperationResponse operationResponse, @Nullable List<Status> errors)
      throws IOException {
    LOG.info("{} processing work item {}", status, uniqueId(workItem));
    WorkItemStatus workItemStatus = buildStatus(workItem, true/*completed*/, counters, metrics,
        options, null, null, operationResponse, errors);
    workUnitClient.reportWorkItemStatus(workItemStatus);
  }

  static WorkItemStatus buildStatus(WorkItem workItem, boolean completed,
      @Nullable CounterSet counters, @Nullable Collection<Metric<?>> metrics,
      DataflowWorkerHarnessOptions options, @Nullable Reader.Progress progress,
      @Nullable Reader.ForkResult forkResult,
      @Nullable SourceFormat.OperationResponse operationResponse, @Nullable List<Status> errors) {
    WorkItemStatus status = new WorkItemStatus();
    status.setWorkItemId(Long.toString(workItem.getId()));
    status.setCompleted(completed);

    List<MetricUpdate> counterUpdates = null;
    List<MetricUpdate> metricUpdates = null;

    if (counters != null) {
      // Currently we lack a reliable exactly-once delivery mechanism for
      // work updates, i.e. they can be retried or reordered, so sending
      // delta updates could lead to double-counted or missed contributions.
      // However, delta updates may be beneficial for performance.
      // TODO: Implement exactly-once delivery and use deltas,
      // if it ever becomes clear that deltas are necessary for performance.
      boolean delta = false;
      counterUpdates = CloudCounterUtils.extractCounters(counters, delta);
    }
    if (metrics != null) {
      metricUpdates = CloudMetricUtils.extractCloudMetrics(metrics, options.getWorkerId());
    }
    List<MetricUpdate> updates = null;
    if (counterUpdates == null) {
      updates = metricUpdates;
    } else if (metrics == null) {
      updates = counterUpdates;
    } else {
      updates = new ArrayList<>();
      updates.addAll(counterUpdates);
      updates.addAll(metricUpdates);
    }
    status.setMetricUpdates(updates);

    // TODO: Provide more structure representation of error,
    // e.g., the serialized exception object.
    if (errors != null) {
      status.setErrors(errors);
    }

    if (progress != null) {
      status.setProgress(readerProgressToCloudProgress(progress));
    }
    if (forkResult instanceof Reader.ForkResultWithPosition) {
      Reader.ForkResultWithPosition asPosition = (Reader.ForkResultWithPosition) forkResult;
      status.setStopPosition(toCloudPosition(asPosition.getAcceptedPosition()));
    } else if (forkResult != null) {
      throw new IllegalArgumentException("Unexpected type of fork result: " + forkResult);
    }

    if (workItem.getSourceOperationTask() != null) {
      status.setSourceOperationResponse(
          sourceOperationResponseToCloudSourceOperationResponse(operationResponse));
    }

    return status;
  }

  static String uniqueId(WorkItem work) {
    return work.getProjectId() + ";" + work.getJobId() + ";" + work.getId();
  }

  /**
   * Abstract base class describing a client for WorkItem work units.
   */
  public abstract static class WorkUnitClient {
    /**
     * Returns a new WorkItem unit for this Worker to work on or null
     * if no work item is available.
     */
    public abstract WorkItem getWorkItem() throws IOException;

    /**
     * Reports a {@link WorkItemStatus} for an assigned {@link WorkItem}.
     *
     * @param workItemStatus the status to report
     * @return a {@link WorkItemServiceState} (e.g. a new stop position)
     */
    public abstract WorkItemServiceState reportWorkItemStatus(WorkItemStatus workItemStatus)
        throws IOException;
  }
}
