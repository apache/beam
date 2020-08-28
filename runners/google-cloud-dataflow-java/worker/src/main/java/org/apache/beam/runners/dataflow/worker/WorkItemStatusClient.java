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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.Status;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingHandler;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitResult;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.Progress;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around {@link WorkUnitClient} with methods for creating and sending work item status
 * updates.
 */
// Very likely real potential for bugs - https://issues.apache.org/jira/browse/BEAM-6565
@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
public class WorkItemStatusClient {

  private static final Logger LOG = LoggerFactory.getLogger(WorkItemStatusClient.class);

  private final WorkItem workItem;
  private final WorkUnitClient workUnitClient;
  private @Nullable DataflowWorkExecutor worker;
  private Long nextReportIndex;

  private transient String uniqueWorkId = null;
  private boolean finalStateSent = false;

  private @Nullable BatchModeExecutionContext executionContext;

  /**
   * Construct a partly-initialized {@link WorkItemStatusClient}. Once the {@link
   * DataflowWorkExecutor worker} has been instantiated initialization should be completed by
   * calling {@link #setWorker}.
   *
   * <p>Initialization is split into two steps because we may need to report failures during the
   * construction of the worker.
   */
  public WorkItemStatusClient(WorkUnitClient workUnitClient, WorkItem workItem) {
    this.workUnitClient = workUnitClient;
    this.workItem = workItem;
    this.nextReportIndex =
        checkNotNull(workItem.getInitialReportIndex(), "WorkItem missing initial report index");
  }

  public String uniqueWorkId() {
    if (uniqueWorkId == null) {
      uniqueWorkId =
          String.format("%s;%s;%s", workItem.getProjectId(), workItem.getJobId(), workItem.getId());
    }
    return uniqueWorkId;
  }

  /**
   * Finish initialization of the {@link WorkItemStatusClient} by assigning the {@link
   * DataflowWorkExecutor} and {@link BatchModeExecutionContext}.
   */
  public synchronized void setWorker(
      DataflowWorkExecutor worker, BatchModeExecutionContext executionContext) {
    checkArgument(worker != null, "worker must be non-null");
    checkState(this.worker == null, "Can only call setWorker once");
    this.worker = worker;
    this.executionContext = executionContext;
  }

  /** Return the {@link WorkItemServiceState} resulting from sending an error completion status. */
  public synchronized WorkItemServiceState reportError(Throwable e) throws IOException {
    checkState(!finalStateSent, "cannot reportUpdates after sending a final state");
    WorkItemStatus status = createStatusUpdate(true);

    // TODO: Provide more structure representation of error, e.g., the serialized exception object.
    // TODO: Look into moving the stack trace thinning into the client.
    Throwable t = e instanceof UserCodeException ? e.getCause() : e;

    Status error = new Status();
    error.setCode(2); // Code.UNKNOWN.  TODO: Replace with a generated definition.
    // TODO: Attach the stack trace as exception details, not to the message.
    String logPrefix = String.format("Failure processing work item %s", uniqueWorkId());
    if (isOutOfMemoryError(t)) {
      String message =
          "An OutOfMemoryException occurred. Consider specifying higher memory "
              + "instances in PipelineOptions.\n";
      LOG.error("{}: {}", logPrefix, message);
      error.setMessage(message + DataflowWorkerLoggingHandler.formatException(t));
    } else {
      LOG.error(
          "{}: Uncaught exception occurred during work unit execution. This will be retried.",
          logPrefix,
          t);
      error.setMessage(DataflowWorkerLoggingHandler.formatException(t));
    }
    status.setErrors(ImmutableList.of(error));

    return execute(status);
  }

  /** Return the {@link WorkItemServiceState} resulting from sending a success completion status. */
  public synchronized WorkItemServiceState reportSuccess() throws IOException {
    checkState(!finalStateSent, "cannot reportSuccess after sending a final state");
    checkState(worker != null, "setWorker should be called before reportSuccess");

    WorkItemStatus status = createStatusUpdate(true);

    if (worker instanceof SourceOperationExecutor) {
      // TODO: Find out a generic way for the DataflowWorkExecutor to report work-specific results
      // into the work update.
      SourceOperationResponse response = ((SourceOperationExecutor) worker).getResponse();
      if (response != null) {
        status.setSourceOperationResponse(response);
      }
    }

    LOG.info("Success processing work item {}", uniqueWorkId());

    return execute(status);
  }

  /** Return the {@link WorkItemServiceState} resulting from sending a progress update. */
  public synchronized WorkItemServiceState reportUpdate(
      @Nullable DynamicSplitResult dynamicSplitResult, Duration requestedLeaseDuration)
      throws Exception {
    checkState(worker != null, "setWorker should be called before reportUpdate");
    checkState(!finalStateSent, "cannot reportUpdates after sending a final state");
    checkArgument(requestedLeaseDuration != null, "requestLeaseDuration must be non-null");

    WorkItemStatus status = createStatusUpdate(false);
    status.setRequestedLeaseDuration(TimeUtil.toCloudDuration(requestedLeaseDuration));
    populateProgress(status);
    populateSplitResult(status, dynamicSplitResult);

    return execute(status);
  }

  private static boolean isOutOfMemoryError(Throwable t) {
    while (t != null) {
      if (t instanceof OutOfMemoryError) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  private @Nullable synchronized WorkItemServiceState execute(WorkItemStatus status)
      throws IOException {
    WorkItemServiceState result = workUnitClient.reportWorkItemStatus(status);
    if (result != null) {
      nextReportIndex =
          checkNotNull(
              result.getNextReportIndex(),
              "Missing next work index in " + result + " when reporting " + status);
      commitMetrics();
    }

    if (status.getCompleted()) {
      checkState(!finalStateSent, "cannot reportUpdates after sending a final state");
      finalStateSent = true;
    }

    return result;
  }

  @VisibleForTesting
  synchronized void populateSplitResult(
      WorkItemStatus status, DynamicSplitResult dynamicSplitResult) {
    if (dynamicSplitResult instanceof NativeReader.DynamicSplitResultWithPosition) {
      NativeReader.DynamicSplitResultWithPosition asPosition =
          (NativeReader.DynamicSplitResultWithPosition) dynamicSplitResult;
      status.setStopPosition(
          SourceTranslationUtils.toCloudPosition(asPosition.getAcceptedPosition()));
    } else if (dynamicSplitResult instanceof WorkerCustomSources.BoundedSourceSplit) {
      status.setDynamicSourceSplit(
          WorkerCustomSources.toSourceSplit(
              (WorkerCustomSources.BoundedSourceSplit<?>) dynamicSplitResult));
    } else if (dynamicSplitResult != null) {
      throw new IllegalArgumentException(
          "Unexpected type of dynamic split result: " + dynamicSplitResult);
    }
  }

  @VisibleForTesting
  synchronized void populateProgress(WorkItemStatus status) throws Exception {
    Progress progress = worker.getWorkerProgress();
    if (progress != null) {
      status.setReportedProgress(SourceTranslationUtils.readerProgressToCloudProgress(progress));
    }
  }

  @VisibleForTesting
  synchronized void populateMetricUpdates(WorkItemStatus status) {
    List<MetricUpdate> updates = new ArrayList<>();
    if (executionContext != null && executionContext.getExecutionStateTracker() != null) {
      ExecutionStateTracker tracker = executionContext.getExecutionStateTracker();

      MetricUpdate update = new MetricUpdate();
      update.setKind("internal");
      MetricStructuredName name = new MetricStructuredName();
      name.setName("state-sampler");
      update.setName(name);
      Map<String, Object> metric = new HashMap<>();
      ExecutionState state = tracker.getCurrentState();
      if (state != null) {
        metric.put("last-state-name", state.getDescription());
      }
      metric.put("num-transitions", tracker.getNumTransitions());
      metric.put("last-state-duration-ms", tracker.getMillisSinceLastTransition());
      update.setInternal(metric);
      updates.add(update);
    }
    status.setMetricUpdates(updates);
  }

  private synchronized WorkItemStatus createStatusUpdate(boolean isFinal) {
    WorkItemStatus status = new WorkItemStatus();
    status.setWorkItemId(Long.toString(workItem.getId()));
    status.setCompleted(isFinal);
    status.setReportIndex(
        checkNotNull(nextReportIndex, "nextReportIndex should be non-null when sending an update"));

    if (worker != null) {
      populateMetricUpdates(status);
      populateCounterUpdates(status);
    }

    double throttleTime = extractThrottleTime();
    status.setTotalThrottlerWaitTimeSeconds(throttleTime);
    return status;
  }

  @VisibleForTesting
  synchronized void populateCounterUpdates(WorkItemStatus status) {
    if (worker == null) {
      return;
    }

    // Checking against boolean, because getCompleted can return null
    boolean isFinalUpdate = Boolean.TRUE.equals(status.getCompleted());

    Map<Object, CounterUpdate> counterUpdatesMap = new HashMap<>();

    final Consumer<CounterUpdate> appendCounterUpdate =
        x ->
            counterUpdatesMap.put(
                x.getStructuredNameAndMetadata() == null
                    ? x.getNameAndKind()
                    : x.getStructuredNameAndMetadata(),
                x);

    // Output counters
    extractCounters(worker.getOutputCounters()).forEach(appendCounterUpdate);

    // User metrics reported in Worker
    extractMetrics(isFinalUpdate).forEach(appendCounterUpdate);

    // MSec counters reported in worker
    extractMsecCounters(isFinalUpdate).forEach(appendCounterUpdate);

    // Metrics reported in SDK runner.
    // This includes all different kinds of metrics coming from SDK.
    // Keep in mind that these metrics might contain different types of counter names:
    // i.e. structuredNameAndMetadata and nameAndKind
    worker.extractMetricUpdates().forEach(appendCounterUpdate);

    status.setCounterUpdates(ImmutableList.copyOf(counterUpdatesMap.values()));
  }

  private synchronized Iterable<CounterUpdate> extractCounters(@Nullable CounterSet counters) {
    if (counters == null) {
      return Collections.emptyList();
    }

    // Currently we lack a reliable exactly-once delivery mechanism for
    // work updates, i.e. they can be retried or reordered, so sending
    // delta updates could lead to double-counted or missed contributions.
    // However, delta updates may be beneficial for performance.
    // TODO: Implement exactly-once delivery and use deltas,
    // if it ever becomes clear that deltas are necessary for performance.
    boolean delta = false;
    return counters.extractUpdates(delta, DataflowCounterUpdateExtractor.INSTANCE);
  }

  /**
   * This and {@link #commitMetrics} need to be synchronized since we should not call {@link
   * MetricsContainerImpl#getUpdates} on any object within an operation while also calling {@link
   * MetricsContainerImpl#commitUpdates}.
   */
  private synchronized Iterable<CounterUpdate> extractMetrics(boolean isFinalUpdate) {
    return executionContext == null
        ? Collections.emptyList()
        : executionContext.extractMetricUpdates(isFinalUpdate);
  }

  public Iterable<CounterUpdate> extractMsecCounters(boolean isFinalUpdate) {
    return executionContext == null
        ? Collections.emptyList()
        : executionContext.extractMsecCounters(isFinalUpdate);
  }

  public long extractThrottleTime() {
    return executionContext == null ? 0L : executionContext.extractThrottleTime();
  }

  /**
   * This and {@link #extractMetrics} need to be synchronized since we should not call {@link
   * MetricsContainerImpl#getUpdates} on any object within an operation while also calling {@link
   * MetricsContainerImpl#commitUpdates}.
   */
  @VisibleForTesting
  synchronized void commitMetrics() {
    if (executionContext == null) {
      return;
    }

    executionContext.commitMetricUpdates();
  }
}
