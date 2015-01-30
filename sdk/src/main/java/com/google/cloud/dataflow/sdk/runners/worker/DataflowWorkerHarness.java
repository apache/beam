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

import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudTime;

import com.google.api.client.util.Lists;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LeaseWorkItemRequest;
import com.google.api.services.dataflow.model.LeaseWorkItemResponse;
import com.google.api.services.dataflow.model.ReportWorkItemStatusRequest;
import com.google.api.services.dataflow.model.ReportWorkItemStatusResponse;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingFormatter;
import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingInitializer;
import com.google.cloud.dataflow.sdk.util.GcsIOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.common.collect.ImmutableList;

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is a harness for executing WorkItem tasks in Java workers.
 * <p>
 * The worker fetches WorkItem units from the Dataflow Service.
 * When the work is complete, the program sends results via the worker service API.
 * <p>
 * Returns status code 0 on successful completion, 1 on any uncaught failures.
 * <p>
 * TODO: add support for VM initialization via config.
 * During initialization, we should take a configuration which specifies
 * an initialization function, allowing user code to run on VM startup.
 */
public class DataflowWorkerHarness {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkerHarness.class);

  private static final String APPLICATION_NAME = "DataflowWorkerHarness";

  /**
   * This uncaught exception handler logs the {@link Throwable} to the logger, {@link System#err}
   * and exits the application with status code 1.
   */
  static class WorkerUncaughtExceptionHandler implements UncaughtExceptionHandler {
    static final WorkerUncaughtExceptionHandler INSTANCE = new WorkerUncaughtExceptionHandler();

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Uncaught exception in main thread. Exiting with status code 1.", e);
      System.err.println("Uncaught exception in main thread. Exiting with status code 1.");
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Fetches and processes work units from the Dataflow service.
   */
  public static void main(String[] args) throws Exception {
    Thread.currentThread().setUncaughtExceptionHandler(WorkerUncaughtExceptionHandler.INSTANCE);
    new DataflowWorkerLoggingInitializer().initialize();

    DataflowWorkerHarnessOptions pipelineOptions =
        PipelineOptionsFactory.createFromSystemProperties();
    final DataflowWorker worker = create(pipelineOptions);
    processWork(pipelineOptions, worker);
  }

  // Visible for testing.
  static void processWork(DataflowWorkerHarnessOptions pipelineOptions,
      final DataflowWorker worker) {

    long startTime = DateTimeUtils.currentTimeMillis();
    int numThreads = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    CompletionService<Boolean> completionService =
        new ExecutorCompletionService<>(pipelineOptions.getExecutorService());
    for (int i = 0; i < numThreads; ++i) {
      completionService.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return worker.getAndPerformWork();
        }
      });
    }

    List<Long> completionTimes = Lists.newArrayList();
    for (int i = 0; i < numThreads; ++i) {
      try {
        // CompletionService returns the tasks in the order in which the completed at.
        completionService.take().get();
      } catch (Exception e) {
        LOG.error("Failed waiting on thread to process work.", e);
      }
      completionTimes.add(DateTimeUtils.currentTimeMillis());
    }

    long endTime = DateTimeUtils.currentTimeMillis();
    LOG.info("processWork() start time: {}, end time: {}",
        ISODateTimeFormat.dateTime().print(startTime),
        ISODateTimeFormat.dateTime().print(endTime));
    for (long completionTime : completionTimes) {
      LOG.info("Duration: {}ms Wasted Time: {}ms",
          completionTime - startTime,
          endTime - completionTime);
    }
  }

  static DataflowWorker create(DataflowWorkerHarnessOptions options) {
    MDC.put(DataflowWorkerLoggingFormatter.MDC_DATAFLOW_JOB_ID, options.getJobId());
    MDC.put(DataflowWorkerLoggingFormatter.MDC_DATAFLOW_WORKER_ID, options.getWorkerId());
    options.setAppName(APPLICATION_NAME);

    // Configure standard IO factories.
    IOChannelUtils.setIOFactory("gs", new GcsIOChannelFactory(options));

    DataflowWorkUnitClient client = DataflowWorkUnitClient.fromOptions(options);
    return new DataflowWorker(client, options);
  }

  /**
   * A Dataflow WorkUnit client that fetches WorkItems from the Dataflow service.
   */
  @ThreadSafe
  static class DataflowWorkUnitClient extends DataflowWorker.WorkUnitClient {
    private final Dataflow dataflow;
    private final DataflowWorkerHarnessOptions options;

    /**
     * Creates a client that fetches WorkItems from the Dataflow service.
     *
     * @param options The pipeline options.
     * @return A WorkItemClient that fetches WorkItems from the Dataflow service.
     */
    static DataflowWorkUnitClient fromOptions(DataflowWorkerHarnessOptions options) {
      return new DataflowWorkUnitClient(
          Transport.newDataflowClient(options).build(),
          options);
    }

    /**
     * Package private constructor for testing.
     */
    DataflowWorkUnitClient(Dataflow dataflow, DataflowWorkerHarnessOptions options) {
      this.dataflow = dataflow;
      this.options = options;
    }

    /**
     * Gets a WorkItem from the Dataflow service.
     */
    @Override
    public WorkItem getWorkItem() throws IOException {
      LeaseWorkItemRequest request = new LeaseWorkItemRequest();
      request.setFactory(Transport.getJsonFactory());
      request.setWorkItemTypes(ImmutableList.<String>of(
          "map_task", "seq_map_task", "remote_source_task"));
      // All remote sources require the "remote_source" capability. Dataflow's
      // custom sources are further tagged with the format "custom_source".
      request.setWorkerCapabilities(ImmutableList.<String>of(
          options.getWorkerId(), "remote_source", PropertyNames.CUSTOM_SOURCE_FORMAT));
      request.setWorkerId(options.getWorkerId());
      request.setCurrentWorkerTime(toCloudTime(DateTime.now()));

      // This shouldn't be necessary, but a valid cloud duration string is
      // required by the Google API parsing framework.  TODO: Fix the framework
      // so that an empty or not-present string can be used as a default value.
      request.setRequestedLeaseDuration(toCloudDuration(Duration.standardSeconds(60)));

      LOG.debug("Leasing work: {}", request);

      LeaseWorkItemResponse response = dataflow.v1b3().projects().jobs().workItems().lease(
          options.getProject(), options.getJobId(), request).execute();
      LOG.debug("Lease work response: {}", response);

      List<WorkItem> workItems = response.getWorkItems();
      if (workItems == null || workItems.isEmpty()) {
        // We didn't lease any work
        return null;
      } else if (workItems.size() > 1){
        throw new IOException(
            "This version of the SDK expects no more than one work item from the service: "
            + response);
      }

      WorkItem work = response.getWorkItems().get(0);
      if (work == null || work.getId() == null) {
        return null;
      }

      MDC.put(DataflowWorkerLoggingFormatter.MDC_DATAFLOW_WORK_ID, Long.toString(work.getId()));
      // Looks like the work's a'ight.
      return work;
    }

    @Override
    public WorkItemServiceState reportWorkItemStatus(WorkItemStatus workItemStatus)
        throws IOException {
      workItemStatus.setFactory(Transport.getJsonFactory());
      LOG.debug("Reporting work status: {}", workItemStatus);
      ReportWorkItemStatusResponse result =
          dataflow.v1b3().projects().jobs().workItems().reportStatus(
              options.getProject(), options.getJobId(),
              new ReportWorkItemStatusRequest()
              .setWorkerId(options.getWorkerId())
              .setWorkItemStatuses(Collections.singletonList(workItemStatus))
              .setCurrentWorkerTime(toCloudTime(DateTime.now())))
          .execute();
      if (result == null || result.getWorkItemServiceStates() == null
          || result.getWorkItemServiceStates().size() != 1) {
        throw new IOException(
            "This version of the SDK expects exactly one work item service state from the service");
      }
      WorkItemServiceState state = result.getWorkItemServiceStates().get(0);
      LOG.debug("ReportWorkItemStatus result: {}", state);
      return state;
    }
  }
}
