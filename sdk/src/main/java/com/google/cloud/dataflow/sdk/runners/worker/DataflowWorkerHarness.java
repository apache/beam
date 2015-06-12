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

import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudTime;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
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
import com.google.cloud.dataflow.sdk.util.AttemptBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.GcsIOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkProgressUpdater;
import com.google.common.collect.ImmutableList;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

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
 * During initialization, we should take a configuration that specifies
 * an initialization function, allowing user code to run on VM startup.
 */
public class DataflowWorkerHarness {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkerHarness.class);

  private static final String APPLICATION_NAME = "DataflowWorkerHarness";

  // ExponentialBackOff parameters for the task retry strategy. Visible for testing.
  static final int BACKOFF_INITIAL_INTERVAL_MILLIS = 5000;  // 5 second
  static final int BACKOFF_MAX_ATTEMPTS = 10;  // 10 attempts will take approx. 15 min.

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
   * Helper for initializing the BackOff used for retries.
   */
  private static BackOff createBackOff() {
    return new AttemptBoundedExponentialBackOff(
            BACKOFF_MAX_ATTEMPTS, BACKOFF_INITIAL_INTERVAL_MILLIS);
  }

  /**
   * Fetches and processes work units from the Dataflow service.
   */
  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(WorkerUncaughtExceptionHandler.INSTANCE);
    DataflowWorkerLoggingInitializer.initialize();

    DataflowWorkerHarnessOptions pipelineOptions =
        PipelineOptionsFactory.createFromSystemPropertiesInternal();
    DataflowWorkerLoggingInitializer.configure(pipelineOptions);

    final Sleeper sleeper = Sleeper.DEFAULT;
    final DataflowWorker worker = create(pipelineOptions);
    processWork(pipelineOptions, worker, sleeper);
  }

  /**
   * A thread that repeatedly fetches and processes work units from the Dataflow service.
   */
  private static class WorkerThread implements Callable<Boolean> {
    // sleeper is used to sleep the appropriate amount of time
    WorkerThread(final DataflowWorker worker, final Sleeper sleeper) {
      this.worker = worker;
      this.sleeper = sleeper;
      this.backOff = createBackOff();
    }

    @Override
    public Boolean call() {
      boolean success = true;
      try {
        do { // We loop getting and processing work.
          try {
            LOG.debug("Thread starting getAndPerformWork.");
            success = worker.getAndPerformWork();
            LOG.debug("{} processing one WorkItem.", success ? "Finished" : "Failed");
          } catch (IOException e) {  // If there is a problem getting work.
            success = false;
          }
          if (success) {
            backOff.reset();
          }
          // Sleeping a while if there is a problem with the work, then go on with the next work.
        } while (success || BackOffUtils.next(sleeper, backOff));
      } catch (IOException e) {  // Failure of BackOff.
        LOG.error("Already tried several attempts at working on tasks. Aborting.", e);
      } catch (InterruptedException e) {
        LOG.error("Interrupted during thread execution or sleep.", e);
      }
      return false;
    }

    private final DataflowWorker worker;
    private final Sleeper sleeper;
    private final BackOff backOff;
  }

  // Visible for testing.
  static void processWork(DataflowWorkerHarnessOptions pipelineOptions,
      final DataflowWorker worker, Sleeper sleeper) throws InterruptedException {
    int numThreads = Math.max(Runtime.getRuntime().availableProcessors(), 1);
    ExecutorService executor = pipelineOptions.getExecutorService();
    final List<Callable<Boolean>> tasks = new LinkedList<>();

    LOG.debug("Starting {} worker threads", numThreads);
    // We start the appropriate number of threads.
    for (int i = 0; i < numThreads; ++i) {
      tasks.add(new WorkerThread(worker, sleeper));
    }

    LOG.debug("Waiting for {} worker threads", numThreads);
    // We wait forever unless there is a big problem.
    executor.invokeAll(tasks);
  }

  static DataflowWorker create(DataflowWorkerHarnessOptions options) {
    DataflowWorkerLoggingFormatter.setJobId(options.getJobId());
    DataflowWorkerLoggingFormatter.setWorkerId(options.getWorkerId());
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
      request.setRequestedLeaseDuration(
          toCloudDuration(Duration.millis(WorkProgressUpdater.DEFAULT_LEASE_DURATION_MILLIS)));

      LOG.debug("Leasing work: {}", request);

      LeaseWorkItemResponse response = dataflow.projects().jobs().workItems().lease(
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

      DataflowWorkerLoggingFormatter.setWorkId(Long.toString(work.getId()));
      // Looks like the work's a'ight.
      return work;
    }

    @Override
    public WorkItemServiceState reportWorkItemStatus(WorkItemStatus workItemStatus)
        throws IOException {
      workItemStatus.setFactory(Transport.getJsonFactory());
      LOG.debug("Reporting work status: {}", workItemStatus);
      ReportWorkItemStatusResponse result =
          dataflow.projects().jobs().workItems().reportStatus(
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
