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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.fn.JvmInitializers;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the batch harness for executing Dataflow jobs where the worker and user/Java SDK code are
 * running together in the same process.
 */
public class DataflowBatchWorkerHarness {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowBatchWorkerHarness.class);
  private final DataflowWorkerHarnessOptions pipelineOptions;

  private DataflowBatchWorkerHarness(DataflowWorkerHarnessOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    checkArgument(
        !pipelineOptions.isStreaming(),
        "%s instantiated with pipeline options indicating a streaming job",
        getClass().getSimpleName());
  }

  /** Creates the worker harness and then runs it. */
  public static void main(String[] args) throws Exception {
    // Call user-defined initialization immediately upon starting, as is guaranteed in
    // JvmInitializer
    JvmInitializers.runOnStartup();
    DataflowWorkerHarnessHelper.initializeLogging(DataflowBatchWorkerHarness.class);
    DataflowWorkerHarnessOptions pipelineOptions =
        DataflowWorkerHarnessHelper.initializeGlobalStateAndPipelineOptions(
            DataflowBatchWorkerHarness.class, DataflowWorkerHarnessOptions.class);
    DataflowBatchWorkerHarness batchHarness = new DataflowBatchWorkerHarness(pipelineOptions);
    DataflowWorkerHarnessHelper.configureLogging(pipelineOptions);

    checkArgument(
        !DataflowRunner.hasExperiment(pipelineOptions, "beam_fn_api"),
        "%s cannot be main() class with beam_fn_api enabled",
        DataflowBatchWorkerHarness.class.getSimpleName());

    CoderTranslation.verifyModelCodersRegistered();

    JvmInitializers.runBeforeProcessing(pipelineOptions);
    batchHarness.run();
  }

  public static DataflowBatchWorkerHarness from(DataflowWorkerHarnessOptions pipelineOptions) {
    return new DataflowBatchWorkerHarness(pipelineOptions);
  }

  /** Initializes the worker and starts the actual read loop (in {@link #processWork}). */
  public void run() throws InterruptedException {
    // Configure standard file systems.
    FileSystems.setDefaultPipelineOptions(pipelineOptions);

    DataflowWorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);
    BatchDataflowWorker worker =
        BatchDataflowWorker.forBatchIntrinsicWorkerHarness(client, pipelineOptions);

    worker.startStatusServer();
    processWork(pipelineOptions, worker, Sleeper.DEFAULT);
  }

  // ExponentialBackOff parameters for the task retry strategy.
  private static final long BACKOFF_INITIAL_INTERVAL_MILLIS = 5000; // 5 second
  @VisibleForTesting static final long BACKOFF_MAX_INTERVAL_MILLIS = 5 * 60 * 1000; // 5 min.

  /** Helper for initializing the BackOff used for retries. */
  private static BackOff createBackOff() {
    return FluentBackoff.DEFAULT
        .withInitialBackoff(Duration.millis(BACKOFF_INITIAL_INTERVAL_MILLIS))
        .withMaxBackoff(Duration.millis(BACKOFF_MAX_INTERVAL_MILLIS))
        .backoff();
  }

  private static int chooseNumberOfThreads(DataflowWorkerHarnessOptions pipelineOptions) {
    if (pipelineOptions.getNumberOfWorkerHarnessThreads() != 0) {
      return pipelineOptions.getNumberOfWorkerHarnessThreads();
    }
    return Math.max(Runtime.getRuntime().availableProcessors(), 1);
  }

  /** A thread that repeatedly fetches and processes work units from the Dataflow service. */
  private static class WorkerThread implements Callable<Boolean> {
    // sleeper is used to sleep the appropriate amount of time
    WorkerThread(final BatchDataflowWorker worker, final Sleeper sleeper) {
      this.worker = worker;
      this.sleeper = sleeper;
      this.backOff = createBackOff();
    }

    @Override
    public Boolean call() {
      boolean success = true;
      try {
        do { // We loop getting and processing work.
          success = doWork();
          if (success) {
            backOff.reset();
          }
          // Sleeping a while if there is a problem with the work, then go on with the next work.
        } while (success || BackOffUtils.next(sleeper, backOff));
      } catch (IOException e) { // Failure of BackOff.
        LOG.error("Already tried several attempts at working on tasks. Aborting.", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Interrupted during thread execution or sleep.", e);
      } catch (Throwable t) {
        LOG.error("Thread {} died.", Thread.currentThread().getId(), t);
      }
      return false;
    }

    private boolean doWork() {
      try {
        LOG.debug("Thread starting getAndPerformWork.");
        boolean success = worker.getAndPerformWork();
        LOG.debug("{} processing one WorkItem.", success ? "Finished" : "Failed");
        return success;
      } catch (IOException e) { // If there is a problem getting work.
        LOG.info("There was a problem getting work.", e);
        return false;
      } catch (Exception e) { // These exceptions are caused by bugs within the SDK
        LOG.error("There was an unhandled error caused by the Dataflow SDK.", e);
        return false;
      }
    }

    private final BatchDataflowWorker worker;
    private final Sleeper sleeper;
    private final BackOff backOff;
  }

  @VisibleForTesting
  static void processWork(
      DataflowWorkerHarnessOptions pipelineOptions,
      final BatchDataflowWorker worker,
      Sleeper sleeper)
      throws InterruptedException {
    int numThreads = chooseNumberOfThreads(pipelineOptions);
    ExecutorService executor = pipelineOptions.getExecutorService();
    final List<Callable<Boolean>> tasks = new ArrayList<>();

    LOG.debug("Starting {} worker threads", numThreads);
    // We start the appropriate number of threads.
    for (int i = 0; i < numThreads; ++i) {
      tasks.add(new WorkerThread(worker, sleeper));
    }

    LOG.debug("Waiting for {} worker threads", numThreads);
    // We wait forever unless there is a big problem.
    executor.invokeAll(tasks);
    LOG.error("All threads died.");
  }
}
