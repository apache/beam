/*
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
 */

package com.google.cloud.dataflow.sdk.runners;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.options.BlockingDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * A {@link PipelineRunner} that's like {@link DataflowPipelineRunner}
 * but that waits for the launched job to finish.
 *
 * <p>Prints out job status updates and console messages while it waits.
 *
 * <p>Returns the final job state, or throws an exception if the job
 * fails or cannot be monitored.
 *
 * <p><h3>Permissions</h3>
 * When reading from a Dataflow source or writing to a Dataflow sink using
 * {@code BlockingDataflowPipelineRunner}, the Google cloud services account and the Google compute
 * engine service account of the GCP project running the Dataflow Job will need access to the
 * corresponding source/sink.
 *
 * <p>Please see <a href="https://cloud.google.com/dataflow/security-and-permissions">Google Cloud
 * Dataflow Security and Permissions</a> for more details.
 */
public class BlockingDataflowPipelineRunner extends
    PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockingDataflowPipelineRunner.class);

  // Defaults to an infinite wait period.
  // TODO: make this configurable after removal of option map.
  private static final long BUILTIN_JOB_TIMEOUT_SEC = -1L;

  private final DataflowPipelineRunner dataflowPipelineRunner;
  private final BlockingDataflowPipelineOptions options;

  protected BlockingDataflowPipelineRunner(
      DataflowPipelineRunner internalRunner,
      BlockingDataflowPipelineOptions options) {
    this.dataflowPipelineRunner = internalRunner;
    this.options = options;
  }

  /**
   * Constructs a runner from the provided options.
   */
  public static BlockingDataflowPipelineRunner fromOptions(
      PipelineOptions options) {
    BlockingDataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(BlockingDataflowPipelineOptions.class, options);
    DataflowPipelineRunner dataflowPipelineRunner =
        DataflowPipelineRunner.fromOptions(dataflowOptions);

    return new BlockingDataflowPipelineRunner(dataflowPipelineRunner, dataflowOptions);
  }

  /**
   * {@inheritDoc}
   *
   * @throws DataflowJobExecutionException if there is an exception during job execution.
   * @throws DataflowServiceException if there is an exception retrieving information about the job.
   */
  @Override
  public DataflowPipelineJob run(Pipeline p) {
    final DataflowPipelineJob job = dataflowPipelineRunner.run(p);

    // We ignore the potential race condition here (Ctrl-C after job submission but before the
    // shutdown hook is registered). Even if we tried to do something smarter (eg., SettableFuture)
    // the run method (which produces the job) could fail or be Ctrl-C'd before it had returned a
    // job. The display of the command to cancel the job is best-effort anyways -- RPC's could fail,
    // etc. If the user wants to verify the job was cancelled they should look at the job status.
    Thread shutdownHook = new Thread() {
      @Override
      public void run() {
        LOG.warn("Job is already running in Google Cloud Platform, Ctrl-C will not cancel it.\n"
            + "To cancel the job in the cloud, run:\n> {}",
            MonitoringUtil.getGcloudCancelCommand(options, job.getJobId()));
      }
    };

    try {
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      @Nullable
      State result;
      try {
        result = job.waitToFinish(
            BUILTIN_JOB_TIMEOUT_SEC, TimeUnit.SECONDS,
            new MonitoringUtil.PrintHandler(options.getJobMessageOutput()));
      } catch (IOException | InterruptedException ex) {
        LOG.debug("Exception caught while retrieving status for job {}", job.getJobId(), ex);
        throw new DataflowServiceException(
            job, "Exception caught while retrieving status for job " + job.getJobId(), ex);
      }

      if (result == null) {
        throw new DataflowServiceException(
            job, "Timed out while retrieving status for job " + job.getJobId());
      }

      LOG.info("Job finished with status {}", result);
      if (!result.isTerminal()) {
        throw new IllegalStateException("Expected terminal state for job " + job.getJobId()
            + ", got " + result);
      }

      if (result == State.DONE) {
        return job;
      } else if (result == State.UPDATED) {
        DataflowPipelineJob newJob = job.getReplacedByJob();
        LOG.info("Job {} has been updated and is running as the new job with id {}."
            + "To access the updated job on the Dataflow monitoring console, please navigate to {}",
            job.getJobId(),
            newJob.getJobId(),
            MonitoringUtil.getJobMonitoringPageURL(newJob.getProjectId(), newJob.getJobId()));
        throw new DataflowJobUpdatedException(
            job,
            String.format("Job %s updated; new job is %s.", job.getJobId(), newJob.getJobId()),
            newJob);
      } else if (result == State.CANCELLED) {
        String message = String.format("Job %s cancelled by user", job.getJobId());
        LOG.info(message);
        throw new DataflowJobCancelledException(job, message);
      } else {
        throw new DataflowJobExecutionException(job, "Job " + job.getJobId()
            + " failed with status " + result);
      }
    } finally {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    return dataflowPipelineRunner.apply(transform, input);
  }

  /**
   * Sets callbacks to invoke during execution. See {@link DataflowPipelineRunnerHooks}.
   */
  @Experimental
  public void setHooks(DataflowPipelineRunnerHooks hooks) {
    this.dataflowPipelineRunner.setHooks(hooks);
  }

  @Override
  public String toString() {
    return "BlockingDataflowPipelineRunner#" + options.getJobName();
  }
}
