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
 * <p> Prints out job status updates and console messages while it waits.
 *
 * <p> Returns the final job state, or throws an exception if the job
 * fails or cannot be monitored.
 */
public class BlockingDataflowPipelineRunner extends
    PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockingDataflowPipelineRunner.class);

  // Defaults to an infinite wait period.
  // TODO: make this configurable after removal of option map.
  private static final long BUILTIN_JOB_TIMEOUT_SEC = -1L;

  private DataflowPipelineRunner dataflowPipelineRunner = null;
  private MonitoringUtil.JobMessagesHandler jobMessagesHandler;

  protected BlockingDataflowPipelineRunner(
      DataflowPipelineRunner internalRunner,
      MonitoringUtil.JobMessagesHandler jobMessagesHandler) {
    this.dataflowPipelineRunner = internalRunner;
    this.jobMessagesHandler = jobMessagesHandler;
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

    return new BlockingDataflowPipelineRunner(dataflowPipelineRunner,
        new MonitoringUtil.PrintHandler(dataflowOptions.getJobMessageOutput()));
  }

  @Override
  public DataflowPipelineJob run(Pipeline p) {
    DataflowPipelineJob job = dataflowPipelineRunner.run(p);

    @Nullable
    State result;
    try {
      result = job.waitToFinish(
          BUILTIN_JOB_TIMEOUT_SEC, TimeUnit.SECONDS, jobMessagesHandler);
    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException("Exception caught during job execution", ex);
    }

    if (result == null) {
      throw new RuntimeException("No result provided: "
          + "possible error requesting job status.");
    }

    LOG.info("Job finished with status {}", result);
    if (result.isTerminal()) {
      return job;
    }

    // TODO: introduce an exception that can wrap a JobState,
    // so that detailed error information can be retrieved.
    throw new RuntimeException(
        "Failed to wait for the job to finish. Returned result: " + result);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    return dataflowPipelineRunner.apply(transform, input);
  }

  /**
   * Sets callbacks to invoke during execution see {@code DataflowPipelineRunnerHooks}.
   */
  @Experimental
  public void setHooks(DataflowPipelineRunnerHooks hooks) {
    this.dataflowPipelineRunner.setHooks(hooks);
  }
}
