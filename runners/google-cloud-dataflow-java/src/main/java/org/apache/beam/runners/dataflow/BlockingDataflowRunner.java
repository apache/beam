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
package org.apache.beam.runners.dataflow;

import org.apache.beam.runners.dataflow.options.BlockingDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A {@link PipelineRunner} that's like {@link DataflowRunner}
 * but that waits for the launched job to finish.
 *
 * <p>Logs job status updates and console messages while it waits.
 *
 * <p>Returns the final job state, or throws an exception if the job
 * fails or cannot be monitored.
 *
 * <p><h3>Permissions</h3>
 * When reading from a Dataflow source or writing to a Dataflow sink using
 * {@code BlockingDataflowRunner}, the Google cloud services account and the Google compute
 * engine service account of the GCP project running the Dataflow Job will need access to the
 * corresponding source/sink.
 *
 * <p>Please see <a href="https://cloud.google.com/dataflow/security-and-permissions">Google Cloud
 * Dataflow Security and Permissions</a> for more details.
 */
public class BlockingDataflowRunner extends
    PipelineRunner<DataflowPipelineJob> {

  private final DataflowRunner dataflowRunner;
  private final BlockingDataflowPipelineOptions options;

  protected BlockingDataflowRunner(
      DataflowRunner internalRunner,
      BlockingDataflowPipelineOptions options) {
    this.dataflowRunner = internalRunner;
    this.options = options;
  }

  /**
   * Constructs a runner from the provided options.
   */
  public static BlockingDataflowRunner fromOptions(
      PipelineOptions options) {
    BlockingDataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(BlockingDataflowPipelineOptions.class, options);
    DataflowRunner dataflowRunner =
        DataflowRunner.fromOptions(dataflowOptions);

    return new BlockingDataflowRunner(dataflowRunner, dataflowOptions);
  }

  /**
   * {@inheritDoc}
   *
   * @throws DataflowJobExecutionException if there is an exception during job execution.
   * @throws DataflowServiceException if there is an exception retrieving information about the job.
   */
  @Override
  public DataflowPipelineJob run(Pipeline p) {
    final DataflowPipelineJob job = dataflowRunner.run(p);
    DataflowRunner.blockOnRun(job, p.getOptions().as(DataflowPipelineOptions.class));
    return job;
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    return dataflowRunner.apply(transform, input);
  }

  /**
   * Sets callbacks to invoke during execution. See {@link DataflowRunnerHooks}.
   */
  @Experimental
  public void setHooks(DataflowRunnerHooks hooks) {
    this.dataflowRunner.setHooks(hooks);
  }

  @Override
  public String toString() {
    return "BlockingDataflowRunner#" + options.getJobName();
  }
}
