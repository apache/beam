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
package org.apache.beam.sdk.runners;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A {@link DataflowPipeline} is a {@link Pipeline} that returns a
 * {@link DataflowPipelineJob} when it is
 * {@link org.apache.beam.sdk.Pipeline#run()}.
 *
 * <p>This is not intended for use by users of Cloud Dataflow.
 * Instead, use {@link Pipeline#create(PipelineOptions)} to initialize a
 * {@link Pipeline}.
 */
public class DataflowPipeline extends Pipeline {

  /**
   * Creates and returns a new {@link DataflowPipeline} instance for tests.
   */
  public static DataflowPipeline create(DataflowPipelineOptions options) {
    return new DataflowPipeline(options);
  }

  private DataflowPipeline(DataflowPipelineOptions options) {
    super(DataflowPipelineRunner.fromOptions(options), options);
  }

  @Override
  public DataflowPipelineJob run() {
    return (DataflowPipelineJob) super.run();
  }

  @Override
  public DataflowPipelineRunner getRunner() {
    return (DataflowPipelineRunner) super.getRunner();
  }

  @Override
  public String toString() {
    return "DataflowPipeline#" + getOptions().as(DataflowPipelineOptions.class).getJobName();
  }
}
