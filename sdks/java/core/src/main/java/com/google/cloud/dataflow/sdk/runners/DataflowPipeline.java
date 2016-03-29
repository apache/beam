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
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * A {@link DataflowPipeline} is a {@link Pipeline} that returns a
 * {@link DataflowPipelineJob} when it is
 * {@link com.google.cloud.dataflow.sdk.Pipeline#run()}.
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
