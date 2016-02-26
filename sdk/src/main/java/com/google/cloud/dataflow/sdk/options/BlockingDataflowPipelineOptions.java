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

package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.PrintStream;

/**
 * Options that are used to configure the {@link BlockingDataflowPipelineRunner}.
 */
@Description("Configure options on the BlockingDataflowPipelineRunner.")
public interface BlockingDataflowPipelineOptions extends DataflowPipelineOptions {
  /**
   * Output stream for job status messages.
   */
  @Description("Where messages generated during execution of the Dataflow job will be output.")
  @JsonIgnore
  @Hidden
  @Default.InstanceFactory(StandardOutputFactory.class)
  PrintStream getJobMessageOutput();
  void setJobMessageOutput(PrintStream value);

  /**
   * Returns a default of {@link System#out}.
   */
  public static class StandardOutputFactory implements DefaultValueFactory<PrintStream> {
    @Override
    public PrintStream create(PipelineOptions options) {
      return System.out;
    }
  }
}
