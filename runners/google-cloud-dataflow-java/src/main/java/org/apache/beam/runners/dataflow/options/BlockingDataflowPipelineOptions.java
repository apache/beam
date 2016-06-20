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
package org.apache.beam.runners.dataflow.options;

import org.apache.beam.runners.dataflow.BlockingDataflowRunner;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.PrintStream;

/**
 * Options that are used to configure the {@link BlockingDataflowRunner}.
 */
@Description("Configure options on the BlockingDataflowRunner.")
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
