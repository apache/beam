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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import org.apache.flink.runtime.client.JobExecutionException;

public class TestFlinkPipelineRunner extends PipelineRunner<FlinkRunnerResult> {

  private FlinkPipelineRunner delegate;

  private TestFlinkPipelineRunner(FlinkPipelineOptions options) {
    // We use [auto] for testing since this will make it pick up the Testing ExecutionEnvironment
    options.setFlinkMaster("[auto]");
    this.delegate = FlinkPipelineRunner.fromOptions(options);
  }

  public static TestFlinkPipelineRunner fromOptions(PipelineOptions options) {
    FlinkPipelineOptions flinkOptions = PipelineOptionsValidator.validate(FlinkPipelineOptions.class, options);
    return new TestFlinkPipelineRunner(flinkOptions);
  }

  public static TestFlinkPipelineRunner create(boolean streaming) {
    FlinkPipelineOptions flinkOptions = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    flinkOptions.setRunner(TestFlinkPipelineRunner.class);
    flinkOptions.setStreaming(streaming);
    return TestFlinkPipelineRunner.fromOptions(flinkOptions);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput>
      OutputT apply(PTransform<InputT,OutputT> transform, InputT input) {
    return delegate.apply(transform, input);
  }

  @Override
  public FlinkRunnerResult run(Pipeline pipeline) {
    try {
      return delegate.run(pipeline);
    } catch (RuntimeException e) {
      // Special case hack to pull out assertion errors from PAssert; instead there should
      // probably be a better story along the lines of UserCodeException.
      if (e.getCause() != null
          && e.getCause() instanceof JobExecutionException
          && e.getCause().getCause() instanceof AssertionError) {
          throw (AssertionError) e.getCause().getCause();
      } else {
        throw e;
      }
    }
  }

  public PipelineOptions getPipelineOptions() {
    return delegate.getPipelineOptions();
  }
}


