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
package org.apache.beam.runners.apex;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * Apex {@link PipelineRunner} for testing.
 */
public class TestApexRunner extends PipelineRunner<ApexRunnerResult> {

  private ApexRunner delegate;

  private TestApexRunner(ApexPipelineOptions options) {
    options.setEmbeddedExecution(true);
    //options.setEmbeddedExecutionDebugMode(false);
    options.setRunMillis(20000);
    this.delegate = ApexRunner.fromOptions(options);
  }

  public static TestApexRunner fromOptions(PipelineOptions options) {
    ApexPipelineOptions apexOptions = PipelineOptionsValidator
        .validate(ApexPipelineOptions.class, options);
    return new TestApexRunner(apexOptions);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput>
      OutputT apply(PTransform<InputT, OutputT> transform, InputT input) {
    return delegate.apply(transform, input);
  }

  @Override
  public ApexRunnerResult run(Pipeline pipeline) {
    return delegate.run(pipeline);
  }

}
