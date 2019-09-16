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

import com.datatorrent.api.DAG;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.joda.time.Duration;

/** Apex {@link PipelineRunner} for testing. */
public class TestApexRunner extends PipelineRunner<ApexRunnerResult> {

  private static final int RUN_WAIT_MILLIS = 20000;
  private final ApexRunner delegate;

  private TestApexRunner(ApexPipelineOptions options) {
    options.setEmbeddedExecution(true);
    // options.setEmbeddedExecutionDebugMode(false);
    this.delegate = ApexRunner.fromOptions(options);
  }

  public static TestApexRunner fromOptions(PipelineOptions options) {
    ApexPipelineOptions apexOptions =
        PipelineOptionsValidator.validate(ApexPipelineOptions.class, options);
    return new TestApexRunner(apexOptions);
  }

  public static DAG translate(Pipeline pipeline, ApexPipelineOptions options) {
    ApexRunner delegate = new ApexRunner(options);
    delegate.translateOnly = true;
    return delegate.run(pipeline).getApexDAG();
  }

  @Override
  @SuppressWarnings("Finally")
  public ApexRunnerResult run(Pipeline pipeline) {
    ApexRunnerResult result = delegate.run(pipeline);
    try {
      // this is necessary for tests that just call run() and not waitUntilFinish
      result.waitUntilFinish(Duration.millis(RUN_WAIT_MILLIS));
      return result;
    } finally {
      try {
        result.cancel();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
