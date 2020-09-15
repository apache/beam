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
package org.apache.beam.runners.twister2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by first translating them
 * to a Twister2 Plan and then executing them either locally or on a Twister2 cluster, depending on
 * the configuration. This is used for testing the Twister2 runner
 */
public class Twister2TestRunner extends PipelineRunner<PipelineResult> {

  private Twister2Runner delegate;

  public Twister2TestRunner(Twister2PipelineOptions options) {
    options.setRunner(Twister2TestRunner.class);
    options.setParallelism(1);
    this.delegate = Twister2Runner.fromOptions(options);
  }

  public static Twister2TestRunner fromOptions(PipelineOptions options) {
    Twister2PipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(Twister2PipelineOptions.class, options);
    return new Twister2TestRunner(pipelineOptions);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    try {
      return delegate.runTest(pipeline);
    } catch (IllegalArgumentException ex) {
      throw ex;
    } catch (Throwable t) {
      RuntimeException exception =
          new RuntimeException(t.getCause().getMessage(), t.getCause().getCause());
      exception.setStackTrace(t.getCause().getStackTrace());
      throw exception;
    }
  }
}
