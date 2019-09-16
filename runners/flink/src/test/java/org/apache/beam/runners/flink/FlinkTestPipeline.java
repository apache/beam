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

/** {@link org.apache.beam.sdk.Pipeline} for testing Beam pipelines on the {@link FlinkRunner}. */
public class FlinkTestPipeline extends Pipeline {

  /**
   * Creates and returns a new test pipeline for batch execution.
   *
   * <p>Use {@link org.apache.beam.sdk.testing.PAssert} to add tests, then call {@link Pipeline#run}
   * to execute the pipeline and check the tests.
   */
  public static FlinkTestPipeline createForBatch() {
    return create(false);
  }

  /**
   * Creates and returns a new test pipeline for streaming execution.
   *
   * <p>Use {@link org.apache.beam.sdk.testing.PAssert} to add tests, then call {@link Pipeline#run}
   * to execute the pipeline and check the tests.
   *
   * @return The Test Pipeline
   */
  public static FlinkTestPipeline createForStreaming() {
    return create(true);
  }

  /**
   * Creates and returns a new test pipeline for streaming or batch execution.
   *
   * <p>Use {@link org.apache.beam.sdk.testing.PAssert} to add tests, then call {@link Pipeline#run}
   * to execute the pipeline and check the tests.
   *
   * @param streaming <code>True</code> for streaming mode, <code>False</code> for batch.
   * @return The Test Pipeline.
   */
  private static FlinkTestPipeline create(boolean streaming) {
    TestFlinkRunner flinkRunner = TestFlinkRunner.create(streaming);
    return new FlinkTestPipeline(flinkRunner.getPipelineOptions());
  }

  private FlinkTestPipeline(PipelineOptions options) {
    super(options);
  }
}
