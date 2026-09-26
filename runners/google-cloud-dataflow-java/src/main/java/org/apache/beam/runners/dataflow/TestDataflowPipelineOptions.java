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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A set of options used to configure the {@link TestPipeline}. */
public interface TestDataflowPipelineOptions extends TestPipelineOptions, DataflowPipelineOptions {

  @Description(
      "If true, concurrent batch TestPipeline runs in the same JVM will be merged into a single"
          + " Dataflow job per batch. If null, defaults to the beam.dataflow.testBatching system"
          + " property.")
  @Nullable Boolean getEnableTestBatching();

  void setEnableTestBatching(@Nullable Boolean value);

  @Description("Maximum number of test pipelines to merge into a single Dataflow job.")
  @Default.Integer(0)
  int getTestBatchMaxSize();

  void setTestBatchMaxSize(int value);

  @Description(
      "Time window in milliseconds to wait for additional concurrent test pipelines before"
          + " launching a merged Dataflow job.")
  @Default.Long(0L)
  long getTestBatchWindowMs();

  void setTestBatchWindowMs(long value);
}
