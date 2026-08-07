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
package org.apache.beam.runners.spark.structuredstreaming;

import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Spark runner {@link PipelineOptions} handles Spark execution-related configurations, such as the
 * master address, and other user-related knobs.
 */
public interface SparkStructuredStreamingPipelineOptions extends SparkCommonPipelineOptions {

  /** Set to true to run the job in test mode. */
  @Default.Boolean(false)
  boolean getTestMode();

  void setTestMode(boolean testMode);

  @Description("Enable if the runner should use the currently active Spark session.")
  @Default.Boolean(false)
  boolean getUseActiveSparkSession();

  void setUseActiveSparkSession(boolean value);

  @Description(
      "Watermark delay in milliseconds applied to event timestamps of streaming sources "
          + "(streaming mode only).")
  @Default.Long(0)
  long getWatermarkDelayMillis();

  void setWatermarkDelayMillis(long value);

  // Note: deliberately NOT named getMaxRecordsPerBatch. The legacy Spark runner's
  // SparkPipelineOptions already declares Long getMaxRecordsPerBatch(); a same-name getter with a
  // different return type breaks proxy generation for every registered PipelineOptions interface.
  @Description(
      "Maximum number of records to read per micro-batch from a streaming source "
          + "(streaming mode only).")
  @Default.Integer(1000)
  int getMaxRecordsPerMicroBatch();

  void setMaxRecordsPerMicroBatch(int value);

  @Description(
      "Maximum duration in milliseconds of a micro-batch trigger interval (streaming mode only).")
  @Default.Long(500)
  long getMaxBatchDurationMillis();

  void setMaxBatchDurationMillis(long value);

  @Description(
      "Test-oriented: gracefully stop streaming queries after this many consecutive empty "
          + "micro-batches. Disabled if negative (streaming mode only).")
  @Default.Integer(-1)
  int getStreamingStopAfterIdleBatches();

  void setStreamingStopAfterIdleBatches(int value);
}
