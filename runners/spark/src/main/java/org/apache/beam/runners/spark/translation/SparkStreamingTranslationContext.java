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
package org.apache.beam.runners.spark.translation;

import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.Instant;

/**
 * Translation context used to lazily store Spark datasets during streaming portable pipeline
 * translation and compute them after translation.
 */
public class SparkStreamingTranslationContext extends SparkTranslationContext {
  private final JavaStreamingContext streamingContext;
  private final Instant firstTimestamp;

  public SparkStreamingTranslationContext(
      JavaSparkContext jsc, SparkPipelineOptions options, JobInfo jobInfo) {
    super(jsc, options, jobInfo);
    Duration batchDuration = new Duration(options.getBatchIntervalMillis());
    this.streamingContext = new JavaStreamingContext(jsc, batchDuration);
    this.firstTimestamp = new Instant();
  }

  public JavaStreamingContext getStreamingContext() {
    return streamingContext;
  }

  public Instant getFirstTimestamp() {
    return firstTimestamp;
  }
}
