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
package org.apache.beam.runners.spark;

import org.apache.beam.runners.core.construction.PipelineResources;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;

/**
 * Spark runner {@link PipelineOptions} handles Spark execution-related configurations, such as the
 * master address, batch-interval, and other user-related knobs.
 */
public interface SparkPipelineOptions extends SparkCommonPipelineOptions {

  @Description("Batch interval for Spark streaming in milliseconds.")
  @Default.Long(500)
  Long getBatchIntervalMillis();

  void setBatchIntervalMillis(Long batchInterval);

  @Description("Batch default storage level")
  @Default.String("MEMORY_ONLY")
  String getStorageLevel();

  void setStorageLevel(String storageLevel);

  @Description("Minimum time to spend on read, for each micro-batch.")
  @Default.Long(200)
  Long getMinReadTimeMillis();

  void setMinReadTimeMillis(Long minReadTimeMillis);

  @Description("Max records per micro-batch. For streaming sources only.")
  @Default.Long(-1)
  Long getMaxRecordsPerBatch();

  void setMaxRecordsPerBatch(Long maxRecordsPerBatch);

  @Description(
      "A value between 0-1 to describe the percentage of a micro-batch dedicated "
          + "to reading from UnboundedSource.")
  @Default.Double(0.1)
  Double getReadTimePercentage();

  void setReadTimePercentage(Double readTimePercentage);

  @Description(
      "The period to checkpoint (in Millis). If not set, Spark will default "
          + "to Max(slideDuration, Seconds(10)). This PipelineOptions default (-1) will end-up "
          + "with the described Spark default.")
  @Default.Long(-1)
  Long getCheckpointDurationMillis();

  void setCheckpointDurationMillis(Long durationMillis);

  @Description(
      "If set bundleSize will be used for splitting BoundedSources, otherwise default to "
          + "splitting BoundedSources on Spark defaultParallelism. Most effective when used with "
          + "Spark dynamicAllocation.")
  @Default.Long(0)
  Long getBundleSize();

  @Experimental
  void setBundleSize(Long value);

  @Description("Enable/disable sending aggregator values to Spark's metric sinks")
  @Default.Boolean(true)
  Boolean getEnableSparkMetricSinks();

  void setEnableSparkMetricSinks(Boolean enableSparkMetricSinks);

  @Description(
      "If the spark runner will be initialized with a provided Spark Context. "
          + "The Spark Context should be provided with SparkContextOptions.")
  @Default.Boolean(false)
  boolean getUsesProvidedSparkContext();

  void setUsesProvidedSparkContext(boolean value);

  @Description(
      "Disable caching of reused PCollections for whole Pipeline."
          + " It's useful when it's faster to recompute RDD rather than save. ")
  @Default.Boolean(false)
  boolean isCacheDisabled();

  void setCacheDisabled(boolean value);

  /**
   * Local configurations work in the same JVM and have no problems with improperly formatted files
   * on classpath (eg. directories with .class files or empty directories). Prepare files for
   * staging only when using remote cluster (passing the master address explicitly).
   */
  static void prepareFilesToStageForRemoteClusterExecution(SparkPipelineOptions options) {
    if (!options.getSparkMaster().matches("local\\[?\\d*\\]?")) {
      options.setFilesToStage(
          PipelineResources.prepareFilesForStaging(
              options.getFilesToStage(),
              MoreObjects.firstNonNull(
                  options.getTempLocation(), System.getProperty("java.io.tmpdir"))));
    }
  }
}
