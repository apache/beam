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

import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.util.construction.resources.PipelineResources;

/**
 * Spark runner {@link PipelineOptions} handles Spark execution-related configurations, such as the
 * master address, and other user-related knobs.
 */
public interface SparkCommonPipelineOptions
    extends PipelineOptions, StreamingOptions, ApplicationNameOptions, FileStagingOptions {

  String DEFAULT_MASTER_URL = "local[4]";

  @Description("The url of the spark master to connect to, (e.g. spark://host:port, local[4]).")
  @Default.String(DEFAULT_MASTER_URL)
  String getSparkMaster();

  void setSparkMaster(String master);

  @Description(
      "A checkpoint directory for streaming resilience, ignored in batch. "
          + "For durability, a reliable filesystem such as HDFS/S3/GS is necessary.")
  @Default.InstanceFactory(TmpCheckpointDirFactory.class)
  String getCheckpointDir();

  void setCheckpointDir(String checkpointDir);

  @Description("Batch default storage level")
  @Default.InstanceFactory(StorageLevelFactory.class)
  String getStorageLevel();

  void setStorageLevel(String storageLevel);

  @Description("Enable/disable sending aggregator values to Spark's metric sinks")
  @Default.Boolean(true)
  Boolean getEnableSparkMetricSinks();

  void setEnableSparkMetricSinks(Boolean enableSparkMetricSinks);

  /**
   * Returns the default checkpoint directory of /tmp/${job.name}. For testing purposes only.
   * Production applications should use a reliable filesystem such as HDFS/S3/GS.
   */
  class TmpCheckpointDirFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return "/tmp/" + options.getJobName();
    }
  }

  /**
   * Classpath contains non jar files (eg. directories with .class files or empty directories) will
   * cause exception in running log. Though the {@link org.apache.spark.SparkContext} can handle
   * this when running in local master, it's better not to include non-jars files in classpath.
   */
  @Internal
  static void prepareFilesToStage(SparkCommonPipelineOptions options) {
    if (!options.getSparkMaster().matches("local\\[?\\d*]?")) {
      PipelineResources.prepareFilesForStaging(options);
    }
  }

  /**
   * Returns Spark's default storage level for the Dataset or RDD API based on the respective
   * runner.
   */
  class StorageLevelFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return SparkStructuredStreamingRunner.class.equals(options.getRunner())
          ? "MEMORY_AND_DISK"
          : "MEMORY_ONLY";
    }
  }
}
