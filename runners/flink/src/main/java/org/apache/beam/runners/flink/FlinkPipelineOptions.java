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

import java.util.List;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

/**
 * Options which can be used to configure a Flink PortablePipelineRunner.
 *
 * <p>Avoid using `org.apache.flink.*` members below. This allows including the flink runner without
 * requiring flink on the classpath (e.g. to use with the direct runner).
 */
public interface FlinkPipelineOptions
    extends PipelineOptions, ApplicationNameOptions, StreamingOptions {

  String AUTO = "[auto]";
  String PIPELINED = "PIPELINED";
  String EXACTLY_ONCE = "EXACTLY_ONCE";

  /**
   * List of local files to make available to workers.
   *
   * <p>Jars are placed on the worker's classpath.
   *
   * <p>The default value is the list of jars from the main program's classpath.
   */
  @Description(
      "Jar-Files to send to all workers and put on the classpath. "
          + "The default value is all files from the classpath.")
  List<String> getFilesToStage();

  void setFilesToStage(List<String> value);

  /**
   * The url of the Flink JobManager on which to execute pipelines. This can either be the the
   * address of a cluster JobManager, in the form "host:port" or one of the special Strings
   * "[local]", "[collection]" or "[auto]". "[local]" will start a local Flink Cluster in the JVM,
   * "[collection]" will execute the pipeline on Java Collections while "[auto]" will let the system
   * decide where to execute the pipeline based on the environment.
   */
  @Description(
      "Address of the Flink Master where the Pipeline should be executed. Can"
          + " either be of the form \"host:port\" or one of the special values [local], "
          + "[collection] or [auto].")
  @Default.String(AUTO)
  String getFlinkMaster();

  void setFlinkMaster(String value);

  @Description(
      "The degree of parallelism to be used when distributing operations onto workers. "
          + "If the parallelism is not set, the configured Flink default is used, or 1 if none can be found.")
  @Default.Integer(-1)
  Integer getParallelism();

  void setParallelism(Integer value);

  @Description(
      "The pipeline wide maximum degree of parallelism to be used. The maximum parallelism specifies the upper limit "
          + "for dynamic scaling and the number of key groups used for partitioned state.")
  @Default.Integer(-1)
  Integer getMaxParallelism();

  void setMaxParallelism(Integer value);

  @Description(
      "The interval in milliseconds at which to trigger checkpoints of the running pipeline. "
          + "Default: No checkpointing.")
  @Default.Long(-1L)
  Long getCheckpointingInterval();

  void setCheckpointingInterval(Long interval);

  @Description("The checkpointing mode that defines consistency guarantee.")
  @Default.String(EXACTLY_ONCE)
  String getCheckpointingMode();

  void setCheckpointingMode(String mode);

  @Description(
      "The maximum time in milliseconds that a checkpoint may take before being discarded.")
  @Default.Long(-1L)
  Long getCheckpointTimeoutMillis();

  void setCheckpointTimeoutMillis(Long checkpointTimeoutMillis);

  @Description("The minimal pause in milliseconds before the next checkpoint is triggered.")
  @Default.Long(-1L)
  Long getMinPauseBetweenCheckpoints();

  void setMinPauseBetweenCheckpoints(Long minPauseInterval);

  @Description(
      "Sets the expected behaviour for tasks in case that they encounter an error in their "
          + "checkpointing procedure. If this is set to true, the task will fail on checkpointing error. "
          + "If this is set to false, the task will only decline a the checkpoint and continue running. ")
  @Default.Boolean(true)
  Boolean getFailOnCheckpointingErrors();

  void setFailOnCheckpointingErrors(Boolean failOnCheckpointingErrors);

  @Description(
      "Sets the number of times that failed tasks are re-executed. "
          + "A value of zero effectively disables fault tolerance. A value of -1 indicates "
          + "that the system default value (as defined in the configuration) should be used.")
  @Default.Integer(-1)
  Integer getNumberOfExecutionRetries();

  void setNumberOfExecutionRetries(Integer retries);

  @Description(
      "Sets the delay in milliseconds between executions. A value of {@code -1} "
          + "indicates that the default value should be used.")
  @Default.Long(-1L)
  Long getExecutionRetryDelay();

  void setExecutionRetryDelay(Long delay);

  @Description("Sets the behavior of reusing objects.")
  @Default.Boolean(false)
  Boolean getObjectReuse();

  void setObjectReuse(Boolean reuse);

  /**
   * State backend to store Beam's state during computation. Note: Only applicable when executing in
   * streaming mode.
   */
  @Description(
      "Sets the state backend factory to use in streaming mode. "
          + "Defaults to the flink cluster's state.backend configuration.")
  Class<? extends FlinkStateBackendFactory> getStateBackendFactory();

  void setStateBackendFactory(Class<? extends FlinkStateBackendFactory> stateBackendFactory);

  @Description("Disable Beam metrics in Flink Runner")
  @Default.Boolean(false)
  Boolean getDisableMetrics();

  void setDisableMetrics(Boolean enableMetrics);

  @Description(
      "By default, uses Flink accumulators to store the metrics which allows to query metrics from the PipelineResult. "
          + "If set to true, metrics will still be reported but can't be queried via PipelineResult. "
          + "This saves network and memory.")
  @Default.Boolean(false)
  Boolean getDisableMetricAccumulator();

  void setDisableMetricAccumulator(Boolean disableMetricAccumulator);

  /** Enables or disables externalized checkpoints. */
  @Description(
      "Enables or disables externalized checkpoints. "
          + "Works in conjunction with CheckpointingInterval")
  @Default.Boolean(false)
  Boolean isExternalizedCheckpointsEnabled();

  void setExternalizedCheckpointsEnabled(Boolean externalCheckpoints);

  @Description("Sets the behavior of externalized checkpoints on cancellation.")
  @Default.Boolean(false)
  Boolean getRetainExternalizedCheckpointsOnCancellation();

  void setRetainExternalizedCheckpointsOnCancellation(Boolean retainOnCancellation);

  @Description("The maximum number of elements in a bundle.")
  @Default.Long(1000)
  Long getMaxBundleSize();

  void setMaxBundleSize(Long size);

  @Description("The maximum time to wait before finalising a bundle (in milliseconds).")
  @Default.Long(1000)
  Long getMaxBundleTimeMills();

  void setMaxBundleTimeMills(Long time);

  /**
   * Whether to shutdown sources when their watermark reaches {@code +Inf}. For production use cases
   * you want this to be disabled because Flink will currently (versions {@literal <=} 1.5) stop
   * doing checkpoints when any operator (which includes sources) is finished.
   *
   * <p>Please see <a href="https://issues.apache.org/jira/browse/FLINK-2491">FLINK-2491</a> for
   * progress on this issue.
   */
  @Description("If set, shutdown sources when their watermark reaches +Inf.")
  @Default.Boolean(false)
  Boolean isShutdownSourcesOnFinalWatermark();

  void setShutdownSourcesOnFinalWatermark(Boolean shutdownOnFinalWatermark);

  @Description(
      "Interval in milliseconds for sending latency tracking marks from the sources to the sinks. "
          + "Interval value <= 0 disables the feature.")
  @Default.Long(0)
  Long getLatencyTrackingInterval();

  void setLatencyTrackingInterval(Long interval);

  @Description("The interval in milliseconds for automatic watermark emission.")
  Long getAutoWatermarkInterval();

  void setAutoWatermarkInterval(Long interval);

  @Description(
      "Flink mode for data exchange of batch pipelines. "
          + "Reference {@link org.apache.flink.api.common.ExecutionMode}. "
          + "Set this to BATCH_FORCED if pipelines get blocked, see "
          + "https://issues.apache.org/jira/browse/FLINK-10672")
  @Default.String(PIPELINED)
  String getExecutionModeForBatch();

  void setExecutionModeForBatch(String executionMode);

  @Description(
      "Savepoint restore path. If specified, restores the streaming pipeline from the provided path.")
  String getSavepointPath();

  void setSavepointPath(String path);

  @Description(
      "Flag indicating whether non restored state is allowed if the savepoint "
          + "contains state for an operator that is no longer part of the pipeline.")
  @Default.Boolean(false)
  Boolean getAllowNonRestoredState();

  void setAllowNonRestoredState(Boolean allowNonRestoredState);

  @Description(
      "Flag indicating whether auto-balance sharding for WriteFiles transform should be enabled. "
          + "This might prove useful in streaming use-case, where pipeline needs to write quite many events "
          + "into files, typically divided into N shards. Default behavior on Flink would be, that some workers "
          + "will receive more shards to take care of than others. This cause workers to go out of balance in "
          + "terms of processing backlog and memory usage. Enabling this feature will make shards to be spread "
          + "evenly among available workers in improve throughput and memory usage stability.")
  @Default.Boolean(false)
  Boolean isAutoBalanceWriteFilesShardingEnabled();

  void setAutoBalanceWriteFilesShardingEnabled(Boolean autoBalanceWriteFilesShardingEnabled);
}
