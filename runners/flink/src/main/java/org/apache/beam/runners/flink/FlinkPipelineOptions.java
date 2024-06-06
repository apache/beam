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

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;

/**
 * Options which can be used to configure the Flink Runner.
 *
 * <p>Avoid using `org.apache.flink.*` members below. This allows including the flink runner without
 * requiring flink on the classpath (e.g. to use with the direct runner).
 */
public interface FlinkPipelineOptions
    extends PipelineOptions,
        ApplicationNameOptions,
        StreamingOptions,
        FileStagingOptions,
        VersionDependentFlinkPipelineOptions {

  String AUTO = "[auto]";
  String PIPELINED = "PIPELINED";
  String EXACTLY_ONCE = "EXACTLY_ONCE";

  /**
   * The url of the Flink JobManager on which to execute pipelines. This can either be the address
   * of a cluster JobManager, in the form "host:port" or one of the special Strings "[local]",
   * "[collection]" or "[auto]". "[local]" will start a local Flink Cluster in the JVM,
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
      "The maximum number of concurrent checkpoints. Defaults to 1 (=no concurrent checkpoints).")
  @Default.Integer(1)
  int getNumConcurrentCheckpoints();

  void setNumConcurrentCheckpoints(int maxConcurrentCheckpoints);

  @Description(
      "Sets the expected behaviour for tasks in case that they encounter an error in their "
          + "checkpointing procedure. If this is set to true, the task will fail on checkpointing error. "
          + "If this is set to false, the task will only decline the checkpoint and continue running. ")
  @Default.Boolean(true)
  Boolean getFailOnCheckpointingErrors();

  void setFailOnCheckpointingErrors(Boolean failOnCheckpointingErrors);

  @Description(
      "If set, finishes the current bundle and flushes all output before checkpointing the state of the operators. "
          + "By default, starts checkpointing immediately and buffers any remaining bundle output as part of the checkpoint. "
          + "The setting may affect the checkpoint alignment.")
  @Default.Boolean(false)
  boolean getFinishBundleBeforeCheckpointing();

  void setFinishBundleBeforeCheckpointing(boolean finishBundleBeforeCheckpointing);

  @Description(
      "If set, Unaligned checkpoints contain in-flight data (i.e., data stored in buffers) as part of the "
          + "checkpoint state, allowing checkpoint barriers to overtake these buffers. Thus, the checkpoint duration "
          + "becomes independent of the current throughput as checkpoint barriers are effectively not embedded into the "
          + "stream of data anymore")
  @Default.Boolean(false)
  boolean getUnalignedCheckpointEnabled();

  void setUnalignedCheckpointEnabled(boolean unalignedCheckpointEnabled);

  @Description("Forces unaligned checkpoints, particularly allowing them for iterative jobs.")
  @Default.Boolean(false)
  boolean getForceUnalignedCheckpointEnabled();

  void setForceUnalignedCheckpointEnabled(boolean forceUnalignedCheckpointEnabled);

  @Description(
      "Shuts down sources which have been idle for the configured time of milliseconds. Once a source has been "
          + "shut down, checkpointing is not possible anymore. Shutting down the sources eventually leads to pipeline "
          + "shutdown (=Flink job finishes) once all input has been processed. Unless explicitly set, this will "
          + "default to Long.MAX_VALUE when checkpointing is enabled and to 0 when checkpointing is disabled. "
          + "See https://issues.apache.org/jira/browse/FLINK-2491 for progress on this issue.")
  @Default.Long(-1L)
  Long getShutdownSourcesAfterIdleMs();

  void setShutdownSourcesAfterIdleMs(Long timeoutMs);

  @Description(
      "Sets the number of times that failed tasks are re-executed. "
          + "A value of zero effectively disables fault tolerance. A value of -1 indicates "
          + "that the system default value (as defined in the configuration) should be used.")
  @Default.Integer(-1)
  Integer getNumberOfExecutionRetries();

  void setNumberOfExecutionRetries(Integer retries);

  @Description(
      "Set job check interval in seconds under detached mode in method waitUntilFinish, "
          + "by default it is 5 seconds")
  @Default.Integer(5)
  int getJobCheckIntervalInSecs();

  void setJobCheckIntervalInSecs(int seconds);

  @Description("Specifies if the pipeline is submitted in attached or detached mode")
  @Default.Boolean(true)
  boolean getAttachedMode();

  void setAttachedMode(boolean attachedMode);

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

  @Description("Sets the behavior of operator chaining.")
  @Default.Boolean(true)
  Boolean getOperatorChaining();

  void setOperatorChaining(Boolean chaining);

  /**
   * State backend to store Beam's state during computation. Note: Only applicable when executing in
   * streaming mode.
   *
   * @deprecated Please use setStateBackend below.
   */
  @Deprecated
  @Description(
      "Sets the state backend factory to use in streaming mode. "
          + "Defaults to the flink cluster's state.backend configuration.")
  Class<? extends FlinkStateBackendFactory> getStateBackendFactory();

  /** @deprecated Please use setStateBackend below. */
  @Deprecated
  void setStateBackendFactory(Class<? extends FlinkStateBackendFactory> stateBackendFactory);

  void setStateBackend(String stateBackend);

  @Description("State backend to store Beam's state. Use 'rocksdb' or 'filesystem'.")
  String getStateBackend();

  void setStateBackendStoragePath(String path);

  @Description(
      "State backend path to persist state backend data. Used to initialize state backend.")
  String getStateBackendStoragePath();

  @Description("Disable Beam metrics in Flink Runner")
  @Default.Boolean(false)
  Boolean getDisableMetrics();

  void setDisableMetrics(Boolean disableMetrics);

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

  @Description(
      "The maximum number of elements in a bundle. Default values are 1000 for a streaming job and 1,000,000 for batch")
  @Default.InstanceFactory(MaxBundleSizeFactory.class)
  Long getMaxBundleSize();

  void setMaxBundleSize(Long size);

  /**
   * Maximum bundle size factory. For a streaming job it's desireable to keep bundle size small to
   * optimize latency. In batch, we optimize for throughput and hence bundle size is kept large.
   */
  class MaxBundleSizeFactory implements DefaultValueFactory<Long> {
    @Override
    public Long create(PipelineOptions options) {
      if (options.as(StreamingOptions.class).isStreaming()) {
        return 1000L;
      } else {
        return 1000000L;
      }
    }
  }

  @Description(
      "The maximum time to wait before finalising a bundle (in milliseconds). Default values are 1000 for streaming and 10,000 for batch.")
  @Default.InstanceFactory(MaxBundleTimeFactory.class)
  Long getMaxBundleTimeMills();

  void setMaxBundleTimeMills(Long time);

  /**
   * Maximum bundle time factory. For a streaming job it's desireable to keep the value small to
   * optimize latency. In batch, we optimize for throughput and hence bundle time size is kept
   * larger.
   */
  class MaxBundleTimeFactory implements DefaultValueFactory<Long> {
    @Override
    public Long create(PipelineOptions options) {
      if (options.as(StreamingOptions.class).isStreaming()) {
        return 1000L;
      } else {
        return 10000L;
      }
    }
  }

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

  @Description(
      "If not null, reports the checkpoint duration of each ParDo stage in the provided metric namespace.")
  String getReportCheckpointDuration();

  void setReportCheckpointDuration(String metricNamespace);

  @Description(
      "Flag indicating whether result of GBK needs to be re-iterable. Re-iterable result implies that all values for a single key must fit in memory as we currently do not support spilling to disk.")
  @Default.Boolean(false)
  Boolean getReIterableGroupByKeyResult();

  void setReIterableGroupByKeyResult(Boolean reIterableGroupByKeyResult);

  @Description(
      "Remove unneeded deep copy between operators. See https://issues.apache.org/jira/browse/BEAM-11146")
  @Default.Boolean(false)
  Boolean getFasterCopy();

  void setFasterCopy(Boolean fasterCopy);

  @Description(
      "Directory containing Flink YAML configuration files. "
          + "These properties will be set to all jobs submitted to Flink and take precedence "
          + "over configurations in FLINK_CONF_DIR.")
  String getFlinkConfDir();

  void setFlinkConfDir(String confDir);

  @Description(
      "Set the maximum size of input split when data is read from a filesystem. 0 implies no max size.")
  @Default.Long(0)
  Long getFileInputSplitMaxSizeMB();

  void setFileInputSplitMaxSizeMB(Long fileInputSplitMaxSizeMB);

  @Description(
      "Allow drain operation for flink pipelines that contain RequiresStableInput operator. Note that at time of draining,"
          + "the RequiresStableInput contract might be violated if there any processing related failures in the DoFn operator.")
  @Default.Boolean(false)
  Boolean getEnableStableInputDrain();

  void setEnableStableInputDrain(Boolean enableStableInputDrain);

  static FlinkPipelineOptions defaults() {
    return PipelineOptionsFactory.as(FlinkPipelineOptions.class);
  }
}
