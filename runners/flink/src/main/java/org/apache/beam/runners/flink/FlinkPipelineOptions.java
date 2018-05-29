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


import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;

/**
 * Options which can be used to configure a Flink PipelineRunner.
 */
public interface FlinkPipelineOptions
    extends PipelineOptions, ApplicationNameOptions, StreamingOptions {

  /**
   * List of local files to make available to workers.
   *
   * <p>Jars are placed on the worker's classpath.
   *
   * <p>The default value is the list of jars from the main program's classpath.
   */
  @Description("Jar-Files to send to all workers and put on the classpath. "
      + "The default value is all files from the classpath.")
  List<String> getFilesToStage();
  void setFilesToStage(List<String> value);

  /**
   * The url of the Flink JobManager on which to execute pipelines. This can either be
   * the the address of a cluster JobManager, in the form "host:port" or one of the special
   * Strings "[local]", "[collection]" or "[auto]". "[local]" will start a local Flink
   * Cluster in the JVM, "[collection]" will execute the pipeline on Java Collections while
   * "[auto]" will let the system decide where to execute the pipeline based on the environment.
   */
  @Description("Address of the Flink Master where the Pipeline should be executed. Can"
      + " either be of the form \"host:port\" or one of the special values [local], "
      + "[collection] or [auto].")
  String getFlinkMaster();
  void setFlinkMaster(String value);

  @Description("The degree of parallelism to be used when distributing operations onto workers.")
  @Default.InstanceFactory(DefaultParallelismFactory.class)
  Integer getParallelism();
  void setParallelism(Integer value);

  @Description("The interval between consecutive checkpoints (i.e. snapshots of the current"
      + "pipeline state used for fault tolerance).")
  @Default.Long(-1L)
  Long getCheckpointingInterval();
  void setCheckpointingInterval(Long interval);

  @Description("The checkpointing mode that defines consistency guarantee.")
  @Default.Enum("AT_LEAST_ONCE")
  CheckpointingMode getCheckpointingMode();
  void setCheckpointingMode(CheckpointingMode mode);

  @Description("The maximum time that a checkpoint may take before being discarded.")
  @Default.Long(20 * 60 * 1000)
  Long getCheckpointTimeoutMillis();
  void setCheckpointTimeoutMillis(Long checkpointTimeoutMillis);

  @Description("Sets the number of times that failed tasks are re-executed. "
      + "A value of zero effectively disables fault tolerance. A value of -1 indicates "
      + "that the system default value (as defined in the configuration) should be used.")
  @Default.Integer(-1)
  Integer getNumberOfExecutionRetries();
  void setNumberOfExecutionRetries(Integer retries);

  @Description("Sets the delay between executions. A value of {@code -1} "
      + "indicates that the default value should be used.")
  @Default.Long(-1L)
  Long getExecutionRetryDelay();
  void setExecutionRetryDelay(Long delay);

  @Description("Sets the behavior of reusing objects.")
  @Default.Boolean(false)
  Boolean getObjectReuse();
  void setObjectReuse(Boolean reuse);

  /**
   * State backend to store Beam's state during computation.
   * Note: Only applicable when executing in streaming mode.
   */
  @Description("Sets the state backend to use in streaming mode. "
      + "Otherwise the default is read from the Flink config.")
  @JsonIgnore
  AbstractStateBackend getStateBackend();
  void setStateBackend(AbstractStateBackend stateBackend);

  @Description("Enable/disable Beam metrics in Flink Runner")
  @Default.Boolean(true)
  Boolean getEnableMetrics();
  void setEnableMetrics(Boolean enableMetrics);

  /**
   * Enables or disables externalized checkpoints.
   */
  @Description("Enables or disables externalized checkpoints. "
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
   * Whether to shutdown sources when their watermark reaches {@code +Inf}. For production use
   * cases you want this to be disabled because Flink will currently (versions {@literal <=} 1.5)
   * stop doing checkpoints when any operator (which includes sources) is finished.
   *
   * <p>Please see <a href="https://issues.apache.org/jira/browse/FLINK-2491">FLINK-2491</a> for
   * progress on this issue.
   */
  @Description("If set, shutdown sources when their watermark reaches +Inf.")
  @Default.Boolean(false)
  Boolean isShutdownSourcesOnFinalWatermark();
  void setShutdownSourcesOnFinalWatermark(Boolean shutdownOnFinalWatermark);
}
