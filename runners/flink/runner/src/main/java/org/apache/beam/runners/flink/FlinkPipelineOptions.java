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
  @JsonIgnore
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

}
