/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;

import java.util.List;

/**
 * Options which can be used to configure a Flink PipelineRunner.
 */
public interface FlinkPipelineOptions extends PipelineOptions, ApplicationNameOptions, StreamingOptions {

	/**
	 * List of local files to make available to workers.
	 * <p>
	 * Jars are placed on the worker's classpath.
	 * <p>
	 * The default value is the list of jars from the main program's classpath.
	 */
	@Description("Jar-Files to send to all workers and put on the classpath. " +
			"The default value is all files from the classpath.")
	@JsonIgnore
	List<String> getFilesToStage();
	void setFilesToStage(List<String> value);

	/**
	 * The job name is used to identify jobs running on a Flink cluster.
	 */
	@Description("Dataflow job name, to uniquely identify active jobs. "
			+ "Defaults to using the ApplicationName-UserName-Date.")
	@Default.InstanceFactory(DataflowPipelineOptions.JobNameFactory.class)
	String getJobName();
	void setJobName(String value);

	/**
	 * The url of the Flink JobManager on which to execute pipelines. This can either be
	 * the the address of a cluster JobManager, in the form "host:port" or one of the special
	 * Strings "[local]", "[collection]" or "[auto]". "[local]" will start a local Flink
	 * Cluster in the JVM, "[collection]" will execute the pipeline on Java Collections while
	 * "[auto]" will let the system decide where to execute the pipeline based on the environment.
	 */
	@Description("Address of the Flink Master where the Pipeline should be executed. Can" +
			" either be of the form \"host:port\" or one of the special values [local], " +
			"[collection] or [auto].")
	String getFlinkMaster();
	void setFlinkMaster(String value);

	@Description("The degree of parallelism to be used when distributing operations onto workers.")
	@Default.Integer(-1)
	Integer getParallelism();
	void setParallelism(Integer value);

	@Description("The interval between consecutive checkpoints (i.e. snapshots of the current pipeline state used for " +
			"fault tolerance).")
	@Default.Long(-1L)
	Long getCheckpointingInterval();
	void setCheckpointingInterval(Long interval);

	@Description("Sets the number of times that failed tasks are re-executed. " +
			"A value of zero effectively disables fault tolerance. A value of -1 indicates " +
			"that the system default value (as defined in the configuration) should be used.")
	@Default.Integer(-1)
	Integer getNumberOfExecutionRetries();
	void setNumberOfExecutionRetries(Integer retries);

	@Description("Sets the delay between executions. A value of {@code -1} indicates that the default value should be used.")
	@Default.Long(-1L)
	Long getExecutionRetryDelay();
	void setExecutionRetryDelay(Long delay);
}
