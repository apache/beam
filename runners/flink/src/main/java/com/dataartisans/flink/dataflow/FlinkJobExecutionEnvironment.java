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

import com.dataartisans.flink.dataflow.translation.FlinkPipelineTranslator;
import com.dataartisans.flink.dataflow.translation.FlinkBatchPipelineTranslator;
import com.dataartisans.flink.dataflow.translation.FlinkStreamingPipelineTranslator;
import com.google.cloud.dataflow.sdk.Pipeline;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlinkJobExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJobExecutionEnvironment.class);

	private final FlinkPipelineOptions options;

	/**
	 * The Flink Batch execution environment. This is instantiated to either a
	 * {@link org.apache.flink.api.java.CollectionEnvironment},
	 * a {@link org.apache.flink.api.java.LocalEnvironment} or
	 * a {@link org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration
	 * options.
	 */
	private ExecutionEnvironment flinkBatchEnv;


	/**
	 * The Flink Streaming execution environment. This is instantiated to either a
	 * {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment} or
	 * a {@link org.apache.flink.streaming.api.environment.RemoteStreamEnvironment}, depending
	 * on the configuration options, and more specifically, the url of the master url.
	 */
	private StreamExecutionEnvironment flinkStreamEnv;

	/**
	 * Translator for this FlinkPipelineRunner. Its role is to translate the Dataflow operators to
	 * their Flink based counterparts. Based on the options provided by the user, if we have a streaming job,
	 * this is instantiated to a FlinkStreamingPipelineTranslator. In other case, i.e. a batch job,
	 * a FlinkBatchPipelineTranslator is created.
	 */
	private FlinkPipelineTranslator flinkPipelineTranslator;

	public FlinkJobExecutionEnvironment(FlinkPipelineOptions options) {
		if (options == null) {
			throw new IllegalArgumentException("Options in the FlinkJobExecutionEnvironment cannot be NULL.");
		}
		this.options = options;
		this.createJobEnvironment();
		this.createJobGraphTranslator();
	}

	/**
	 * Depending on the type of job (Streaming or Batch) and the user-specified options,
	 * this method creates the adequate ExecutionEnvironment.
	 */
	private void createJobEnvironment() {
		if (options.isStreaming()) {
			LOG.info("Creating the required STREAMING Environment.");
			createStreamExecutionEnvironment();
		} else {
			LOG.info("Creating the required BATCH Environment.");
			createBatchExecutionEnvironment();
		}
	}

	/**
	 * Depending on the type of job (Streaming or Batch), this method creates the adequate job graph
	 * translator. In the case of batch, it will work with DataSets, while for streaming, it will work
	 * with DataStreams.
	 */
	private void createJobGraphTranslator() {
		checkInitializationState();
		if (this.flinkPipelineTranslator != null) {
			throw new IllegalStateException("JobGraphTranslator already initialized.");
		}

		this.flinkPipelineTranslator = options.isStreaming() ?
				new FlinkStreamingPipelineTranslator(flinkStreamEnv, options) :
				new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
	}

	public void translate(Pipeline pipeline) {
		checkInitializationState();
		if(this.flinkBatchEnv == null && this.flinkStreamEnv == null) {
			createJobEnvironment();
		}
		if (this.flinkPipelineTranslator == null) {
			createJobGraphTranslator();
		}
		this.flinkPipelineTranslator.translate(pipeline);
	}

	public JobExecutionResult executeJob() throws Exception {
		if (options.isStreaming()) {

			System.out.println("Plan: " + this.flinkStreamEnv.getExecutionPlan());

			if (this.flinkStreamEnv == null) {
				throw new RuntimeException("JobExecutionEnvironment not initialized.");
			}
			if (this.flinkPipelineTranslator == null) {
				throw new RuntimeException("JobGraphTranslator not initialized.");
			}
			return this.flinkStreamEnv.execute();
		} else {
			if (this.flinkBatchEnv == null) {
				throw new RuntimeException("JobExecutionEnvironment not initialized.");
			}
			if (this.flinkPipelineTranslator == null) {
				throw new RuntimeException("JobGraphTranslator not initialized.");
			}
			return this.flinkBatchEnv.execute();
		}
	}

	/**
	 * If the submitted job is a batch processing job, this method creates the adequate
	 * Flink {@link org.apache.flink.api.java.ExecutionEnvironment} depending
	 * on the user-specified options.
	 */
	private void createBatchExecutionEnvironment() {
		if (this.flinkStreamEnv != null || this.flinkBatchEnv != null) {
			throw new RuntimeException("JobExecutionEnvironment already initialized.");
		}

		String masterUrl = options.getFlinkMaster();
		this.flinkStreamEnv = null;

		// depending on the master, create the right environment.
		if (masterUrl.equals("[local]")) {
			this.flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment();
		} else if (masterUrl.equals("[collection]")) {
			this.flinkBatchEnv = new CollectionEnvironment();
		} else if (masterUrl.equals("[auto]")) {
			this.flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
		} else if (masterUrl.matches(".*:\\d*")) {
			String[] parts = masterUrl.split(":");
			List<String> stagingFiles = options.getFilesToStage();
			this.flinkBatchEnv = ExecutionEnvironment.createRemoteEnvironment(parts[0],
					Integer.parseInt(parts[1]),
					stagingFiles.toArray(new String[stagingFiles.size()]));
		} else {
			LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
			this.flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
		}

		// set the correct parallelism.
		if (options.getParallelism() != -1 && !(this.flinkBatchEnv instanceof CollectionEnvironment)) {
			this.flinkBatchEnv.setParallelism(options.getParallelism());
		}

		// set parallelism in the options (required by some execution code)
		options.setParallelism(flinkBatchEnv.getParallelism());
	}

	/**
	 * If the submitted job is a stream processing job, this method creates the adequate
	 * Flink {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} depending
	 * on the user-specified options.
	 */
	private void createStreamExecutionEnvironment() {
		if (this.flinkStreamEnv != null || this.flinkBatchEnv != null) {
			throw new RuntimeException("JobExecutionEnvironment already initialized.");
		}

		String masterUrl = options.getFlinkMaster();
		this.flinkBatchEnv = null;

		// depending on the master, create the right environment.
		if (masterUrl.equals("[local]")) {
			this.flinkStreamEnv = StreamExecutionEnvironment.createLocalEnvironment();
		} else if (masterUrl.equals("[auto]")) {
			this.flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		} else if (masterUrl.matches(".*:\\d*")) {
			String[] parts = masterUrl.split(":");
			List<String> stagingFiles = options.getFilesToStage();
			this.flinkStreamEnv = StreamExecutionEnvironment.createRemoteEnvironment(parts[0],
					Integer.parseInt(parts[1]), stagingFiles.toArray(new String[stagingFiles.size()]));
		} else {
			LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
			this.flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		}

		// set the correct parallelism.
		if (options.getParallelism() != -1) {
			this.flinkStreamEnv.setParallelism(options.getParallelism());
		}

		// set parallelism in the options (required by some execution code)
		options.setParallelism(flinkStreamEnv.getParallelism());

		// although we do not use the generated timestamps,
		// enabling timestamps is needed for the watermarks.
		this.flinkStreamEnv.getConfig().enableTimestamps();

		this.flinkStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		this.flinkStreamEnv.enableCheckpointing(1000);
		this.flinkStreamEnv.setNumberOfExecutionRetries(5);

		LOG.info("Setting execution retry delay to 3 sec");
		this.flinkStreamEnv.getConfig().setExecutionRetryDelay(3000);
	}

	private final void checkInitializationState() {
		if (this.options == null) {
			throw new IllegalStateException("FlinkJobExecutionEnvironment is not initialized yet.");
		}

		if (options.isStreaming() && this.flinkBatchEnv != null) {
			throw new IllegalStateException("Attempted to run a Streaming Job with a Batch Execution Environment.");
		} else if (!options.isStreaming() && this.flinkStreamEnv != null) {
			throw new IllegalStateException("Attempted to run a Batch Job with a Streaming Execution Environment.");
		}
	}
}
