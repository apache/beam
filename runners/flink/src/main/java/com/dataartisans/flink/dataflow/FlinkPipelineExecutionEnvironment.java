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
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The class that instantiates and manages the execution of a given job.
 * Depending on if the job is a Streaming or Batch processing one, it creates
 * the adequate execution environment ({@link ExecutionEnvironment} or {@link StreamExecutionEnvironment}),
 * the necessary {@link FlinkPipelineTranslator} ({@link FlinkBatchPipelineTranslator} or
 * {@link FlinkStreamingPipelineTranslator})to transform the Beam job into a Flink one, and
 * executes the (translated) job.
 */
public class FlinkPipelineExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineExecutionEnvironment.class);

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
	 * on the configuration options, and more specifically, the url of the master.
	 */
	private StreamExecutionEnvironment flinkStreamEnv;

	/**
	 * Translator for this FlinkPipelineRunner. Its role is to translate the Beam operators to
	 * their Flink counterparts. Based on the options provided by the user, if we have a streaming job,
	 * this is instantiated as a {@link FlinkStreamingPipelineTranslator}. In other case, i.e. a batch job,
	 * a {@link FlinkBatchPipelineTranslator} is created.
	 */
	private FlinkPipelineTranslator flinkPipelineTranslator;

	/**
	 * Creates a {@link FlinkPipelineExecutionEnvironment} with the user-specified parameters in the
	 * provided {@link FlinkPipelineOptions}.
	 *
	 * @param options the user-defined pipeline options.
	 * */
	public FlinkPipelineExecutionEnvironment(FlinkPipelineOptions options) {
		this.options = Preconditions.checkNotNull(options);
		this.createPipelineExecutionEnvironment();
		this.createPipelineTranslator();
	}

	/**
	 * Depending on the type of job (Streaming or Batch) and the user-specified options,
	 * this method creates the adequate ExecutionEnvironment.
	 */
	private void createPipelineExecutionEnvironment() {
		if (options.isStreaming()) {
			createStreamExecutionEnvironment();
		} else {
			createBatchExecutionEnvironment();
		}
	}

	/**
	 * Depending on the type of job (Streaming or Batch), this method creates the adequate job graph
	 * translator. In the case of batch, it will work with {@link org.apache.flink.api.java.DataSet},
	 * while for streaming, it will work with {@link org.apache.flink.streaming.api.datastream.DataStream}.
	 */
	private void createPipelineTranslator() {
		checkInitializationState();
		if (this.flinkPipelineTranslator != null) {
			throw new IllegalStateException("FlinkPipelineTranslator already initialized.");
		}

		this.flinkPipelineTranslator = options.isStreaming() ?
				new FlinkStreamingPipelineTranslator(flinkStreamEnv, options) :
				new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
	}

	/**
	 * Depending on if the job is a Streaming or a Batch one, this method creates
	 * the necessary execution environment and pipeline translator, and translates
	 * the {@link com.google.cloud.dataflow.sdk.values.PCollection} program into
	 * a {@link org.apache.flink.api.java.DataSet} or {@link org.apache.flink.streaming.api.datastream.DataStream}
	 * one.
	 * */
	public void translate(Pipeline pipeline) {
		checkInitializationState();
		if(this.flinkBatchEnv == null && this.flinkStreamEnv == null) {
			createPipelineExecutionEnvironment();
		}
		if (this.flinkPipelineTranslator == null) {
			createPipelineTranslator();
		}
		this.flinkPipelineTranslator.translate(pipeline);
	}

	/**
	 * Launches the program execution.
	 * */
	public JobExecutionResult executePipeline() throws Exception {
		if (options.isStreaming()) {
			if (this.flinkStreamEnv == null) {
				throw new RuntimeException("FlinkPipelineExecutionEnvironment not initialized.");
			}
			if (this.flinkPipelineTranslator == null) {
				throw new RuntimeException("FlinkPipelineTranslator not initialized.");
			}
			return this.flinkStreamEnv.execute();
		} else {
			if (this.flinkBatchEnv == null) {
				throw new RuntimeException("FlinkPipelineExecutionEnvironment not initialized.");
			}
			if (this.flinkPipelineTranslator == null) {
				throw new RuntimeException("FlinkPipelineTranslator not initialized.");
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
			throw new RuntimeException("FlinkPipelineExecutionEnvironment already initialized.");
		}

		LOG.info("Creating the required Batch Execution Environment.");

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
			throw new RuntimeException("FlinkPipelineExecutionEnvironment already initialized.");
		}

		LOG.info("Creating the required Streaming Environment.");

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
		// this.flinkStreamEnv.getConfig().enableTimestamps();
		this.flinkStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// for the following 2 parameters, a value of -1 means that Flink will use
		// the default values as specified in the configuration.
		this.flinkStreamEnv.setNumberOfExecutionRetries(options.getNumberOfExecutionRetries());
		this.flinkStreamEnv.getConfig().setExecutionRetryDelay(options.getExecutionRetryDelay());

		// A value of -1 corresponds to disabled checkpointing (see CheckpointConfig in Flink).
		// If the value is not -1, then the validity checks are applied.
		// By default, checkpointing is disabled.
		long checkpointInterval = options.getCheckpointingInterval();
		if(checkpointInterval != -1) {
			if (checkpointInterval < 1) {
				throw new IllegalArgumentException("The checkpoint interval must be positive");
			}
			this.flinkStreamEnv.enableCheckpointing(checkpointInterval);
		}
	}

	private void checkInitializationState() {
		if (options.isStreaming() && this.flinkBatchEnv != null) {
			throw new IllegalStateException("Attempted to run a Streaming Job with a Batch Execution Environment.");
		} else if (!options.isStreaming() && this.flinkStreamEnv != null) {
			throw new IllegalStateException("Attempted to run a Batch Job with a Streaming Execution Environment.");
		}
	}
}
