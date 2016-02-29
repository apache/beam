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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to a Flink Plan and then executing them either locally
 * or on a Flink cluster, depending on the configuration.
 * <p>
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner}.
 */
public class FlinkPipelineRunner extends PipelineRunner<FlinkRunnerResult> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineRunner.class);

	/**
	 * Provided options.
	 */
	private final FlinkPipelineOptions options;

	private final FlinkPipelineExecutionEnvironment flinkJobEnv;

	/**
	 * Construct a runner from the provided options.
	 *
	 * @param options Properties which configure the runner.
	 * @return The newly created runner.
	 */
	public static FlinkPipelineRunner fromOptions(PipelineOptions options) {
		FlinkPipelineOptions flinkOptions =
				PipelineOptionsValidator.validate(FlinkPipelineOptions.class, options);
		ArrayList<String> missing = new ArrayList<>();

		if (flinkOptions.getAppName() == null) {
			missing.add("appName");
		}
		if (missing.size() > 0) {
			throw new IllegalArgumentException(
					"Missing required values: " + Joiner.on(',').join(missing));
		}

		if (flinkOptions.getFilesToStage() == null) {
			flinkOptions.setFilesToStage(detectClassPathResourcesToStage(
					DataflowPipelineRunner.class.getClassLoader()));
			LOG.info("PipelineOptions.filesToStage was not specified. "
							+ "Defaulting to files from the classpath: will stage {} files. "
							+ "Enable logging at DEBUG level to see which files will be staged.",
					flinkOptions.getFilesToStage().size());
			LOG.debug("Classpath elements: {}", flinkOptions.getFilesToStage());
		}

		// Verify jobName according to service requirements.
		String jobName = flinkOptions.getJobName().toLowerCase();
		Preconditions.checkArgument(jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?"), "JobName invalid; " +
				"the name must consist of only the characters " + "[-a-z0-9], starting with a letter " +
				"and ending with a letter " + "or number");
		Preconditions.checkArgument(jobName.length() <= 40,
				"JobName too long; must be no more than 40 characters in length");

		// Set Flink Master to [auto] if no option was specified.
		if (flinkOptions.getFlinkMaster() == null) {
			flinkOptions.setFlinkMaster("[auto]");
		}

		return new FlinkPipelineRunner(flinkOptions);
	}

	private FlinkPipelineRunner(FlinkPipelineOptions options) {
		this.options = options;
		this.flinkJobEnv = new FlinkPipelineExecutionEnvironment(options);
	}

	@Override
	public FlinkRunnerResult run(Pipeline pipeline) {
		LOG.info("Executing pipeline using FlinkPipelineRunner.");

		LOG.info("Translating pipeline to Flink program.");

		this.flinkJobEnv.translate(pipeline);

		LOG.info("Starting execution of Flink program.");
		
		JobExecutionResult result;
		try {
			result = this.flinkJobEnv.executePipeline();
		} catch (Exception e) {
			LOG.error("Pipeline execution failed", e);
			throw new RuntimeException("Pipeline execution failed", e);
		}

		LOG.info("Execution finished in {} msecs", result.getNetRuntime());

		Map<String, Object> accumulators = result.getAllAccumulatorResults();
		if (accumulators != null && !accumulators.isEmpty()) {
			LOG.info("Final aggregator values:");

			for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
				LOG.info("{} : {}", entry.getKey(), entry.getValue());
			}
		}

		return new FlinkRunnerResult(accumulators, result.getNetRuntime());
	}

	/**
	 * For testing.
	 */
	public FlinkPipelineOptions getPipelineOptions() {
		return options;
	}

	/**
	 * Constructs a runner with default properties for testing.
	 *
	 * @return The newly created runner.
	 */
	public static FlinkPipelineRunner createForTest(boolean streaming) {
		FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
		// we use [auto] for testing since this will make it pick up the Testing
		// ExecutionEnvironment
		options.setFlinkMaster("[auto]");
		options.setStreaming(streaming);
		return new FlinkPipelineRunner(options);
	}

	@Override
	public <Output extends POutput, Input extends PInput> Output apply(
			PTransform<Input, Output> transform, Input input) {
		return super.apply(transform, input);
	}

	/////////////////////////////////////////////////////////////////////////////

	@Override
	public String toString() {
		return "DataflowPipelineRunner#" + hashCode();
	}

	/**
	 * Attempts to detect all the resources the class loader has access to. This does not recurse
	 * to class loader parents stopping it from pulling in resources from the system class loader.
	 *
	 * @param classLoader The URLClassLoader to use to detect resources to stage.
	 * @return A list of absolute paths to the resources the class loader uses.
	 * @throws IllegalArgumentException If either the class loader is not a URLClassLoader or one
	 *                                  of the resources the class loader exposes is not a file resource.
	 */
	protected static List<String> detectClassPathResourcesToStage(ClassLoader classLoader) {
		if (!(classLoader instanceof URLClassLoader)) {
			String message = String.format("Unable to use ClassLoader to detect classpath elements. "
					+ "Current ClassLoader is %s, only URLClassLoaders are supported.", classLoader);
			LOG.error(message);
			throw new IllegalArgumentException(message);
		}

		List<String> files = new ArrayList<>();
		for (URL url : ((URLClassLoader) classLoader).getURLs()) {
			try {
				files.add(new File(url.toURI()).getAbsolutePath());
			} catch (IllegalArgumentException | URISyntaxException e) {
				String message = String.format("Unable to convert url (%s) to file.", url);
				LOG.error(message);
				throw new IllegalArgumentException(message, e);
			}
		}
		return files;
	}
}
