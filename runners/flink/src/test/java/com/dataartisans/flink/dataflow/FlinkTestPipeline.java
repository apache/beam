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
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;

/**
 * {@link com.google.cloud.dataflow.sdk.Pipeline} for testing Dataflow programs on the
 * {@link com.dataartisans.flink.dataflow.FlinkPipelineRunner}.
 */
public class FlinkTestPipeline extends Pipeline {

	/**
	 * Creates and returns a new test pipeline for batch execution.
	 *
	 * <p> Use {@link com.google.cloud.dataflow.sdk.testing.DataflowAssert} to add tests, then call
	 * {@link Pipeline#run} to execute the pipeline and check the tests.
	 */
	public static FlinkTestPipeline create() {
		return create(false);
	}

	/**
	 * Creates and returns a new test pipeline for streaming execution.
	 *
	 * <p> Use {@link com.google.cloud.dataflow.sdk.testing.DataflowAssert} to add tests, then call
	 * {@link Pipeline#run} to execute the pipeline and check the tests.
	 *
	 * @return The Test Pipeline
	 */
	public static FlinkTestPipeline createStreaming() {
		return create(true);
	}

	/**
	 * Creates and returns a new test pipeline for streaming or batch execution.
	 *
	 * <p> Use {@link com.google.cloud.dataflow.sdk.testing.DataflowAssert} to add tests, then call
	 * {@link Pipeline#run} to execute the pipeline and check the tests.
	 *
	 * @param streaming True for streaming mode, False for batch
	 * @return The Test Pipeline
	 */
	public static FlinkTestPipeline create(boolean streaming) {
		FlinkPipelineRunner flinkRunner = FlinkPipelineRunner.createForTest(streaming);
		FlinkPipelineOptions pipelineOptions = flinkRunner.getPipelineOptions();
		pipelineOptions.setStreaming(streaming);
		return new FlinkTestPipeline(flinkRunner, pipelineOptions);
	}

	private FlinkTestPipeline(PipelineRunner<? extends PipelineResult> runner, PipelineOptions
			options) {
		super(runner, options);
	}
}

