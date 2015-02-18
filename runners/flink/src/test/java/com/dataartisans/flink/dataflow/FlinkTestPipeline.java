/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow;
/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A creator of test pipelines which can be used inside of tests that can be
 * configured to run locally or against the live service.
 *
 * <p> It is recommended to tag hand-selected tests for this purpose using the
 * RunnableOnService Category annotation, as each test run against the service
 * will spin up and tear down a single VM.
 *
 * <p> In order to run tests on the dataflow pipeline service, the following
 * conditions must be met:
 * <ul>
 * <li> runIntegrationTestOnService System property must be set to true.
 * <li> System property "projectName" must be set to your Cloud project.
 * <li> System property "temp_gcs_directory" must be set to a valid GCS bucket.
 * <li> Jars containing the SDK and test classes must be added to the test classpath.
 * </ul>
 *
 * <p> Use {@link com.google.cloud.dataflow.sdk.testing.DataflowAssert} for tests, as it integrates with this test
 * harness in both direct and remote execution modes.  For example:
 *
 * <pre>{@code
 * Pipeline p = TestPipeline.create();
 * PCollection<Integer> output = ...
 *
 * DataflowAssert.that(output)
 *     .containsInAnyOrder(1, 2, 3, 4);
 * p.run();
 * }</pre>
 *
 */
public class FlinkTestPipeline extends Pipeline {
	private static final String PROPERTY_DATAFLOW_OPTIONS = "dataflowOptions";
	private static final Logger LOG = LoggerFactory.getLogger(FlinkTestPipeline.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	/**
	 * Creates and returns a new test pipeline.
	 *
	 * <p> Use {@link com.google.cloud.dataflow.sdk.testing.DataflowAssert} to add tests, then call
	 * {@link Pipeline#run} to execute the pipeline and check the tests.
	 */
	public static FlinkTestPipeline create() {
		FlinkLocalPipelineRunner flinkRunner = FlinkLocalPipelineRunner.createForTest();
		return new FlinkTestPipeline(flinkRunner, flinkRunner.getPipelineOptions());
	}

	private FlinkTestPipeline(PipelineRunner<? extends PipelineResult> runner, PipelineOptions
			options) {
		super(runner, options);
	}
}

