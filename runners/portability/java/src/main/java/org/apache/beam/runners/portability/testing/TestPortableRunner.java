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
package org.apache.beam.runners.portability.testing;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.beam.runners.jobsubmission.JobServerDriver;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TestPortableRunner} is a pipeline runner that wraps a {@link PortableRunner} when running
 * tests against the {@link TestPipeline}.
 *
 * <p>This runner requires a {@link JobServerDriver} subclass with the following factory method:
 * <code>public static JobServerDriver fromParams(String[] args)</code>
 *
 * @see TestPipeline
 */
public class TestPortableRunner extends PipelineRunner<PipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(TestPortableRunner.class);
  private final PortablePipelineOptions options;

  private TestPortableRunner(PortablePipelineOptions options) {
    this.options = options;
  }

  public static TestPortableRunner fromOptions(PipelineOptions options) {
    return new TestPortableRunner(options.as(PortablePipelineOptions.class));
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    TestPortablePipelineOptions testPortablePipelineOptions =
        options.as(TestPortablePipelineOptions.class);
    String jobServerHostPort;
    JobServerDriver jobServerDriver;
    Class<JobServerDriver> jobServerDriverClass = testPortablePipelineOptions.getJobServerDriver();
    String[] parameters = testPortablePipelineOptions.getJobServerConfig();
    try {
      jobServerDriver =
          InstanceBuilder.ofType(jobServerDriverClass)
              .fromFactoryMethod("fromParams")
              .withArg(String[].class, parameters)
              .build();
      jobServerHostPort = jobServerDriver.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start job server", e);
    }

    try {
      PortablePipelineOptions portableOptions = options.as(PortablePipelineOptions.class);
      portableOptions.setRunner(PortableRunner.class);
      portableOptions.setJobEndpoint(jobServerHostPort);
      PortableRunner runner = PortableRunner.fromOptions(portableOptions);
      PipelineResult result = runner.run(pipeline);
      assertThat("Pipeline did not succeed.", result.waitUntilFinish(), Matchers.is(State.DONE));
      return result;
    } finally {
      jobServerDriver.stop();
    }
  }
}
