/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * A creator of test pipelines that can be used inside of tests that can be
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
 * <p> Use {@link DataflowAssert} for tests, as it integrates with this test
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
public class TestPipeline extends Pipeline {
  private static final String PROPERTY_DATAFLOW_OPTIONS = "dataflowOptions";
  private static final Logger LOG = LoggerFactory.getLogger(TestPipeline.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Creates and returns a new test pipeline.
   *
   * <p> Use {@link DataflowAssert} to add tests, then call
   * {@link Pipeline#run} to execute the pipeline and check the tests.
   */
  public static TestPipeline create() {
    if (Boolean.parseBoolean(System.getProperty("runIntegrationTestOnService"))) {
      TestDataflowPipelineOptions options = getPipelineOptions();
      LOG.info("Using passed in options: " + options);
      options.setStableUniqueNames(CheckEnabled.ERROR);
      return new TestPipeline(TestDataflowPipelineRunner.fromOptions(options), options);
    } else {
      DirectPipelineRunner directRunner = DirectPipelineRunner.createForTest();
      directRunner.getPipelineOptions().setAppName(getAppName());
      directRunner.getPipelineOptions().setStableUniqueNames(CheckEnabled.ERROR);
      return new TestPipeline(directRunner, directRunner.getPipelineOptions());
    }
  }

  private TestPipeline(PipelineRunner<? extends PipelineResult> runner, PipelineOptions options) {
    super(runner, options);
  }

  /**
   * Runs this {@link TestPipeline}, unwrapping any {@code AssertionError}
   * that is raised during testing.
   */
  @Override
  public PipelineResult run() {
    try {
      return super.run();
    } catch (RuntimeException exc) {
      Throwable cause = exc.getCause();
      if (cause instanceof AssertionError) {
        throw (AssertionError) cause;
      } else {
        throw exc;
      }
    }
  }

  @Override
  public String toString() {
    return "TestPipeline#" + getOptions().as(ApplicationNameOptions.class).getAppName();
  }

  /**
   * Creates PipelineOptions for testing with a DataflowPipelineRunner.
   */
  static TestDataflowPipelineOptions getPipelineOptions() {
    try {
      TestDataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(
              MAPPER.readValue(System.getProperty(PROPERTY_DATAFLOW_OPTIONS), String[].class))
          .as(TestDataflowPipelineOptions.class);
      options.setAppName(getAppName());
      return options;
    } catch (IOException e) {
      throw new RuntimeException("Unable to instantiate test options from system property "
          + PROPERTY_DATAFLOW_OPTIONS + ":" + System.getProperty(PROPERTY_DATAFLOW_OPTIONS), e);
    }
  }

  /** Returns the class + method name of the test, or a default name. */
  private static String getAppName() {
    Optional<StackTraceElement> stackTraceElement = findCallersStackTrace();
    if (stackTraceElement.isPresent()) {
      String methodName = stackTraceElement.get().getMethodName();
      String className = stackTraceElement.get().getClassName();
      if (className.contains(".")) {
        className = className.substring(className.lastIndexOf(".") + 1);
      }
      return className + "-" + methodName;
    }
    return "UnitTest";
  }

  /** Returns the {@link StackTraceElement} of the calling class. */
  private static Optional<StackTraceElement> findCallersStackTrace() {
    Iterator<StackTraceElement> elements =
        Iterators.forArray(Thread.currentThread().getStackTrace());
    // First find the TestPipeline class in the stack trace.
    while (elements.hasNext()) {
      StackTraceElement next = elements.next();
      if (TestPipeline.class.getName().equals(next.getClassName())) {
        break;
      }
    }
    // Then find the first instance after that is not the TestPipeline
    while (elements.hasNext()) {
      StackTraceElement next = elements.next();
      if (!TestPipeline.class.getName().equals(next.getClassName())) {
        return Optional.of(next);
      }
    }
    return Optional.absent();
  }
}
