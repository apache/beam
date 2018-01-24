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
package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.fail;

import com.google.common.collect.FluentIterable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A creator of test pipelines that can be used inside of tests that can be configured to run
 * locally or against a remote pipeline runner.
 *
 * <p>It is recommended to tag hand-selected tests for this purpose using the {@link
 * ValidatesRunner} {@link Category} annotation, as each test run against a pipeline runner will
 * utilize resources of that pipeline runner.
 *
 * <p>In order to run tests on a pipeline runner, the following conditions must be met:
 *
 * <ul>
 * <li>System property "beamTestPipelineOptions" must contain a JSON delimited list of pipeline
 *     options. For example:
 *     <pre>{@code [
 *     "--runner=TestDataflowRunner",
 *     "--project=mygcpproject",
 *     "--stagingLocation=gs://mygcsbucket/path"
 *     ]}</pre>
 *     Note that the set of pipeline options required is pipeline runner specific.
 * <li>Jars containing the SDK and test classes must be available on the classpath.
 * </ul>
 *
 * <p>Use {@link PAssert} for tests, as it integrates with this test harness in both direct and
 * remote execution modes. For example:
 *
 * <pre><code>
 * {@literal @Rule}
 * public final transient TestPipeline p = TestPipeline.create();
 *
 * {@literal @Test}
 * {@literal @Category}(NeedsRunner.class)
 * public void myPipelineTest() throws Exception {
 *   final PCollection&lt;String&gt; pCollection = pipeline.apply(...)
 *   PAssert.that(pCollection).containsInAnyOrder(...);
 *   pipeline.run();
 * }
 * </code></pre>
 *
 * <p>For pipeline runners, it is required that they must throw an {@link AssertionError} containing
 * the message from the {@link PAssert} that failed.</p>
 * <p>See also the <a href="https://beam.apache.org/contribute/testing/">Testing</a> documentation
 * section.</p>
 */
public class TestPipeline extends Pipeline implements TestRule {
  /**
   * @deprecated use TestPipelineHandler flavor instead
   */
  @Deprecated
  public static final String PROPERTY_BEAM_TEST_PIPELINE_OPTIONS = "beamTestPipelineOptions";

  private final TestPipelineHandler<Description> execution;

  /**
   * Creates and returns a new test pipeline.
   *
   * <p>Use {@link PAssert} to add tests, then call {@link Pipeline#run} to execute the pipeline and
   * check the tests.
   */
  public static TestPipeline create() {
    return fromOptions(testingPipelineOptions());
  }

  public static TestPipeline fromOptions(PipelineOptions options) {
    return new TestPipeline(options);
  }

  private TestPipeline(final PipelineOptions options) {
    super(options);
    this.execution = new TestPipelineHandler<Description>(options) {
      @Override
      protected boolean doesNeedRunner(final Description description) {
        return FluentIterable.from(description.getAnnotations())
           .filter(Annotations.Predicates.isAnnotationOfType(Category.class))
           .anyMatch(Annotations.Predicates.isCategoryOf(NeedsRunner.class, true));
      }

      /** Returns the class + method name of the test. */
      @Override
      protected String getAppName(final Description description) {
        final String methodName = description.getMethodName();
        final Class<?> testClass = description.getTestClass();
        return appName(testClass, methodName);
      }
    };
  }

  public <T> ValueProvider<T> newProvider(final T runtimeValue) {
    return execution.newProvider(runtimeValue);
  }

  public TestPipeline enableAbandonedNodeEnforcement(final boolean enable) {
    execution.enableAbandonedNodeEnforcement(enable);
    return this;
  }

  public TestPipeline enableAutoRunIfMissing(final boolean enable) {
    execution.enableAutoRunIfMissing(enable);
    return this;
  }

  @Override
  public PipelineResult run() {
    checkState(
            execution.isEnforcementPresent(),
            "Is your TestPipeline declaration missing a @Rule annotation? Usage: "
                  + "@Rule public final transient TestPipeline pipeline = TestPipeline.create();");
    return super.run();
  }

  public PipelineOptions getOptions() {
    return execution.getOptions();
  }

  @Override
  public Statement apply(final Statement statement, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (final AutoCloseable closeable = execution.start(description)) {
          statement.evaluate();
        }
      }
    };
  }

  @Override
  public String toString() {
    return "TestPipeline#" + getOptions().as(ApplicationNameOptions.class).getAppName();
  }

  /** Creates {@link PipelineOptions} for testing. */
  public static PipelineOptions testingPipelineOptions() {
    return TestPipelineHandler.testingPipelineOptions();
  }

  public static String[] convertToArgs(PipelineOptions options) {
    return TestPipelineHandler.convertToArgs(options);
  }

  /**
   * Verifies all {{@link PAssert PAsserts}} in the pipeline have been executed and were successful.
   *
   * <p>Note this only runs for runners which support Metrics. Runners which do not should verify
   * this in some other way. See: https://issues.apache.org/jira/browse/BEAM-2001</p>
   */
  public static void verifyPAssertsSucceeded(Pipeline pipeline, PipelineResult pipelineResult) {
    try {
      TestPipelineHandler.verifyPAssertsSucceeded(pipeline, pipelineResult);
    } catch (final IllegalStateException ise) { // backward compatibility
      fail(ise.getMessage());
    }
  }
}
