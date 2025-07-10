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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 *   <li>System property "beamTestPipelineOptions" must contain a JSON delimited list of pipeline
 *       options. For example:
 *       <pre>{@code [
 *     "--runner=TestDataflowRunner",
 *     "--project=mygcpproject",
 *     "--stagingLocation=gs://mygcsbucket/path"
 * ]}</pre>
 *       Note that the set of pipeline options required is pipeline runner specific.
 *   <li>Jars containing the SDK and test classes must be available on the classpath.
 * </ul>
 *
 * <p>Use {@link PAssert} for tests, as it integrates with this test harness in both direct and
 * remote execution modes. For example:
 *
 * <pre><code>
 * {@literal @Rule}
 *  public final transient TestPipeline p = TestPipeline.create();
 *
 * {@literal @Test}
 * {@literal @Category}(NeedsRunner.class)
 *  public void myPipelineTest() throws Exception {
 *    final PCollection&lt;String&gt; pCollection = pipeline.apply(...)
 *    PAssert.that(pCollection).containsInAnyOrder(...);
 *    pipeline.run();
 *  }
 * </code></pre>
 *
 * <p>For pipeline runners, it is required that they must throw an {@link AssertionError} containing
 * the message from the {@link PAssert} that failed.
 *
 * <p>See also the <a
 * href="https://beam.apache.org/documentation/pipelines/test-your-pipeline/">Testing</a>
 * documentation section.
 */
public class TestPipeline extends Pipeline implements TestRule {

  private final PipelineOptions options;

  private static class PipelineRunEnforcement {

    @SuppressWarnings("WeakerAccess")
    protected boolean enableAutoRunIfMissing;

    protected final Pipeline pipeline;

    protected boolean runAttempted;

    private PipelineRunEnforcement(final Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    protected void enableAutoRunIfMissing(final boolean enable) {
      enableAutoRunIfMissing = enable;
    }

    protected void beforePipelineExecution() {
      runAttempted = true;
    }

    protected void afterPipelineExecution() {}

    protected void afterUserCodeFinished() {
      if (!runAttempted && enableAutoRunIfMissing) {
        pipeline.run().waitUntilFinish();
      }
    }
  }

  private static class PipelineAbandonedNodeEnforcement extends PipelineRunEnforcement {

    // Null until the pipeline has been run
    private @MonotonicNonNull List<TransformHierarchy.Node> runVisitedNodes;

    private final Predicate<TransformHierarchy.Node> isPAssertNode =
        node ->
            node.getTransform() instanceof PAssert.GroupThenAssert
                || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton
                || node.getTransform() instanceof PAssert.OneSideInputAssert;

    private static class NodeRecorder extends PipelineVisitor.Defaults {

      private final List<TransformHierarchy.Node> visited = new ArrayList<>();

      @Override
      public void leaveCompositeTransform(final TransformHierarchy.Node node) {
        visited.add(node);
      }

      @Override
      public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
        visited.add(node);
      }
    }

    private PipelineAbandonedNodeEnforcement(final TestPipeline pipeline) {
      super(pipeline);
      runVisitedNodes = null;
    }

    private List<TransformHierarchy.Node> recordPipelineNodes(final Pipeline pipeline) {
      final NodeRecorder nodeRecorder = new NodeRecorder();
      pipeline.traverseTopologically(nodeRecorder);
      return nodeRecorder.visited;
    }

    private boolean isEmptyPipeline(final Pipeline pipeline) {
      final IsEmptyVisitor isEmptyVisitor = new IsEmptyVisitor();
      pipeline.traverseTopologically(isEmptyVisitor);
      return isEmptyVisitor.isEmpty();
    }

    private void verifyPipelineExecution() {
      if (isEmptyPipeline(pipeline)) {
        return;
      }

      if (!runAttempted && !enableAutoRunIfMissing) {
        throw new PipelineRunMissingException("The pipeline has not been run.");
      }

      if (!pipelineRunSucceeded()) {
        return; // this method is to protect against spurious success, so failure is fine
      }

      final List<TransformHierarchy.Node> runVisitedNodes =
          checkStateNotNull(
              this.runVisitedNodes,
              "Internal error: non-empty pipeline has been visited but still no runVisitedNodes");
      final List<TransformHierarchy.Node> pipelineNodes = recordPipelineNodes(pipeline);
      if (runVisitedNodes.equals(pipelineNodes)) {
        return;
      }

      final boolean hasDanglingPAssert =
          pipelineNodes.stream()
              .filter(Predicates.not(Predicates.in(runVisitedNodes)))
              .anyMatch(isPAssertNode);

      if (hasDanglingPAssert) {
        throw new AbandonedNodeException("The pipeline contains abandoned PAssert(s).");
      } else {
        throw new AbandonedNodeException("The pipeline contains abandoned PTransform(s).");
      }
    }

    private boolean pipelineRunSucceeded() {
      return runVisitedNodes != null;
    }

    @Override
    protected void afterPipelineExecution() {
      runVisitedNodes = recordPipelineNodes(pipeline);
      super.afterPipelineExecution();
    }

    @Override
    protected void afterUserCodeFinished() {
      super.afterUserCodeFinished();
      verifyPipelineExecution();
    }
  }

  /**
   * An exception thrown in case an abandoned {@link org.apache.beam.sdk.transforms.PTransform} is
   * detected, that is, a {@link org.apache.beam.sdk.transforms.PTransform} that has not been run.
   */
  public static class AbandonedNodeException extends RuntimeException {

    AbandonedNodeException(final String msg) {
      super(msg);
    }
  }

  /** An exception thrown in case a test finishes without invoking {@link Pipeline#run()}. */
  public static class PipelineRunMissingException extends RuntimeException {

    PipelineRunMissingException(final String msg) {
      super(msg);
    }
  }

  /** System property used to set {@link TestPipelineOptions}. */
  public static final String PROPERTY_BEAM_TEST_PIPELINE_OPTIONS = "beamTestPipelineOptions";

  static final String PROPERTY_USE_DEFAULT_DUMMY_RUNNER = "beamUseDummyRunner";

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<? extends PipelineRunEnforcement> enforcement = Optional.absent();

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
    this.options = options;
  }

  @Override
  public PipelineOptions getOptions() {
    return this.options;
  }

  @Override
  public Statement apply(final Statement statement, final Description description) {
    return new Statement() {

      private void setDeducedEnforcementLevel() {
        // if the enforcement level has not been set by the user do auto-inference
        if (!enforcement.isPresent()) {

          final boolean annotatedWithNeedsRunner =
              FluentIterable.from(description.getAnnotations())
                  .filter(Annotations.Predicates.isAnnotationOfType(Category.class))
                  .anyMatch(Annotations.Predicates.isCategoryOf(NeedsRunner.class, true));

          final boolean crashingRunner = CrashingRunner.class.isAssignableFrom(options.getRunner());

          checkState(
              !(annotatedWithNeedsRunner && crashingRunner),
              "The test was annotated with a [@%s] / [@%s] while the runner "
                  + "was set to [%s]. Please re-check your configuration.",
              NeedsRunner.class.getSimpleName(),
              ValidatesRunner.class.getSimpleName(),
              CrashingRunner.class.getSimpleName());

          enableAbandonedNodeEnforcement(annotatedWithNeedsRunner || !crashingRunner);
        }
      }

      @Override
      public void evaluate() throws Throwable {
        options.as(ApplicationNameOptions.class).setAppName(getAppName(description));

        setDeducedEnforcementLevel();

        // statement.evaluate() essentially runs the user code contained in the unit test at hand.
        // Exceptions thrown during the execution of the user's test code will propagate here,
        // unless the user explicitly handles them with a "catch" clause in his code. If the
        // exception is handled by a user's "catch" clause, is does not interrupt the flow and
        // we move on to invoking the configured enforcements.
        // If the user does not handle a thrown exception, it will propagate here and interrupt
        // the flow, preventing the enforcement(s) from being activated.
        // The motivation for this is avoiding enforcements over faulty pipelines.
        statement.evaluate();
        enforcement.get().afterUserCodeFinished();
      }
    };
  }

  /**
   * Runs this {@link TestPipeline}, unwrapping any {@code AssertionError} that is raised during
   * testing.
   */
  @Override
  public PipelineResult run() {
    return run(getOptions());
  }

  /**
   * Runs this {@link TestPipeline} with additional cmd pipeline option args.
   *
   * <p>This is useful when using {@link PipelineOptions#as(Class)} directly introduces circular
   * dependency.
   *
   * <p>Most of logic is similar to {@link #testingPipelineOptions}.
   */
  public PipelineResult runWithAdditionalOptionArgs(List<String> additionalArgs) {
    try {
      String beamTestPipelineOptions = System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS, "");
      List<String> args = new ArrayList<>();
      if (!beamTestPipelineOptions.isEmpty()) {
        args.addAll(MAPPER.readValue(beamTestPipelineOptions, List.class));
      }
      args.addAll(additionalArgs);
      String[] newArgs = Iterables.toArray(args, String.class);
      PipelineOptions newOptions =
          PipelineOptionsFactory.fromArgs(newArgs).as(TestPipelineOptions.class);

      // If no options were specified, set some reasonable defaults
      if (beamTestPipelineOptions.isEmpty()) {
        // If there are no provided options, check to see if a dummy runner should be used.
        String useDefaultDummy = System.getProperty(PROPERTY_USE_DEFAULT_DUMMY_RUNNER);
        if (!Strings.isNullOrEmpty(useDefaultDummy) && Boolean.valueOf(useDefaultDummy)) {
          newOptions.setRunner(CrashingRunner.class);
        }
      }
      newOptions.setStableUniqueNames(CheckEnabled.ERROR);

      FileSystems.registerFileSystemsOnce(options);
      return run(newOptions);
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to instantiate test options from system property "
              + PROPERTY_BEAM_TEST_PIPELINE_OPTIONS
              + ":"
              + System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS),
          e);
    }
  }

  /** Like {@link #run} but with the given potentially modified options. */
  @Override
  public PipelineResult run(PipelineOptions options) {
    checkState(
        enforcement.isPresent(),
        "Is your TestPipeline declaration missing a @Rule annotation? Usage: "
            + "@Rule public final transient TestPipeline pipeline = TestPipeline.create();");

    final PipelineResult pipelineResult;
    try {
      enforcement.get().beforePipelineExecution();
      PipelineOptions updatedOptions =
          MAPPER.convertValue(MAPPER.valueToTree(options), PipelineOptions.class);
      updatedOptions
          .as(TestValueProviderOptions.class)
          .setProviderRuntimeValues(StaticValueProvider.of(providerRuntimeValues));
      pipelineResult = super.run(updatedOptions);
      verifyPAssertsSucceeded(this, pipelineResult);
    } catch (RuntimeException exc) {
      Throwable cause = exc.getCause();
      if (cause instanceof AssertionError) {
        throw (AssertionError) cause;
      } else {
        throw exc;
      }
    }

    // If we reach this point, the pipeline has been run and no exceptions have been thrown during
    // its execution.
    enforcement.get().afterPipelineExecution();
    return pipelineResult;
  }

  /** Implementation detail of {@link #newProvider}, do not use. */
  @Internal
  public interface TestValueProviderOptions extends PipelineOptions {
    ValueProvider<Map<String, Object>> getProviderRuntimeValues();

    void setProviderRuntimeValues(ValueProvider<Map<String, Object>> runtimeValues);
  }

  /**
   * Returns a new {@link ValueProvider} that is inaccessible before {@link #run}, but will be
   * accessible while the pipeline runs.
   */
  public <T> ValueProvider<T> newProvider(T runtimeValue) {
    String uuid = UUID.randomUUID().toString();
    if (runtimeValue != null) {
      providerRuntimeValues.put(uuid, runtimeValue);
    }
    return ValueProvider.NestedValueProvider.of(
        options.as(TestValueProviderOptions.class).getProviderRuntimeValues(),
        new GetFromRuntimeValues<T>(uuid));
  }

  private final Map<String, Object> providerRuntimeValues = Maps.newHashMap();

  private static class GetFromRuntimeValues<T>
      implements SerializableFunction<Map<String, Object>, T> {
    private final String key;

    private GetFromRuntimeValues(String key) {
      this.key = key;
    }

    @Override
    public T apply(Map<String, Object> input) {
      return (T) input.get(key);
    }
  }

  /**
   * Enables the abandoned node detection. Abandoned nodes are <code>PTransforms</code>, <code>
   * PAsserts</code> included, that were not executed by the pipeline runner. Abandoned nodes are
   * most likely to occur due to the one of the following scenarios:
   *
   * <ul>
   *   <li>Lack of a <code>pipeline.run()</code> statement at the end of a test.
   *   <li>Addition of PTransforms after the pipeline has already run.
   * </ul>
   *
   * Abandoned node detection is automatically enabled when a real pipeline runner (i.e. not a
   * {@link CrashingRunner}) and/or a {@link NeedsRunner} or a {@link ValidatesRunner} annotation
   * are detected.
   */
  public TestPipeline enableAbandonedNodeEnforcement(final boolean enable) {
    enforcement =
        enable
            ? Optional.of(new PipelineAbandonedNodeEnforcement(this))
            : Optional.of(new PipelineRunEnforcement(this));

    return this;
  }

  /**
   * If enabled, a <code>pipeline.run()</code> statement will be added automatically in case it is
   * missing in the test.
   */
  public TestPipeline enableAutoRunIfMissing(final boolean enable) {
    enforcement.get().enableAutoRunIfMissing(enable);
    return this;
  }

  @Override
  public String toString() {
    return "TestPipeline#" + options.as(ApplicationNameOptions.class).getAppName();
  }

  /** Creates {@link PipelineOptions} for testing. */
  public static PipelineOptions testingPipelineOptions() {
    try {
      String beamTestPipelineOptions = System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS, "");

      PipelineOptions options =
          beamTestPipelineOptions.isEmpty()
              ? PipelineOptionsFactory.create()
              : PipelineOptionsFactory.fromArgs(
                      MAPPER.readValue(beamTestPipelineOptions, String[].class))
                  .as(TestPipelineOptions.class);

      // If no options were specified, set some reasonable defaults
      if (beamTestPipelineOptions.isEmpty()) {
        // If there are no provided options, check to see if a dummy runner should be used.
        String useDefaultDummy = System.getProperty(PROPERTY_USE_DEFAULT_DUMMY_RUNNER);
        if (!Strings.isNullOrEmpty(useDefaultDummy) && Boolean.valueOf(useDefaultDummy)) {
          options.setRunner(CrashingRunner.class);
        }
      }
      options.setStableUniqueNames(CheckEnabled.ERROR);

      FileSystems.registerFileSystemsOnce(options);
      return options;
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to instantiate test options from system property "
              + PROPERTY_BEAM_TEST_PIPELINE_OPTIONS
              + ":"
              + System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS),
          e);
    }
  }

  /** Returns the class + method name of the test. */
  private String getAppName(Description description) {
    String methodName = description.getMethodName();
    Class<?> testClass = description.getTestClass();
    @Nullable Class<?> enclosingClass = testClass.getEnclosingClass();
    if (enclosingClass != null) {
      return String.format(
          "%s$%s-%s", enclosingClass.getSimpleName(), testClass.getSimpleName(), methodName);
    } else {
      return String.format("%s-%s", testClass.getSimpleName(), methodName);
    }
  }

  /**
   * Verifies all {{@link PAssert PAsserts}} in the pipeline have been executed and were successful.
   *
   * <p>Note this only runs for runners which support Metrics. Runners which do not should verify
   * this in some other way. See: https://issues.apache.org/jira/browse/BEAM-2001
   */
  public static void verifyPAssertsSucceeded(Pipeline pipeline, PipelineResult pipelineResult) {
    if (MetricsEnvironment.isMetricsSupported()) {
      long expectedNumberOfAssertions = (long) PAssert.countAsserts(pipeline);

      long successfulAssertions = 0;
      Iterable<MetricResult<Long>> successCounterResults =
          pipelineResult
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.named(PAssert.class, PAssert.SUCCESS_COUNTER))
                      .build())
              .getCounters();
      for (MetricResult<Long> counter : successCounterResults) {
        if (counter.getAttempted() > 0) {
          successfulAssertions++;
        }
      }

      assertThat(
          String.format(
              "Expected %d successful assertions, but found %d.",
              expectedNumberOfAssertions, successfulAssertions),
          successfulAssertions,
          is(expectedNumberOfAssertions));
    }
  }

  private static class IsEmptyVisitor extends PipelineVisitor.Defaults {
    private boolean empty = true;

    public boolean isEmpty() {
      return empty;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      empty = false;
    }
  }
}
