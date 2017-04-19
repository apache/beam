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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.IOChannelUtils;
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
 *     "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowRunner",
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

    private List<TransformHierarchy.Node> runVisitedNodes;

    private final Predicate<TransformHierarchy.Node> isPAssertNode =
        new Predicate<TransformHierarchy.Node>() {

          @Override
          public boolean apply(final TransformHierarchy.Node node) {
            return node.getTransform() instanceof PAssert.GroupThenAssert
                || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton
                || node.getTransform() instanceof PAssert.OneSideInputAssert;
          }
        };

    private static class NodeRecorder extends PipelineVisitor.Defaults {

      private final List<TransformHierarchy.Node> visited = new LinkedList<>();

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
      if (!isEmptyPipeline(pipeline)) {
        if (!runAttempted && !enableAutoRunIfMissing) {
          throw new PipelineRunMissingException(
              "The pipeline has not been run (runner: "
                  + pipeline.getOptions().getRunner().getSimpleName()
                  + ")");

        } else {
          final List<TransformHierarchy.Node> pipelineNodes = recordPipelineNodes(pipeline);
          if (pipelineRunSucceeded() && !visitedAll(pipelineNodes)) {
            final boolean hasDanglingPAssert =
                FluentIterable.from(pipelineNodes)
                              .filter(Predicates.not(Predicates.in(runVisitedNodes)))
                              .anyMatch(isPAssertNode);
            if (hasDanglingPAssert) {
              throw new AbandonedNodeException("The pipeline contains abandoned PAssert(s).");
            } else {
              throw new AbandonedNodeException("The pipeline contains abandoned PTransform(s).");
            }
          }
        }
      }
    }

    private boolean visitedAll(final List<TransformHierarchy.Node> pipelineNodes) {
      return runVisitedNodes.equals(pipelineNodes);
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

  static final String PROPERTY_BEAM_TEST_PIPELINE_OPTIONS = "beamTestPipelineOptions";
  static final String PROPERTY_USE_DEFAULT_DUMMY_RUNNER = "beamUseDummyRunner";
  private static final ObjectMapper MAPPER = new ObjectMapper();

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
    return new TestPipeline(PipelineRunner.fromOptions(options), options);
  }

  private TestPipeline(
      final PipelineRunner<? extends PipelineResult> runner, final PipelineOptions options) {
    super(runner, options);
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

          final boolean crashingRunner =
              CrashingRunner.class.isAssignableFrom(getOptions().getRunner());

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
    checkState(
        enforcement.isPresent(),
        "Is your TestPipeline declaration missing a @Rule annotation? Usage: "
        + "@Rule public final transient TestPipeline pipeline = TestPipeline.create();");

    final PipelineResult pipelineResult;
    try {
      enforcement.get().beforePipelineExecution();
      pipelineResult = super.run();
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

  /**
   * Enables the abandoned node detection. Abandoned nodes are <code>PTransforms</code>, <code>
   * PAsserts</code> included, that were not executed by the pipeline runner. Abandoned nodes are
   * most likely to occur due to the one of the following scenarios:
   * <ul>
   * <li>Lack of a <code>pipeline.run()</code> statement at the end of a test.
   * <li>Addition of PTransforms after the pipeline has already run.
   * </ul>
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
    return "TestPipeline#" + getOptions().as(ApplicationNameOptions.class).getAppName();
  }

  /** Creates {@link PipelineOptions} for testing. */
  public static PipelineOptions testingPipelineOptions() {
    try {
      @Nullable
      String beamTestPipelineOptions = System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);

      PipelineOptions options =
          Strings.isNullOrEmpty(beamTestPipelineOptions)
              ? PipelineOptionsFactory.create()
              : PipelineOptionsFactory.fromArgs(
                      MAPPER.readValue(beamTestPipelineOptions, String[].class))
                  .as(TestPipelineOptions.class);

      options.as(ApplicationNameOptions.class).setAppName(getAppName());
      // If no options were specified, set some reasonable defaults
      if (Strings.isNullOrEmpty(beamTestPipelineOptions)) {
        // If there are no provided options, check to see if a dummy runner should be used.
        String useDefaultDummy = System.getProperty(PROPERTY_USE_DEFAULT_DUMMY_RUNNER);
        if (!Strings.isNullOrEmpty(useDefaultDummy) && Boolean.valueOf(useDefaultDummy)) {
          options.setRunner(CrashingRunner.class);
        }
      }
      options.setStableUniqueNames(CheckEnabled.ERROR);

      IOChannelUtils.registerIOFactoriesAllowOverride(options);
      FileSystems.setDefaultConfigInWorkers(options);
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

  public static String[] convertToArgs(PipelineOptions options) {
    try {
      byte[] opts = MAPPER.writeValueAsBytes(options);

      JsonParser jsonParser = MAPPER.getFactory().createParser(opts);
      TreeNode node = jsonParser.readValueAsTree();
      ObjectNode optsNode = (ObjectNode) node.get("options");
      ArrayList<String> optArrayList = new ArrayList<>();
      Iterator<Entry<String, JsonNode>> entries = optsNode.fields();
      while (entries.hasNext()) {
        Entry<String, JsonNode> entry = entries.next();
        if (entry.getValue().isTextual()) {
          optArrayList.add("--" + entry.getKey() + "=" + entry.getValue().asText());
        } else {
          optArrayList.add("--" + entry.getKey() + "=" + entry.getValue());
        }
      }
      return optArrayList.toArray(new String[optArrayList.size()]);
    } catch (IOException e) {
      throw new IllegalStateException(e);
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
    Optional<StackTraceElement> firstInstanceAfterTestPipeline = Optional.absent();
    while (elements.hasNext()) {
      StackTraceElement next = elements.next();
      if (!TestPipeline.class.getName().equals(next.getClassName())) {
        if (!firstInstanceAfterTestPipeline.isPresent()) {
          firstInstanceAfterTestPipeline = Optional.of(next);
        }
        try {
          Class<?> nextClass = Class.forName(next.getClassName());
          for (Method method : nextClass.getMethods()) {
            if (method.getName().equals(next.getMethodName())) {
              if (method.isAnnotationPresent(org.junit.Test.class)) {
                return Optional.of(next);
              } else if (method.isAnnotationPresent(org.junit.Before.class)) {
                break;
              }
            }
          }
        } catch (Throwable t) {
          break;
        }
      }
    }
    return firstInstanceAfterTestPipeline;
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
