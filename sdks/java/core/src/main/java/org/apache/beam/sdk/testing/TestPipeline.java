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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * remote execution modes.
 *
 * <h3>JUnit 4 Usage</h3>
 *
 * For JUnit 4 tests, use this class as a TestRule:
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
 * <h3>JUnit5 Usage</h3>
 *
 * For JUnit5 tests, use {@link TestPipelineExtension} from the module <code>
 * sdks/java/testing/junit</code> (artifact <code>org.apache.beam:beam-sdks-java-testing-junit
 * </code>):
 *
 * <pre><code>
 * {@literal @ExtendWith}(TestPipelineExtension.class)
 * class MyPipelineTest {
 *   {@literal @Test}
 *   {@literal @Category}(NeedsRunner.class)
 *   void myPipelineTest(TestPipeline pipeline) {
 *     final PCollection&lt;String&gt; pCollection = pipeline.apply(...)
 *     PAssert.that(pCollection).containsInAnyOrder(...);
 *     pipeline.run();
 *   }
 * }
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
  private static final Logger LOG = LoggerFactory.getLogger(TestPipeline.class);

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

  /** */
  static TestPipeline createWithEnforcement() {
    TestPipeline p = create();

    return p;
  }

  public static TestPipeline fromOptions(PipelineOptions options) {
    return new TestPipeline(new TransformHierarchy(ResourceHints.fromOptions(options)), options);
  }

  private TestPipeline(final TransformHierarchy hierarchy, final PipelineOptions options) {
    super(hierarchy, options);
    this.hierarchy = hierarchy;
    this.options = options;
  }

  @Override
  public PipelineOptions getOptions() {
    return this.options;
  }

  // package private for JUnit5 TestPipelineExtension
  void setDeducedEnforcementLevel(Collection<Annotation> annotations) {
    // if the enforcement level has not been set by the user do auto-inference
    if (!enforcement.isPresent()) {

      final boolean annotatedWithNeedsRunner = hasCategory(annotations, NeedsRunner.class);

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

  // package private for JUnit5 TestPipelineExtension
  void afterUserCodeFinished() {
    enforcement.get().afterUserCodeFinished();
  }

  private boolean standaloneExecutionRequired = false;
  private int initialOptionsRevision = -1;

  private static boolean hasCategory(Collection<Annotation> annotations, Class<?> targetCategory) {
    return FluentIterable.from(annotations)
        .filter(Annotations.Predicates.isAnnotationOfType(Category.class))
        .anyMatch(Annotations.Predicates.isCategoryOf(targetCategory, true));
  }

  /**
   * Whether {@code testClass}, a superclass, or an enclosing class carries a {@link Category} that
   * is or extends {@code targetCategory}. A method's {@link Description} only exposes the method's
   * own annotations, so class-level categories have to be looked up here.
   */
  private static boolean hasClassCategory(@Nullable Class<?> testClass, Class<?> targetCategory) {
    for (Class<?> clazz = testClass; clazz != null; clazz = clazz.getEnclosingClass()) {
      Category category = clazz.getAnnotation(Category.class); // @Inherited: covers superclasses
      if (category != null
          && Annotations.Predicates.isCategoryOf(targetCategory, true).apply(category)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Statement apply(final Statement statement, final Description description) {
    return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        String appName = getAppName(description);
        options.as(ApplicationNameOptions.class).setAppName(appName);
        initialOptionsRevision = options.revision();
        if (Boolean.getBoolean(PROPERTY_BEAM_TEST_PIPELINE_UNIQUE_ROOT_NAMES)) {
          nameRoot(appName);
        }

        Collection<Annotation> annotations = description.getAnnotations();
        setDeducedEnforcementLevel(annotations);

        Test testAnnotation = description.getAnnotation(Test.class);
        if ((testAnnotation != null && testAnnotation.expected() != Test.None.class)
            || hasCategory(annotations, UsesFailureMessage.class)
            || hasClassCategory(description.getTestClass(), UsesFailureMessage.class)
            || BeamParallelJunit4Runner.hasSerialAnnotation(annotations)
            || BeamParallelJunit4Runner.isClassMarkedSerial(description.getTestClass())
            || BeamParallelJunit4Runner.isStandaloneRerun(description)) {
          standaloneExecutionRequired = true;
        }

        // statement.evaluate() essentially runs the user code contained in the unit test at hand.
        // Exceptions thrown during the execution of the user's test code will propagate here,
        // unless the user explicitly handles them with a "catch" clause in his code. If the
        // exception is handled by a user's "catch" clause, it does not interrupt the flow, and
        // we move on to invoking the configured enforcements.
        // If the user does not handle a thrown exception, it will propagate here and interrupt
        // the flow, preventing the enforcement(s) from being activated.
        // The motivation for this is avoiding enforcements over faulty pipelines.
        statement.evaluate();
        afterUserCodeFinished();
      }
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Unique root names
  // ---------------------------------------------------------------------------------------------

  /**
   * System property which, when {@code true}, gives every {@link TestPipeline} a unique root
   * transform name of the form {@code t<N>-<test name>} as soon as its JUnit rule is applied. Every
   * transform and {@link PCollection} name in the test is then prefixed by it (for example {@code
   * t12-ParDoTest$BasicTests-testParDo/ParDo(Anonymous)}), so the graphs of different tests never
   * share a name. Runners that merge several test pipelines into one job rely on this; see {@link
   * #getRootName()}.
   */
  public static final String PROPERTY_BEAM_TEST_PIPELINE_UNIQUE_ROOT_NAMES =
      "beamTestPipelineUniqueRootNames";

  /**
   * Upper bound on a root name. Generous enough that real test names ({@code Class$Nested-method},
   * typically 40-80 characters) survive intact, since the method name at the end is the most
   * informative part, while still bounding pathological names.
   */
  @VisibleForTesting static final int MAX_ROOT_NAME_LENGTH = 100;

  private static final AtomicInteger ROOT_NAME_COUNTER = new AtomicInteger(1);

  private final TransformHierarchy hierarchy;
  private String rootName = "";

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns the unique root transform name of this pipeline, or the empty string if it has none
   * (see {@link #PROPERTY_BEAM_TEST_PIPELINE_UNIQUE_ROOT_NAMES}). When non-empty, every full
   * transform name in this pipeline starts with {@code getRootName() + "/"}.
   */
  @Internal
  public String getRootName() {
    return rootName;
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Gives this pipeline a unique root name of the form {@code t<N>-<testName>} (sanitized and
   * truncated). Normally invoked by the JUnit rule or extension; only has an effect if no transform
   * has been applied yet, otherwise a warning is logged and the pipeline stays unnamed.
   */
  @Internal
  public void nameRoot(String testName) {
    String candidate = rootNameFor(ROOT_NAME_COUNTER.getAndIncrement(), testName);
    try {
      hierarchy.setRootName(candidate);
      rootName = candidate;
    } catch (IllegalStateException e) {
      LOG.warn(
          "Not giving the TestPipeline of {} a unique root name: transforms were applied to it"
              + " before the test started. Its transform names stay unprefixed.",
          testName);
    }
  }

  /**
   * Builds {@code t<sequence>-<testName>}. The sequence number alone makes the name unique, so the
   * test name is only a readability aid: it is sanitized to a conservative character set (notably
   * no {@code /}, which separates name segments) and truncated to keep the root within {@link
   * #MAX_ROOT_NAME_LENGTH}.
   */
  @VisibleForTesting
  static String rootNameFor(int sequence, String testName) {
    String prefix = "t" + sequence + "-";
    String hint = testName.replaceAll("[^A-Za-z0-9_.$-]", "_");
    int room = MAX_ROOT_NAME_LENGTH - prefix.length();
    if (hint.length() > room) {
      hint = hint.substring(0, Math.max(0, room));
    }
    return prefix + hint;
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns {@code true} if this {@link TestPipeline} should not be merged into a shared batch
   * pipeline (for example, when the test expects an exception or assertion failure, uses custom
   * per-test option arguments, is marked for serial execution, or is being re-executed after a
   * merged attempt; see {@link StandaloneRerunRequested}).
   */
  @Internal
  public boolean isStandaloneExecutionRequired() {
    return standaloneExecutionRequired
        || (initialOptionsRevision >= 0 && options.revision() != initialOptionsRevision)
        || !providerRuntimeValues.isEmpty()
        || testExpectsException();
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Thrown out of {@link #run()} by a runner that merged this pipeline into a job shared with
   * other tests and could not reach a verdict for this test from that job. The pipeline is not run
   * again by the runner: once the shared job was built from it, this pipeline's graph is no longer
   * guaranteed to be the one the test constructed. Instead the whole test has to be executed again
   * from scratch, with {@linkplain #isStandaloneExecutionRequired() standalone execution} forced so
   * that the fresh pipeline runs as a job of its own. {@link BeamParallelJunit4Runner} does that
   * automatically (once); under any other runner this surfaces as the test's failure.
   *
   * <p>An {@link Error} rather than an exception so that test code wrapping {@code run()} in {@code
   * catch (Exception e)} or {@code assertThrows(Exception.class, ...)} cannot mistake it for the
   * failure it was looking for.
   */
  @Internal
  public static final class StandaloneRerunRequested extends Error {
    public StandaloneRerunRequested(String message) {
      super(message);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Detecting tests that expect run() to fail
  // ---------------------------------------------------------------------------------------------
  //
  // A test that expects run() to throw must not be merged with other tests: it would fail the
  // shared job. Static signals (@Test(expected), categories, serial markers) are read from the
  // JUnit Description in apply(). The dynamic one, an ExpectedException rule on which the test
  // has called expect(...), lives on the test instance, which a TestRule never sees; the runner
  // hands the instance to its TestPipeline rule(s) through attachTestInstance so that it can be
  // inspected here at run() time. The instance is attached to the rule rather than published in a
  // thread-local because the test body does not necessarily run on the thread that created the
  // instance: JUnit's Timeout rule, for one, evaluates the test on a thread of its own.
  // A missed detection is a performance problem (that test and its co-members re-run standalone),
  // never a correctness one.

  /** The test-class instance this rule belongs to, if the runner has told us; see above. */
  private transient @Nullable Object testInstance;

  /**
   * {@code ExpectedException#isAnyExceptionExpected()}, the rule's own (private) notion of whether
   * the test has called {@code expect(...)}. Resolved once; {@code null} if this JUnit version does
   * not have it, in which case such tests are simply not recognized.
   */
  private static final @Nullable Method IS_ANY_EXCEPTION_EXPECTED = lookupIsAnyExceptionExpected();

  private static final AtomicBoolean PROBE_FAILURE_LOGGED = new AtomicBoolean(false);

  /** {@link ExpectedException}-typed fields per test class, resolved once. */
  private static final ConcurrentHashMap<Class<?>, List<Field>> EXPECTED_EXCEPTION_FIELDS =
      new ConcurrentHashMap<>();

  /** {@link TestPipeline}-typed fields per test class, resolved once. */
  private static final ConcurrentHashMap<Class<?>, List<Field>> TEST_PIPELINE_FIELDS =
      new ConcurrentHashMap<>();

  /**
   * Attaches {@code testInstance} to every {@link TestPipeline} held in one of its fields (declared
   * in its class or a superclass), so that those pipelines can inspect the instance's other rules.
   * Called by {@link BeamParallelJunit4Runner} for each freshly created test instance.
   */
  static void attachTestInstance(Object testInstance) {
    for (Field field :
        fieldsOfType(TEST_PIPELINE_FIELDS, testInstance.getClass(), TestPipeline.class)) {
      try {
        Object value = field.get(testInstance);
        if (value instanceof TestPipeline) {
          ((TestPipeline) value).testInstance = testInstance;
        }
      } catch (ReflectiveOperationException | RuntimeException e) {
        if (PROBE_FAILURE_LOGGED.compareAndSet(false, true)) {
          LOG.warn(
              "Failed to read TestPipeline field {} of {}; tests that expect an exception via an"
                  + " ExpectedException rule may be merged into shared test jobs.",
              field.getName(),
              testInstance.getClass().getName(),
              e);
        }
      }
    }
  }

  /**
   * Returns {@code true} if the test this pipeline belongs to has armed an {@link
   * ExpectedException} rule, i.e. it expects to fail. Only {@link ExpectedException} fields of the
   * test instance are consulted; tests that expect failure some other way (for example {@code
   * assertThrows} around {@code run()}) are not recognized, and neither is anything when the test
   * runner has not attached the instance (see {@link #attachTestInstance}).
   */
  @VisibleForTesting
  boolean testExpectsException() {
    Object instance = testInstance;
    if (instance == null || IS_ANY_EXCEPTION_EXPECTED == null) {
      return false;
    }
    for (Field field :
        fieldsOfType(EXPECTED_EXCEPTION_FIELDS, instance.getClass(), ExpectedException.class)) {
      try {
        Object rule = field.get(instance);
        if (rule instanceof ExpectedException
            && Boolean.TRUE.equals(IS_ANY_EXCEPTION_EXPECTED.invoke(rule))) {
          return true;
        }
      } catch (ReflectiveOperationException | RuntimeException e) {
        if (PROBE_FAILURE_LOGGED.compareAndSet(false, true)) {
          LOG.warn(
              "Failed to inspect ExpectedException rule {} of {}; tests that expect an exception"
                  + " via that rule may be merged into shared test jobs.",
              field.getName(),
              instance.getClass().getName(),
              e);
        }
      }
    }
    return false;
  }

  private static @Nullable Method lookupIsAnyExceptionExpected() {
    try {
      Method method = ExpectedException.class.getDeclaredMethod("isAnyExceptionExpected");
      method.setAccessible(true);
      return method;
    } catch (ReflectiveOperationException | RuntimeException e) {
      LOG.warn(
          "Cannot inspect JUnit's ExpectedException rule; tests that expect an exception via that"
              + " rule will not be recognized as such and may be merged into shared test jobs.",
          e);
      return null;
    }
  }

  /**
   * The instance fields of {@code testClass} (or a superclass) whose declared type is assignable to
   * {@code type}, made accessible, cached in {@code cache}.
   */
  private static List<Field> fieldsOfType(
      ConcurrentHashMap<Class<?>, List<Field>> cache, Class<?> testClass, Class<?> type) {
    return cache.computeIfAbsent(
        testClass,
        k -> {
          List<Field> fields = new ArrayList<>();
          for (Class<?> clazz = k;
              clazz != null && clazz != Object.class;
              clazz = clazz.getSuperclass()) {
            for (Field field : clazz.getDeclaredFields()) {
              if (Modifier.isStatic(field.getModifiers())
                  || !type.isAssignableFrom(field.getType())) {
                continue;
              }
              try {
                field.setAccessible(true);
                fields.add(field);
              } catch (RuntimeException e) {
                if (PROBE_FAILURE_LOGGED.compareAndSet(false, true)) {
                  LOG.warn(
                      "Cannot access field {} of {}; tests that expect an exception via an"
                          + " ExpectedException rule may be merged into shared test jobs.",
                      field.getName(),
                      k.getName(),
                      e);
                }
              }
            }
          }
          return fields;
        });
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
    standaloneExecutionRequired = true;
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
    if (options != this.options
        || (initialOptionsRevision >= 0 && options.revision() != initialOptionsRevision)) {
      standaloneExecutionRequired = true;
    }

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
