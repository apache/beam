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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link TestPipeline}. */
public class TestPipelineTest implements Serializable {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  /** Tests related to the creation of a {@link TestPipeline}. */
  @RunWith(JUnit4.class)
  public static class TestPipelineCreationTest {
    @Rule public transient TestRule restoreSystemProperties = new RestoreSystemProperties();
    @Rule public transient ExpectedException thrown = ExpectedException.none();
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCreationUsingDefaults() {
      assertNotNull(pipeline);
      assertNotNull(TestPipeline.create());
    }

    @Test
    public void testCreationNotAsTestRule() {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("@Rule");

      TestPipeline.create().run();
    }

    @Test
    public void testCreationOfPipelineOptions() throws Exception {
      String stringOptions =
          MAPPER.writeValueAsString(
              new String[] {"--runner=org.apache.beam.sdk.testing.CrashingRunner"});
      System.getProperties().put("beamTestPipelineOptions", stringOptions);
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      assertEquals(CrashingRunner.class, options.getRunner());
    }

    @Test
    public void testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase() throws Exception {
      PipelineOptions options = pipeline.getOptions();
      assertThat(
          options.as(ApplicationNameOptions.class).getAppName(),
          startsWith(
              "TestPipelineTest$TestPipelineCreationTest"
                  + "-testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase"));
    }

    @Test
    public void testToString() {
      assertEquals(
          "TestPipeline#TestPipelineTest$TestPipelineCreationTest-testToString",
          pipeline.toString());
    }

    @Test
    public void testRunWithDummyEnvironmentVariableFails() {
      System.getProperties()
          .setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.toString(true));
      pipeline.apply(Create.of(1, 2, 3));

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Cannot call #run");
      pipeline.run();
    }

    /** TestMatcher is a matcher designed for testing matcher serialization/deserialization. */
    public static class TestMatcher extends BaseMatcher<PipelineResult>
        implements SerializableMatcher<PipelineResult> {
      private final UUID uuid = UUID.randomUUID();

      @Override
      public boolean matches(Object o) {
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("%tL", LocalDateTime.now(ZoneId.of("UTC"))));
      }

      @Override
      public boolean equals(@Nullable Object obj) {
        if (!(obj instanceof TestMatcher)) {
          return false;
        }
        TestMatcher other = (TestMatcher) obj;
        return other.uuid.equals(uuid);
      }

      @Override
      public int hashCode() {
        return uuid.hashCode();
      }
    }
  }

  /** Tests for {@link TestPipeline#PROPERTY_BEAM_TEST_PIPELINE_UNIQUE_ROOT_NAMES}. */
  @RunWith(JUnit4.class)
  public static class UniqueRootNamesTest implements Serializable {
    /** Turns unique root names on before the {@link TestPipeline} rule below evaluates. */
    private static final TestRule ENABLE_UNIQUE_ROOT_NAMES =
        (base, description) ->
            new Statement() {
              @Override
              public void evaluate() throws Throwable {
                System.setProperty(
                    TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_UNIQUE_ROOT_NAMES, "true");
                base.evaluate();
              }
            };

    private final transient TestPipeline pipeline = TestPipeline.create();

    @Rule
    public final transient RuleChain rules =
        RuleChain.outerRule(new RestoreSystemProperties())
            .around(ENABLE_UNIQUE_ROOT_NAMES)
            .around(pipeline);

    @Test
    public void testRuleNamesRootAfterTest() {
      String root = pipeline.getRootName();
      assertThat(
          root, matchesPattern("t[0-9]+-TestPipelineTest\\$UniqueRootNamesTest-testRuleNames.*"));
      assertThat(root.length(), lessThanOrEqualTo(TestPipeline.MAX_ROOT_NAME_LENGTH));

      PCollection<Integer> created = pipeline.apply("MyCreate", Create.of(1, 2, 3));
      PCollection<Integer> mapped =
          created.apply(
              "MyMap",
              MapElements.via(
                  new SimpleFunction<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer input) {
                      return input;
                    }
                  }));
      assertThat(created.getName(), startsWith(root + "/"));
      assertThat(mapped.getName(), startsWith(root + "/"));

      List<String> fullNames = new ArrayList<>();
      pipeline.traverseTopologically(
          new Pipeline.PipelineVisitor.Defaults() {
            @Override
            public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
              if (!node.isRootNode()) {
                fullNames.add(node.getFullName());
              }
              return CompositeBehavior.ENTER_TRANSFORM;
            }

            @Override
            public void visitPrimitiveTransform(TransformHierarchy.Node node) {
              fullNames.add(node.getFullName());
            }
          });
      assertThat(fullNames, hasItem(root + "/MyCreate"));
      assertThat(fullNames, hasItem(root + "/MyMap"));
      assertThat(fullNames, everyItem(startsWith(root + "/")));
    }

    @Test
    public void testRootNamesAreUniqueAcrossPipelines() {
      TestPipeline other = TestPipeline.create();
      other.nameRoot("someOtherTest");
      assertThat(other.getRootName(), not(equalTo(pipeline.getRootName())));
      assertThat(other.getRootName(), matchesPattern("t[0-9]+-someOtherTest"));
    }

    @Test
    public void testNameRootIsIgnoredOnceTransformsExist() {
      TestPipeline late = TestPipeline.create();
      late.apply(Create.of(1));
      late.nameRoot("tooLate");
      assertEquals("", late.getRootName());
    }

    @Test
    public void testRootNameForSanitizesAndTruncates() {
      assertEquals("t7-Foo$Bar-baz", TestPipeline.rootNameFor(7, "Foo$Bar-baz"));
      // Characters outside the conservative set, notably the '/' name separator, are replaced.
      assertEquals("t7-a_b_c_d", TestPipeline.rootNameFor(7, "a/b c[d"));
      // Only the human-readable part is truncated; the unique sequence prefix is kept intact.
      String longName = "x".repeat(200);
      String truncated = TestPipeline.rootNameFor(123456, longName);
      assertEquals(TestPipeline.MAX_ROOT_NAME_LENGTH, truncated.length());
      assertThat(truncated, startsWith("t123456-xxx"));
      assertEquals("t1-", TestPipeline.rootNameFor(1, ""));
    }
  }

  /** Tests for the default (unnamed) root. */
  @RunWith(JUnit4.class)
  public static class DefaultRootNameTest {
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testRootIsUnnamedUnlessPropertySet() {
      assertEquals("", pipeline.getRootName());
      PCollection<Integer> created = pipeline.apply("MyCreate", Create.of(1));
      assertThat(created.getName(), startsWith("MyCreate"));
    }
  }

  /**
   * Tests for each condition under which {@link TestPipeline#isStandaloneExecutionRequired()}
   * reports that the pipeline must not be merged with others. Runs under {@link
   * BeamParallelJunit4Runner} because that is what hands the test instance to the rule, which the
   * {@link ExpectedException} detection relies on. The {@link Timeout} rule is deliberate: like
   * many ValidatesRunner suites this class then executes its test bodies on a thread of JUnit's
   * choosing rather than the runner's, and detection must not depend on which thread that is.
   */
  @RunWith(BeamParallelJunit4Runner.class)
  public static class StandaloneExecutionTest {
    @Rule public transient ExpectedException thrown = ExpectedException.none();
    @Rule public transient TestPipeline pipeline = TestPipeline.create();
    @Rule public transient Timeout globalTimeout = Timeout.seconds(60);

    @Test
    public void testPlainTestDoesNotRequireStandalone() {
      assertFalse(pipeline.isStandaloneExecutionRequired());
      pipeline.apply("MyCreate", Create.of(1));
      assertFalse(pipeline.isStandaloneExecutionRequired());
    }

    @Test(expected = IllegalStateException.class)
    public void testExpectedAnnotationRequiresStandalone() {
      assertTrue(pipeline.isStandaloneExecutionRequired());
      throw new IllegalStateException("expected");
    }

    @Test
    @Category(UsesFailureMessage.class)
    public void testUsesFailureMessageCategoryRequiresStandalone() {
      assertTrue(pipeline.isStandaloneExecutionRequired());
    }

    @Test
    @BeamParallelJunit4Runner.SerialTest
    public void testSerialTestMethodRequiresStandalone() {
      assertTrue(pipeline.isStandaloneExecutionRequired());
    }

    @Test
    public void testExpectedExceptionRuleRequiresStandalone() {
      assertFalse(pipeline.isStandaloneExecutionRequired());
      thrown.expect(IllegalStateException.class);
      assertTrue(pipeline.isStandaloneExecutionRequired());
      throw new IllegalStateException("expected");
    }

    @Test
    public void testOptionsMutatedAfterRuleRequiresStandalone() {
      assertFalse(pipeline.isStandaloneExecutionRequired());
      pipeline.getOptions().setJobName("custom-job-name");
      assertTrue(pipeline.isStandaloneExecutionRequired());
    }

    @Test
    public void testRuntimeValueProviderRequiresStandalone() {
      assertFalse(pipeline.isStandaloneExecutionRequired());
      pipeline.newProvider("runtime-value");
      assertTrue(pipeline.isStandaloneExecutionRequired());
    }
  }

  /** Class-level {@link BeamParallelJunit4Runner.SerialTest} also forces standalone execution. */
  @RunWith(JUnit4.class)
  @BeamParallelJunit4Runner.SerialTest
  public static class SerialClassStandaloneExecutionTest {
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testSerialTestClassRequiresStandalone() {
      assertTrue(pipeline.isStandaloneExecutionRequired());
    }
  }

  /** Class-level {@code @Category(UsesFailureMessage.class)} also forces standalone execution. */
  @RunWith(JUnit4.class)
  @Category(UsesFailureMessage.class)
  public static class UsesFailureMessageClassStandaloneExecutionTest {
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testUsesFailureMessageClassRequiresStandalone() {
      assertTrue(pipeline.isStandaloneExecutionRequired());
    }
  }

  /**
   * Tests for {@link TestPipeline}'s detection of missing {@link Pipeline#run()}, or abandoned
   * (dangling) {@link PAssert} or {@link org.apache.beam.sdk.transforms.PTransform} nodes.
   */
  public static class TestPipelineEnforcementsTest implements Serializable {

    private static final List<String> WORDS = Collections.singletonList("hi there");
    private static final String WHATEVER = "expected";
    private static final String P_TRANSFORM = "PTransform";
    private static final String P_ASSERT = "PAssert";

    @SuppressWarnings("UnusedReturnValue")
    private static PCollection<String> addTransform(final PCollection<String> pCollection) {
      return pCollection.apply(
          "Map2",
          MapElements.via(
              new SimpleFunction<String, String>() {

                @Override
                public String apply(final String input) {
                  return WHATEVER;
                }
              }));
    }

    private static PCollection<String> pCollection(final Pipeline pipeline) {
      return pipeline
          .apply("Create", Create.of(WORDS).withCoder(StringUtf8Coder.of()))
          .apply(
              "Map1",
              MapElements.via(
                  new SimpleFunction<String, String>() {

                    @Override
                    public String apply(final String input) {
                      return WHATEVER;
                    }
                  }));
    }

    /** Tests for {@link TestPipeline}s with a non {@link CrashingRunner}. */
    @RunWith(JUnit4.class)
    public static class WithRealPipelineRunner {

      private final transient ExpectedException exception = ExpectedException.none();

      private final transient TestPipeline pipeline = TestPipeline.create();

      @Rule
      public final transient RuleChain chain = RuleChain.outerRule(exception).around(pipeline);

      @Category(NeedsRunner.class)
      @Test
      public void testNormalFlow() throws Exception {
        addTransform(pCollection(pipeline));
        pipeline.run();
      }

      @Category(NeedsRunner.class)
      @Test
      public void testMissingRun() throws Exception {
        exception.expect(TestPipeline.PipelineRunMissingException.class);
        addTransform(pCollection(pipeline));
      }

      @Category(NeedsRunner.class)
      @Test
      public void testMissingRunWithDisabledEnforcement() throws Exception {
        pipeline.enableAbandonedNodeEnforcement(false);
        addTransform(pCollection(pipeline));

        // disable abandoned node detection
      }

      @Category(NeedsRunner.class)
      @Test
      public void testMissingRunAutoAdd() throws Exception {
        pipeline.enableAutoRunIfMissing(true);
        addTransform(pCollection(pipeline));

        // have the pipeline.run() auto-added
      }

      @Category(NeedsRunner.class)
      @Test
      public void testDanglingPTransformValidatesRunner() throws Exception {
        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        exception.expect(TestPipeline.AbandonedNodeException.class);
        exception.expectMessage(P_TRANSFORM);
        // dangling PTransform
        addTransform(pCollection);
      }

      @Category(NeedsRunner.class)
      @Test
      public void testDanglingPTransformNeedsRunner() throws Exception {
        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        exception.expect(TestPipeline.AbandonedNodeException.class);
        exception.expectMessage(P_TRANSFORM);
        // dangling PTransform
        addTransform(pCollection);
      }

      @Category(NeedsRunner.class)
      @Test
      public void testDanglingPAssertValidatesRunner() throws Exception {
        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        exception.expect(TestPipeline.AbandonedNodeException.class);
        exception.expectMessage(P_ASSERT);
        // dangling PAssert
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
      }

      @Category(NeedsRunner.class)
      @Test
      public void testRunWithAdditionalArgsEffective() {
        ArrayList<String> pipelineArgs = new ArrayList<String>();
        pipelineArgs.add("--tempLocation=gs://some-location/");
        pipeline.apply(Create.of("")).apply(new ValidateTempLocation<>());
        PipelineResult.State result =
            pipeline.runWithAdditionalOptionArgs(pipelineArgs).waitUntilFinish();
        assertEquals(PipelineResult.State.DONE, result);
      }

      static class ValidateTempLocation<T> extends PTransform<PCollection<T>, PCollection<T>> {
        @Override
        public void validate(PipelineOptions pipelineOptions) {
          assert !Strings.isNullOrEmpty(pipelineOptions.getTempLocation());
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
          return input;
        }
      }

      /**
       * Tests that a {@link TestPipeline} rule behaves as expected when there is no pipeline usage
       * within a test that has a {@link ValidatesRunner} annotation.
       */
      @Category(NeedsRunner.class)
      @Test
      public void testNoTestPipelineUsedValidatesRunner() {}

      /**
       * Tests that a {@link TestPipeline} rule behaves as expected when there is no pipeline usage
       * present in a test.
       */
      @Test
      public void testNoTestPipelineUsedNoAnnotation() {}
    }

    /** Tests for {@link TestPipeline}s with a {@link CrashingRunner}. */
    @RunWith(JUnit4.class)
    public static class WithCrashingPipelineRunner {

      static {
        System.setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.TRUE.toString());
      }

      private final transient ExpectedException exception = ExpectedException.none();

      private final transient TestPipeline pipeline = TestPipeline.create();

      @Rule
      public final transient RuleChain chain = RuleChain.outerRule(exception).around(pipeline);

      @Test
      public void testNoTestPipelineUsed() {}

      @Test
      public void testMissingRun() throws Exception {
        addTransform(pCollection(pipeline));

        // pipeline.run() is missing, BUT:
        // 1. Neither @ValidatesRunner nor @NeedsRunner are present, AND
        // 2. The runner class is CrashingRunner.class
        // (1) + (2) => we assume this pipeline was never meant to be run, so no exception is
        // thrown on account of the missing run / dangling nodes.
      }
    }
  }

  /** Tests for {@link TestPipeline#newProvider}. */
  @RunWith(JUnit4.class)
  public static class NewProviderTest implements Serializable {
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testNewProvider() {
      ValueProvider<String> foo = pipeline.newProvider("foo");
      ValueProvider<String> foobar =
          ValueProvider.NestedValueProvider.of(foo, input -> input + "bar");

      assertFalse(foo.isAccessible());
      assertFalse(foobar.isAccessible());

      PAssert.that(pipeline.apply("create foo", Create.ofProvider(foo, StringUtf8Coder.of())))
          .containsInAnyOrder("foo");
      PAssert.that(pipeline.apply("create foobar", Create.ofProvider(foobar, StringUtf8Coder.of())))
          .containsInAnyOrder("foobar");

      pipeline.run();
    }
  }
}
