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
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Tests for {@link TestPipelineJunit5}. */
public class TestPipelineJunit5Test implements Serializable {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  /** Tests related to the creation of a {@link TestPipeline}. */
  @ExtendWith({RestoreSystemPropertiesJunit5.class})
  public static class TestPipelineCreationTest {

    @RegisterExtension public transient TestPipelineJunit5 pipeline = TestPipelineJunit5.create();

    @Test
    public void testCreationUsingDefaults() {
      assertNotNull(pipeline);
      assertNotNull(TestPipelineJunit5.create());
    }

    @Test
    public void testCreationNotAsTestRule() {
      assertThrows(IllegalStateException.class, () -> TestPipelineJunit5.create().run());
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
    public void testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase() {
      PipelineOptions options = pipeline.getOptions();
      assertThat(
          options.as(ApplicationNameOptions.class).getAppName(),
          startsWith(
              "TestPipelineJunit5Test$TestPipelineCreationTest"
                  + "-testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase"));
    }

    @Test
    public void testToString() {
      assertEquals(
          "TestPipeline#TestPipelineJunit5Test$TestPipelineCreationTest-testToString",
          pipeline.toString());
    }

    @Test
    public void testRunWithDummyEnvironmentVariableFails() {
      System.getProperties()
          .setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.toString(true));
      pipeline.apply(Create.of(1, 2, 3));

      assertThrows(IllegalArgumentException.class, () -> pipeline.run());
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
        description.appendText(String.format("%tL", new Date()));
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

  /**
   * Tests for {@link TestPipeline}'s detection of missing {@link Pipeline#run()}, or abandoned
   * (dangling) {@link PAssert} or {@link org.apache.beam.sdk.transforms.PTransform} nodes.
   */
  public static class TestPipelineEnforcementsTest implements Serializable {

    private static final List<String> WORDS = Collections.singletonList("hi there");
    private static final String WHATEVER = "expected";

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
    public static class WithRealPipelineRunner {

      private final transient TestPipeline pipeline = TestPipelineJunit5.create();

      @Tag("needsRunner")
      @Test
      public void testNormalFlow() {
        addTransform(pCollection(pipeline));
        pipeline.run();
      }

      // TODO Junit5 has no easy way to expect exceptions above test level, the normal way is
      // 'assertThrows'
      // @Category(NeedsRunner.class)
      // @Test
      // public void testMissingRun() throws Exception {
      //   exception.expect(TestPipeline.PipelineRunMissingException.class);
      //   addTransform(pCollection(pipeline));
      // }

      @Tag("needsRunner")
      @Test
      public void testMissingRunWithDisabledEnforcement() {
        pipeline.enableAbandonedNodeEnforcement(false);
        addTransform(pCollection(pipeline));

        // disable abandoned node detection
      }

      @Tag("needsRunner")
      @Test
      public void testMissingRunAutoAdd() {
        pipeline.enableAutoRunIfMissing(true);
        addTransform(pCollection(pipeline));

        // have the pipeline.run() auto-added
      }

      @Tag("needsRunner")
      @Test
      public void testDanglingPTransformValidatesRunner() {
        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);

        assertThrows(
            TestPipeline.AbandonedNodeException.class, () -> pipeline.run().waitUntilFinish());

        // dangling PTransform
        addTransform(pCollection);
      }

      @Tag("needsRunner")
      @Test
      public void testDanglingPTransformNeedsRunner() {
        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);

        assertThrows(
            TestPipeline.AbandonedNodeException.class, () -> pipeline.run().waitUntilFinish());

        // dangling PTransform
        addTransform(pCollection);
      }

      @Tag("needsRunner")
      @Test
      public void testDanglingPAssertValidatesRunner() {
        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        assertThrows(
            TestPipeline.AbandonedNodeException.class, () -> pipeline.run().waitUntilFinish());

        // dangling PAssert
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
      }

      /**
       * Tests that a {@link TestPipeline} rule behaves as expected when there is no pipeline usage
       * within a test that has a {@link ValidatesRunner} annotation.
       */
      @Tag("needsRunner")
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
    public static class WithCrashingPipelineRunner {

      static {
        System.setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.TRUE.toString());
      }

      private final transient TestPipeline pipeline = TestPipelineJunit5.create();

      @Test
      public void testNoTestPipelineUsed() {}

      @Test
      public void testMissingRun() {
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
  public static class NewProviderTest implements Serializable {

    @RegisterExtension public transient TestPipeline pipeline = TestPipelineJunit5.create();

    @Tag("needsRunner")
    @Test
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
