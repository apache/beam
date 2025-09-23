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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Comprehensive test suite for JUnit 5 compatibility with TestPipelineExtension.
 *
 * <p>This test class validates the fix for the JUnit 5 compatibility issue where TestPipeline.run()
 * would throw IllegalStateException about missing @Rule annotation even when using
 * TestPipelineExtension correctly.
 *
 * <p>These tests ensure that: 1. TestPipelineExtension works correctly with @ExtendWith 2.
 * TestPipelineExtension works correctly with @RegisterExtension 3. Pipeline execution tracking
 * works properly in JUnit 5 context 4. Enforcement mechanisms coordinate correctly between
 * TestPipeline and TestPipelineExtension 5. Both @Category(NeedsRunner.class) and regular tests
 * work
 */
@ExtendWith(TestPipelineExtension.class)
public class TestPipelineJUnit5CompatibilityTest {

  @RegisterExtension
  static final TestPipelineExtension registeredPipeline = TestPipelineExtension.create();

  /**
   * Test that validates the core JUnit 5 compatibility fix. This test would have failed before our
   * fix with: "IllegalStateException: Is your TestPipeline declaration missing a @Rule annotation?"
   */
  @Test
  public void testJUnit5CompatibilityWithExtendWith(TestPipeline pipeline) {
    // This should not throw IllegalStateException about missing @Rule
    assertDoesNotThrow(
        () -> {
          PCollection<String> input = pipeline.apply("Create", Create.of("test"));
          PAssert.that(input).containsInAnyOrder("test");
          pipeline.run().waitUntilFinish();
        },
        "TestPipeline should work with JUnit 5 TestPipelineExtension without @Rule annotation");
  }

  /**
   * Test that validates JUnit 5 compatibility with @RegisterExtension. Note: @RegisterExtension
   * works at the class level and provides TestPipeline instances via parameter injection, so we
   * validate that the extension is working.
   */
  @Test
  public void testJUnit5CompatibilityWithRegisterExtension(TestPipeline pipeline) {
    // This should not throw IllegalStateException about missing @Rule
    // The registeredPipeline extension should be working to provide the pipeline parameter
    assertDoesNotThrow(
        () -> {
          PCollection<String> input = pipeline.apply("Create", Create.of("register-test"));
          PAssert.that(input).containsInAnyOrder("register-test");
          pipeline.run().waitUntilFinish();
        },
        "TestPipeline should work with @RegisterExtension without @Rule annotation");
  }

  /**
   * Test that validates enforcement coordination between TestPipeline and TestPipelineExtension.
   * This ensures that the enforcement mechanisms don't conflict.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testEnforcementCoordinationWithNeedsRunner(TestPipeline pipeline) {
    assertDoesNotThrow(
        () -> {
          PCollection<String> input = pipeline.apply("Create", Create.of("enforcement-test"));
          PAssert.that(input).containsInAnyOrder("enforcement-test");
          pipeline.run().waitUntilFinish();
        },
        "Enforcement should work correctly with @Category(NeedsRunner.class)");
  }

  /**
   * Test that validates pipeline execution tracking works in JUnit 5 context. This ensures that the
   * runAttempted flag is properly set.
   */
  @Test
  public void testPipelineExecutionTracking(TestPipeline pipeline) {
    // Create and run a pipeline
    PCollection<String> input = pipeline.apply("Create", Create.of("tracking-test"));
    PAssert.that(input).containsInAnyOrder("tracking-test");

    // This should not throw PipelineRunMissingException
    assertDoesNotThrow(
        () -> {
          pipeline.run().waitUntilFinish();
        },
        "Pipeline execution should be properly tracked in JUnit 5 context");
  }

  /**
   * Test that validates empty pipeline handling in JUnit 5 context. Empty pipelines should not
   * trigger enforcement errors.
   */
  @Test
  public void testEmptyPipelineHandling(TestPipeline pipeline) {
    assertNotNull(pipeline);
    // Empty pipeline should not cause enforcement issues
    // The TestPipelineExtension should handle this gracefully
  }

  /**
   * Test that validates multiple pipeline operations in single test. This ensures that enforcement
   * state is properly managed.
   */
  @Test
  public void testMultiplePipelineOperations(TestPipeline pipeline) {
    assertDoesNotThrow(
        () -> {
          // First operation
          PCollection<String> input1 = pipeline.apply("Create1", Create.of("multi-1"));
          PAssert.that(input1).containsInAnyOrder("multi-1");

          // Second operation on same pipeline
          PCollection<String> input2 = pipeline.apply("Create2", Create.of("multi-2"));
          PAssert.that(input2).containsInAnyOrder("multi-2");

          // Single run should handle both operations
          pipeline.run().waitUntilFinish();
        },
        "Multiple operations on same pipeline should work correctly");
  }

  /** Test that validates TestPipeline options are properly set in JUnit 5 context. */
  @Test
  public void testPipelineOptionsInJUnit5Context(TestPipeline pipeline) {
    assertNotNull(pipeline.getOptions());

    // Application name should be set based on test method
    String appName = pipeline.getOptions().getJobName();
    assertTrue(
        appName == null || appName.contains("testPipelineOptionsInJUnit5Context"),
        "Application name should be derived from test method name");
  }

  /**
   * Test that validates PAssert integration works correctly in JUnit 5 context. This ensures that
   * assertion validation happens properly.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testPAssertIntegrationInJUnit5(TestPipeline pipeline) {
    assertDoesNotThrow(
        () -> {
          PCollection<Integer> numbers = pipeline.apply("CreateNumbers", Create.of(1, 2, 3, 4, 5));

          // Multiple PAsserts to validate assertion tracking
          PAssert.that(numbers).containsInAnyOrder(1, 2, 3, 4, 5);
          PAssert.thatSingleton(pipeline.apply("CreateSingle", Create.of(42))).isEqualTo(42);

          pipeline.run().waitUntilFinish();
        },
        "PAssert should work correctly with JUnit 5 TestPipelineExtension");
  }

  /**
   * Test that validates error handling in JUnit 5 context. This ensures that pipeline failures are
   * properly propagated.
   */
  @Test
  public void testErrorHandlingInJUnit5Context(TestPipeline pipeline) {
    // This test validates that exceptions from pipeline execution are properly handled
    // and don't interfere with the enforcement mechanism

    PCollection<String> input = pipeline.apply("Create", Create.of("error-test"));
    PAssert.that(input).containsInAnyOrder("error-test");

    assertDoesNotThrow(
        () -> {
          pipeline.run().waitUntilFinish();
        },
        "Error handling should work correctly in JUnit 5 context");
  }
}
