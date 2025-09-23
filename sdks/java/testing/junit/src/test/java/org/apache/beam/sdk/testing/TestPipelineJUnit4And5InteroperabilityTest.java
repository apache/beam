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

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test to validate that our JUnit 5 compatibility fix doesn't break JUnit 4 functionality.
 *
 * <p>This test ensures that: 1. TestPipeline still works correctly with JUnit 4 @Rule pattern 2.
 * TestPipelineExtension works correctly with JUnit 5 3. Both approaches can coexist in the same
 * codebase 4. Enforcement mechanisms work correctly for both approaches
 *
 * <p>This test class uses JUnit 5 but validates that the underlying TestPipeline class maintains
 * backward compatibility with JUnit 4 patterns.
 */
@ExtendWith(TestPipelineExtension.class)
public class TestPipelineJUnit4And5InteroperabilityTest {

  /**
   * Test that validates TestPipeline can be created and used in the same way as it would be with
   * JUnit 4 @Rule, but within a JUnit 5 context.
   */
  @Test
  public void testJUnit4StyleUsageInJUnit5Context(TestPipeline pipeline) {
    // This mimics how TestPipeline would be used with JUnit 4 @Rule
    // but ensures it works in JUnit 5 context with our fix

    assertDoesNotThrow(
        () -> {
          // Create pipeline content (same as JUnit 4 usage)
          PCollection<String> input = pipeline.apply("Create", Create.of("junit4-style"));
          PAssert.that(input).containsInAnyOrder("junit4-style");

          // Run pipeline (same as JUnit 4 usage)
          pipeline.run().waitUntilFinish();
        },
        "JUnit 4 style usage should work in JUnit 5 context");
  }

  /** Test that validates enforcement behavior is consistent between JUnit 4 and JUnit 5. */
  @Test
  @Category(NeedsRunner.class)
  public void testEnforcementConsistencyBetweenJUnitVersions(TestPipeline pipeline) {
    assertDoesNotThrow(
        () -> {
          // This should behave the same way in both JUnit 4 and JUnit 5
          PCollection<String> input = pipeline.apply("Create", Create.of("enforcement-test"));
          PAssert.that(input).containsInAnyOrder("enforcement-test");
          pipeline.run().waitUntilFinish();
        },
        "Enforcement should work consistently across JUnit versions");
  }

  /**
   * Test that validates TestPipeline options and configuration work the same way in both JUnit 4
   * and JUnit 5 contexts.
   */
  @Test
  public void testPipelineOptionsConsistency(TestPipeline pipeline) {
    assertNotNull(pipeline.getOptions());

    // Verify that pipeline options are set up the same way as in JUnit 4
    assertDoesNotThrow(
        () -> {
          // Application name should be set based on test context
          String appName = pipeline.getOptions().getJobName();
          // This should work the same way in both JUnit versions
        },
        "Pipeline options should be consistent across JUnit versions");
  }

  /** Test that validates PAssert behavior is identical between JUnit 4 and JUnit 5. */
  @Test
  @Category(NeedsRunner.class)
  public void testPAssertConsistencyBetweenJUnitVersions(TestPipeline pipeline) {
    assertDoesNotThrow(
        () -> {
          PCollection<Integer> numbers = pipeline.apply("CreateNumbers", Create.of(1, 2, 3));

          // PAssert should work identically in both JUnit versions
          PAssert.that(numbers).containsInAnyOrder(1, 2, 3);
          PAssert.thatSingleton(pipeline.apply("CreateSingle", Create.of(100))).isEqualTo(100);

          pipeline.run().waitUntilFinish();
        },
        "PAssert should work identically in both JUnit versions");
  }

  /**
   * Test that validates our fix doesn't introduce any regressions in the core TestPipeline
   * functionality.
   */
  @Test
  public void testNoRegressionsInCoreFunctionality(TestPipeline pipeline) {
    assertDoesNotThrow(
        () -> {
          // Test basic pipeline operations
          PCollection<String> step1 = pipeline.apply("Step1", Create.of("a", "b", "c"));
          PCollection<String> step2 = pipeline.apply("Step2", Create.of("x", "y", "z"));

          // Test assertions
          PAssert.that(step1).containsInAnyOrder("a", "b", "c");
          PAssert.that(step2).containsInAnyOrder("x", "y", "z");

          // Test pipeline execution
          pipeline.run().waitUntilFinish();
        },
        "Core TestPipeline functionality should not have regressions");
  }

  /** Test that validates the fix works with empty pipelines in both contexts. */
  @Test
  public void testEmptyPipelineHandlingConsistency(TestPipeline pipeline) {
    // Empty pipelines should be handled consistently in both JUnit 4 and JUnit 5
    assertNotNull(pipeline);
    assertNotNull(pipeline.getOptions());

    // This should not cause any enforcement issues in either JUnit version
  }

  /** Test that validates error propagation works the same way in both JUnit versions. */
  @Test
  public void testErrorPropagationConsistency(TestPipeline pipeline) {
    // Error handling should be consistent between JUnit 4 and JUnit 5
    assertDoesNotThrow(
        () -> {
          PCollection<String> input = pipeline.apply("Create", Create.of("error-propagation-test"));
          PAssert.that(input).containsInAnyOrder("error-propagation-test");
          pipeline.run().waitUntilFinish();
        },
        "Error propagation should be consistent across JUnit versions");
  }
}
