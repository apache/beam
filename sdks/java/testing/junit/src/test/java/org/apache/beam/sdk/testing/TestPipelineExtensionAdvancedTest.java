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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Advanced tests for {@link TestPipelineExtension} demonstrating comprehensive functionality. */
@ExtendWith(TestPipelineExtension.class)
public class TestPipelineExtensionAdvancedTest {

  @Test
  public void testApplicationNameIsSet(TestPipeline pipeline) {
    String appName = pipeline.getOptions().as(ApplicationNameOptions.class).getAppName();
    assertNotNull(appName);
    assertTrue(appName.contains("TestPipelineExtensionAdvancedTest"));
    assertTrue(appName.contains("testApplicationNameIsSet"));
  }

  @Test
  public void testMultipleTransforms(TestPipeline pipeline) {
    PCollection<String> input = pipeline.apply("Create", Create.of("a", "b", "c"));

    PCollection<String> output =
        input.apply(
            "Transform",
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(c.element().toUpperCase());
                  }
                }));

    PAssert.that(output).containsInAnyOrder("A", "B", "C");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWithValidatesRunnerCategory(TestPipeline pipeline) {
    // This test demonstrates that @Category annotations work with JUnit 5
    PCollection<Integer> numbers = pipeline.apply("Create", Create.of(1, 2, 3, 4, 5));
    PAssert.that(numbers).containsInAnyOrder(1, 2, 3, 4, 5);
    pipeline.run();
  }

  @Test
  public void testPipelineInstancesAreIsolated(TestPipeline pipeline1) {
    // Each test method gets its own pipeline instance
    assertNotNull(pipeline1);
    pipeline1.apply("Create", Create.of("test"));
    // Don't run the pipeline - test should still pass due to auto-run functionality
  }

  @Test
  public void testAnotherPipelineInstance(TestPipeline pipeline2) {
    // This should be a different instance from the previous test
    assertNotNull(pipeline2);
    PCollection<String> data = pipeline2.apply("Create", Create.of("different", "data"));
    PAssert.that(data).containsInAnyOrder("different", "data");
    pipeline2.run();
  }
}
