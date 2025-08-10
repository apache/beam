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

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests for {@link TestPipelineExtension} to demonstrate JUnit 5 integration. */
@ExtendWith(TestPipelineExtension.class)
public class TestPipelineExtensionTest {

  @Test
  public void testPipelineInjection(TestPipeline pipeline) {
    // Verify that the pipeline is injected and not null
    assertNotNull(pipeline);
    assertNotNull(pipeline.getOptions());
  }

  @Test
  public void testBasicPipelineExecution(TestPipeline pipeline) {
    // Create a simple pipeline
    PCollection<String> input = pipeline.apply("Create", Create.of("hello", "world"));

    // Use PAssert to verify the output
    PAssert.that(input).containsInAnyOrder("hello", "world");

    // Run the pipeline
    pipeline.run();
  }

  @Test
  public void testEmptyPipeline(TestPipeline pipeline) {
    // Test that an empty pipeline doesn't cause issues
    assertNotNull(pipeline);
    // The extension should handle empty pipelines gracefully
  }
}
