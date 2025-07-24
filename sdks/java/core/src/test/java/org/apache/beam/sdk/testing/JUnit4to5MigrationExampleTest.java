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

/**
 * This file demonstrates how to migrate from JUnit 4 to JUnit 5 for Apache Beam pipeline tests.
 *
 * <h2>JUnit 4 Version</h2>
 *
 * <pre><code>
 * import org.apache.beam.sdk.testing.TestPipeline;
 * import org.apache.beam.sdk.testing.PAssert;
 * import org.apache.beam.sdk.transforms.Create;
 * import org.apache.beam.sdk.values.PCollection;
 * import org.junit.Rule;
 * import org.junit.Test;
 * import org.junit.experimental.categories.Category;
 *
 * public class MyPipelineTest {
 *   {@literal @}Rule
 *   public final transient TestPipeline pipeline = TestPipeline.create();
 *
 *   {@literal @}Test
 *   {@literal @}Category(NeedsRunner.class)
 *   public void testMyPipeline() {
 *     PCollection&lt;String&gt; input = pipeline.apply("Create", Create.of("hello", "world"));
 *     PAssert.that(input).containsInAnyOrder("hello", "world");
 *     pipeline.run();
 *   }
 *
 *   {@literal @}Test
 *   public void testEmptyPipeline() {
 *     // Empty pipeline test
 *   }
 * }
 * </code></pre>
 *
 * <h2>JUnit 5 Version</h2>
 *
 * <pre><code>
 * import org.apache.beam.sdk.testing.TestPipeline;
 * import org.apache.beam.sdk.testing.TestPipelineExtension;
 * import org.apache.beam.sdk.testing.PAssert;
 * import org.apache.beam.sdk.transforms.Create;
 * import org.apache.beam.sdk.values.PCollection;
 * import org.junit.jupiter.api.Test;
 * import org.junit.jupiter.api.extension.ExtendWith;
 * import org.junit.experimental.categories.Category;
 *
 * {@literal @}ExtendWith(TestPipelineExtension.class)
 * class MyPipelineTest {
 *
 *   {@literal @}Test
 *   {@literal @}Category(NeedsRunner.class)
 *   void testMyPipeline(TestPipeline pipeline) {
 *     PCollection&lt;String&gt; input = pipeline.apply("Create", Create.of("hello", "world"));
 *     PAssert.that(input).containsInAnyOrder("hello", "world");
 *     pipeline.run();
 *   }
 *
 *   {@literal @}Test
 *   void testEmptyPipeline(TestPipeline pipeline) {
 *     // Empty pipeline test - pipeline is automatically injected
 *   }
 * }
 * </code></pre>
 *
 * <h2>Key Differences</h2>
 *
 * <ul>
 *   <li><strong>No {@literal @}Rule</strong>: Replace {@literal @}Rule with
 *       {@literal @}ExtendWith(TestPipelineExtension.class)
 *   <li><strong>Parameter Injection</strong>: TestPipeline is injected as a method parameter
 *   <li><strong>Import Changes</strong>: Use JUnit 5 imports instead of JUnit 4
 *   <li><strong>Method Visibility</strong>: Test methods can be package-private in JUnit 5
 *   <li><strong>Category Support</strong>: {@literal @}Category annotations still work the same way
 * </ul>
 *
 * <h2>Benefits of JUnit 5</h2>
 *
 * <ul>
 *   <li><strong>Parameter Injection</strong>: Cleaner test methods with dependency injection
 *   <li><strong>Better Assertions</strong>: More expressive assertion methods
 *   <li><strong>Dynamic Tests</strong>: Support for creating tests at runtime
 *   <li><strong>Extension Model</strong>: More powerful and flexible extension system
 * </ul>
 *
 * <p>The TestPipelineExtension provides the same functionality as the JUnit 4 TestRule, including
 * automatic pipeline lifecycle management, abandoned node detection, and enforcement of pipeline
 * execution.
 */
public class JUnit4to5MigrationExampleTest {
  // This class exists only for documentation purposes
}
