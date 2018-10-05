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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Test behavior of operator {@code Union}. */
public class UnionTest {

  @Test
  public void testBuild() {
    final TestPipeline pipeline = OperatorTests.createTestPipeline();
    final Dataset<String> left =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());
    final Dataset<String> right =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());

    final Dataset<String> unioned = Union.named("Union1").of(left, right).output();

    assertTrue(unioned.getProducer().isPresent());
    final Union union = (Union) unioned.getProducer().get();
    assertTrue(union.getName().isPresent());
    assertEquals("Union1", union.getName().get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuild_OneDataSet() {
    final Dataset<String> first = OperatorTests.createMockDataset(TypeDescriptors.strings());
    Union.named("Union1").of(first).output();
  }

  @Test
  public void testBuild_ThreeDataSet() {
    final TestPipeline pipeline = OperatorTests.createTestPipeline();
    final Dataset<String> first =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());
    final Dataset<String> second =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());
    final Dataset<String> third =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());

    final Dataset<String> unioned = Union.named("Union1").of(first, second, third).output();

    assertTrue(unioned.getProducer().isPresent());
    final Union union = (Union) unioned.getProducer().get();
    assertTrue(union.getName().isPresent());
    assertEquals("Union1", union.getName().get());
  }

  @Test
  public void testBuild_ImplicitName() {
    final TestPipeline pipeline = OperatorTests.createTestPipeline();
    final Dataset<String> left =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());
    final Dataset<String> right =
        OperatorTests.createMockDataset(pipeline, TypeDescriptors.strings());
    final Dataset<String> unioned = Union.of(left, right).output();
    assertTrue(unioned.getProducer().isPresent());
    final Union union = (Union) unioned.getProducer().get();
    assertFalse(union.getName().isPresent());
  }
}
