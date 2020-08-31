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
package org.apache.beam.sdk.extensions.euphoria.core.client.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryPredicate;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.TestUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test suite for {@link Split} library. */
@RunWith(JUnit4.class)
public class SplitTest {

  @Test
  public void testBuild() {
    final String opName = "split";
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());

    final Split.Output<String> split =
        Split.named(opName).of(dataset).using((UnaryPredicate<String>) what -> true).output();

    final Filter positive = (Filter) TestUtils.getProducer(split.positive());
    assertNotNull(positive.getPredicate());
    final Filter negative = (Filter) TestUtils.getProducer(split.negative());
    assertNotNull(negative.getPredicate());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final Split.Output<String> split =
        Split.of(dataset).using((UnaryPredicate<String>) what -> true).output();

    final Filter positive = (Filter) TestUtils.getProducer(split.positive());
    assertTrue(positive.getName().isPresent());
    assertEquals(Split.DEFAULT_NAME + Split.POSITIVE_FILTER_SUFFIX, positive.getName().get());
    final Filter negative = (Filter) TestUtils.getProducer(split.negative());
    assertTrue(negative.getName().isPresent());
    assertEquals(Split.DEFAULT_NAME + Split.NEGATIVE_FILTER_SUFFIX, negative.getName().get());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBuild_NegatedPredicate() {
    final PCollection<Integer> dataset = TestUtils.createMockDataset(TypeDescriptors.integers());
    final Split.Output<Integer> split =
        Split.of(dataset).using((UnaryPredicate<Integer>) what -> what % 2 == 0).output();

    final Filter<Integer> oddNumbers = (Filter) TestUtils.getProducer(split.negative());
    assertFalse(oddNumbers.getPredicate().apply(0));
    assertFalse(oddNumbers.getPredicate().apply(2));
    assertFalse(oddNumbers.getPredicate().apply(4));
    assertTrue(oddNumbers.getPredicate().apply(1));
    assertTrue(oddNumbers.getPredicate().apply(3));
    assertTrue(oddNumbers.getPredicate().apply(5));
  }
}
