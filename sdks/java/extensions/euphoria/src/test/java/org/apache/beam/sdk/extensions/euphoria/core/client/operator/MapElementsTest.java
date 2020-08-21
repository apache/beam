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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypePropagationAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator MapElement. */
@RunWith(JUnit4.class)
public class MapElementsTest {

  @Test
  public void testBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped = MapElements.named("Map1").of(dataset).using(s -> s).output();
    final MapElements map = (MapElements) TestUtils.getProducer(mapped);
    assertTrue(map.getName().isPresent());
    assertEquals("Map1", map.getName().get());
    assertNotNull(map.getMapper());
  }

  @Test
  public void testBuild_WithCounters() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped =
        MapElements.named("Map1")
            .of(dataset)
            .using(
                (input, context) -> {
                  // use simple counter
                  context.getCounter("my-counter").increment();

                  return input.toLowerCase();
                })
            .output();

    final MapElements map = (MapElements) TestUtils.getProducer(mapped);
    assertTrue(map.getName().isPresent());
    assertEquals("Map1", map.getName().get());
    assertNotNull(map.getMapper());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped = MapElements.of(dataset).using(s -> s).output();
    final MapElements map = (MapElements) TestUtils.getProducer(mapped);
    assertFalse(map.getName().isPresent());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testTypePropagation() {
    final PCollection<Integer> input = TestUtils.createMockDataset(TypeDescriptors.integers());
    final TypeDescriptor<String> outputType = TypeDescriptors.strings();
    final PCollection<String> mapped =
        MapElements.named("Int2Str").of(input).using(String::valueOf, outputType).output();
    final MapElements map = (MapElements) TestUtils.getProducer(mapped);
    TypePropagationAssert.assertOperatorTypeAwareness(map, outputType);
  }
}
