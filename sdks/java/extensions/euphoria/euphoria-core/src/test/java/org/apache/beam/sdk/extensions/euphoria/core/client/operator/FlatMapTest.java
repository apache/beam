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

import java.math.BigDecimal;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

/** Test operator FlatMap. */
public class FlatMapTest {

  @Test
  public void testBuild() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> mapped =
        FlatMap.named("FlatMap1")
            .of(dataset)
            .using((String s, Collector<String> c) -> c.collect(s))
            .output();
    assertTrue(mapped.getProducer().isPresent());
    final FlatMap map = (FlatMap) mapped.getProducer().get();
    assertTrue(map.getName().isPresent());
    assertEquals("FlatMap1", map.getName().get());
    assertNotNull(map.getFunctor());
    assertFalse(map.getEventTimeExtractor().isPresent());
  }

  @Test
  public void testBuild_EventTimeExtractor() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<BigDecimal> mapped =
        FlatMap.named("FlatMap2")
            .of(dataset)
            .using((String s, Collector<BigDecimal> c) -> c.collect(null))
            .eventTimeBy(Long::parseLong) // ~ consuming the original input elements
            .output();
    assertTrue(mapped.getProducer().isPresent());
    final FlatMap map = (FlatMap) mapped.getProducer().get();
    assertTrue(map.getName().isPresent());
    assertEquals("FlatMap2", map.getName().get());
    assertNotNull(map.getFunctor());
    assertTrue(map.getEventTimeExtractor().isPresent());
  }

  @Test
  public void testBuild_WithCounters() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> mapped =
        FlatMap.named("FlatMap1")
            .of(dataset)
            .using(
                (String s, Collector<String> c) -> {
                  c.getCounter("my-counter").increment();
                  c.collect(s);
                })
            .output();
    assertTrue(mapped.getProducer().isPresent());
    final FlatMap map = (FlatMap) mapped.getProducer().get();
    assertTrue(map.getName().isPresent());
    assertEquals("FlatMap1", map.getName().get());
    assertNotNull(map.getFunctor());
  }

  @Test
  public void testBuild_ImplicitName() {
    final Dataset<String> dataset = OperatorTests.createMockDataset(TypeDescriptors.strings());
    final Dataset<String> mapped =
        FlatMap.of(dataset).using((String s, Collector<String> c) -> c.collect(s)).output();
    assertTrue(mapped.getProducer().isPresent());
    final FlatMap map = (FlatMap) mapped.getProducer().get();
    assertFalse(map.getName().isPresent());
  }
}
