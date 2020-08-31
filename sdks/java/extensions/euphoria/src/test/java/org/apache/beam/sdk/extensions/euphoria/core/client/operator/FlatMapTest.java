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
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test operator FlatMap. */
@RunWith(JUnit4.class)
public class FlatMapTest {

  @Test
  public void testBuild() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped =
        FlatMap.named("FlatMap1")
            .of(dataset)
            .using((String s, Collector<String> c) -> c.collect(s))
            .output();
    final FlatMap map = (FlatMap) TestUtils.getProducer(mapped);
    assertTrue(map.getName().isPresent());
    assertEquals("FlatMap1", map.getName().get());
    assertNotNull(map.getFunctor());
    assertFalse(map.getEventTimeExtractor().isPresent());
  }

  @Test
  public void testBuild_EventTimeExtractor() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<BigDecimal> mapped =
        FlatMap.named("FlatMap2")
            .of(dataset)
            .using((String s, Collector<BigDecimal> c) -> c.collect(null))
            .eventTimeBy(Long::parseLong) // ~ consuming the original input elements
            .output();
    final FlatMap map = (FlatMap) TestUtils.getProducer(mapped);
    assertTrue(map.getName().isPresent());
    assertEquals("FlatMap2", map.getName().get());
    assertNotNull(map.getFunctor());
    assertTrue(map.getEventTimeExtractor().isPresent());
  }

  @Test
  public void testBuild_WithCounters() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped =
        FlatMap.named("FlatMap1")
            .of(dataset)
            .using(
                (String s, Collector<String> c) -> {
                  c.getCounter("my-counter").increment();
                  c.collect(s);
                })
            .output();
    final FlatMap map = (FlatMap) TestUtils.getProducer(mapped);
    assertTrue(map.getName().isPresent());
    assertEquals("FlatMap1", map.getName().get());
    assertNotNull(map.getFunctor());
  }

  @Test
  public void testBuild_ImplicitName() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped =
        FlatMap.of(dataset).using((String s, Collector<String> c) -> c.collect(s)).output();
    final FlatMap map = (FlatMap) TestUtils.getProducer(mapped);
    assertFalse(map.getName().isPresent());
  }

  @Test
  public void testBuild_TimestampSkew() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped =
        FlatMap.of(dataset)
            .using((String s, Collector<String> c) -> c.collect(s))
            .eventTimeBy(in -> System.currentTimeMillis(), Duration.millis(100))
            .output();
    final FlatMap map = (FlatMap) TestUtils.getProducer(mapped);
    assertEquals(100, map.getAllowedTimestampSkew().getMillis());
  }

  @Test
  public void testBuild_NoTimestampSkew() {
    final PCollection<String> dataset = TestUtils.createMockDataset(TypeDescriptors.strings());
    final PCollection<String> mapped =
        FlatMap.of(dataset)
            .using((String s, Collector<String> c) -> c.collect(s))
            .eventTimeBy(in -> System.currentTimeMillis())
            .output();
    final FlatMap map = (FlatMap) TestUtils.getProducer(mapped);
    assertEquals(Long.MAX_VALUE, map.getAllowedTimestampSkew().getMillis());
  }
}
