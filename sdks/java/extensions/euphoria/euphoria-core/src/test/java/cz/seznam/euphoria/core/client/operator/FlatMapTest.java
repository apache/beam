/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FlatMapTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = FlatMap.named("FlatMap1")
       .of(dataset)
       .using((String s, Collector<String> c) -> c.collect(s))
       .output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    FlatMap map = (FlatMap) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("FlatMap1", map.getName());
    assertNotNull(map.getFunctor());
    assertEquals(mapped, map.output());
    assertNull(map.getEventTimeExtractor());
  }

  @Test
  public void testBuild_EventTimeExtractor() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<BigDecimal> mapped = FlatMap.named("FlatMap2")
        .of(dataset)
        .using((String s, Collector<BigDecimal> c) -> c.collect(null))
        .eventTimeBy(Long::parseLong) // ~ consuming the original input elements
        .output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    FlatMap map = (FlatMap) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("FlatMap2", map.getName());
    assertNotNull(map.getFunctor());
    assertEquals(mapped, map.output());
    assertNotNull(map.getEventTimeExtractor());
  }

  @Test
  public void testBuild_WithCounters() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = FlatMap.named("FlatMap1")
            .of(dataset)
            .using((String s, Collector<String> c) -> {
              c.getCounter("my-counter").increment();
              c.collect(s);
            })
            .output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    FlatMap map = (FlatMap) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("FlatMap1", map.getName());
    assertNotNull(map.getFunctor());
    assertEquals(mapped, map.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = FlatMap.of(dataset)
            .using((String s, Collector<String> c) -> c.collect(s))
            .output();

    FlatMap map = (FlatMap) flow.operators().iterator().next();
    assertEquals("FlatMap", map.getName());
  }

  /**
   * Verify that the number of partitions of the flat map
   * operator's input is preserved in the output.
   */
  @Test
  public void testOutputNumPartitionsIsUnchanged() {
    final int N_PARTITIONS = 78;

    Flow f = Flow.create();
    Dataset<Object> input = Util.createMockDataset(f, N_PARTITIONS);
    assertEquals(N_PARTITIONS, input.getNumPartitions());

    Dataset<Object> output = FlatMap.of(input)
        .using((Object o, Collector<Object> c) -> c.collect(o))
        .output();
    assertEquals(N_PARTITIONS, output.getNumPartitions());
  }
}