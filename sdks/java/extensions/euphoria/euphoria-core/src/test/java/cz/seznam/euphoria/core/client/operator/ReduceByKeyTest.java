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
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

public class ReduceByKeyTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Time<String> windowing = Time.of(Duration.ofHours(1));
    Dataset<Pair<String, Long>> reduced = ReduceByKey.named("ReduceByKey1")
            .of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(windowing)
            .output();

    assertEquals(flow, reduced.getFlow());
    assertEquals(1, flow.size());

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals(flow, reduce.getFlow());
    assertEquals("ReduceByKey1", reduce.getName());
    assertNotNull(reduce.getKeyExtractor());
    assertNotNull(reduce.valueExtractor);
    assertNotNull(reduce.reducer);
    assertEquals(reduced, reduce.output());
    assertSame(windowing, reduce.getWindowing());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertEquals("ReduceByKey", reduce.getName());
  }

  @Test
  public void testBuild_ReduceBy() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.reducer);
  }


  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .combineBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertTrue(reduce.getWindowing() instanceof Time);
    assertNull(reduce.valueComparator);
  }

  @Test
  public void testBuild_sortedValues() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .withSortedValues(Long::compare)
            .windowBy(Time.of(Duration.ofHours(1)))
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.valueComparator);
  }

  @Test
  public void testBuild_sortedValuesWithNoWindowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Pair<String, Long>> reduced = ReduceByKey.of(dataset)
            .keyBy(s -> s)
            .valueBy(s -> 1L)
            .reduceBy(n -> StreamSupport.stream(n.spliterator(), false).mapToLong(Long::new).sum())
            .withSortedValues(Long::compare)
            .output();

    ReduceByKey reduce = (ReduceByKey) flow.operators().iterator().next();
    assertNotNull(reduce.valueComparator);
  }

}