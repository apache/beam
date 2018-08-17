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
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.operator.test.accumulators.SnapshotProvider;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code FlatMap}.
 */
@Processing(Processing.Type.ALL)
public class FlatMapTest extends AbstractOperatorTest {

  @Test
  public void testExplodeOnTwoPartitions() {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return FlatMap.of(input)
            .using((Integer e, Collector<Integer> c) -> {
              for (int i = 1; i <= e; i++) {
                c.collect(i);
              }
            })
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3)
            .add(4, 3, 2, 1)
            .build();
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(2, partitions.size());
        List<Integer> first = partitions.get(0);
        assertEquals(Arrays.asList(1, 1, 2, 1, 2, 3), first);
        List<Integer> second = partitions.get(1);
        assertEquals(Arrays.asList(1, 2, 3, 4, 1, 2, 3, 1, 2, 1), second);
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

    });
  }

  @Test
  public void testCounterTest() {
    execute(new AbstractTestCase<Integer, Integer>() {
      @Override
      protected Partitions<Integer> getInput() {
        return Partitions.add(1, 2, 3, 4, 5, 6)
            .add(0, 10, 20)
            .build();
      }

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return FlatMap.of(input).using(
            (UnaryFunctor<Integer, Integer>) (elem, collector) -> {
              collector.getCounter("input").increment();
              collector.getCounter("sum").increment(elem);
              collector.collect(elem * elem);
            })
            .output();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(Arrays.asList(1, 4, 9, 16, 25, 36), partitions.get(0));
        assertEquals(Arrays.asList(0, 100, 400), partitions.get(1));
      }

      @Override
      public void validateAccumulators(SnapshotProvider snapshots) {
        Map<String, Long> counters = snapshots.getCounterSnapshots();
        assertEquals(Long.valueOf(9L), counters.get("input"));
        assertEquals(Long.valueOf(51L), counters.get("sum"));
      }
    });
  }

}
