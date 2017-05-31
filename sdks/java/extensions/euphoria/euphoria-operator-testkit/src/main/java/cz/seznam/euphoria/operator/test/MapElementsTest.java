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
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctionEnv;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.operator.test.accumulators.SnapshotProvider;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for operator {@code MapElements}.
 */
@Processing(Processing.Type.ALL)
public class MapElementsTest extends AbstractOperatorTest {

  @Test
  public void testOnTwoPartitions() {
    execute(new AbstractTestCase<Integer, String>() {

      @Override
      protected Dataset<String> getOutput(Dataset<Integer> input) {
        return MapElements.of(input)
            .using((UnaryFunction<Integer, String>) String::valueOf)
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions
            .add(1, 2, 3)
            .add(4, 5, 6, 7)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @SuppressWarnings("unchecked")
      @Override
      public void validate(Partitions<String> partitions) {
        assertEquals(2, partitions.size());
        // the ordering of partitions is undefined here, because
        // the mapping from input to output partition numbers might
        // be random
        assertEquals(Sets.newHashSet(
            Arrays.asList("1", "2", "3"),
            Arrays.asList("4", "5", "6", "7")),
            Sets.newHashSet(partitions.getAll()));
      }
    });
  }

  @Test
  public void testAccumulators() {
    execute(new AbstractTestCase<Integer, Integer>() {

      @Override
      protected Dataset<Integer> getOutput(Dataset<Integer> input) {
        return MapElements.of(input)
            .using((UnaryFunctionEnv<Integer, Integer>) (x, context) -> {
              context.getHistogram("dist").add(x, 1);
              return x;
            })
            .output();
      }

      @Override
      protected Partitions<Integer> getInput() {
        return Partitions.add(1, 2, 3, 1, 2, 2, 10, 20, 10).build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @SuppressWarnings("unchecked")
      @Override
      public void validate(Partitions<Integer> partitions) {
        assertEquals(
            Arrays.asList(1, 2, 3, 1, 2, 2, 10, 20, 10),
            partitions.get(0));
      }

      @Override
      public void validateAccumulators(SnapshotProvider snapshots) {
        Map<Long, Long> hists = snapshots.getHistogramSnapshots().get("dist");
        assertEquals(5, hists.size());
        assertEquals(Long.valueOf(2), hists.get(1L));
        assertEquals(Long.valueOf(3), hists.get(2L));
        assertEquals(Long.valueOf(1), hists.get(3L));
        assertEquals(Long.valueOf(2), hists.get(10L));
        assertEquals(Long.valueOf(1), hists.get(20L));
      }
    });
  }
}
