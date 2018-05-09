/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.CountByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.operator.test.junit.Processing.Type;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;


/**
 * Test operator {@code CountByKey}.
 */
@Processing(Type.ALL)
public class CountByKeyTest extends AbstractOperatorTest {

  @Test
  public void testCount() {
    execute(new AbstractTestCase<Integer, Pair<Integer, Long>>() {
      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(Dataset<Integer> input) {
        // ~ use stable event-time watermark
        input = AssignEventTime.of(input).using(e -> 0).output();
        return CountByKey.of(input)
            .keyBy(e -> e)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();
      }

      @Override
      protected List<Integer> getInput() {
        return Arrays.asList(
            1, 2, 3, 4, 5, 6, 7,
            10, 9, 8, 7, 6, 5, 4);
      }

      @Override
      public List<Pair<Integer, Long>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(2, 1L),
            Pair.of(4, 2L),
            Pair.of(6, 2L),
            Pair.of(8, 1L),
            Pair.of(10, 1L),
            Pair.of(1, 1L),
            Pair.of(3, 1L),
            Pair.of(5, 2L),
            Pair.of(7, 2L),
            Pair.of(9, 1L));
      }
    });
  }

  @Test
  public void testWithEventTimeWindow() {
    execute(new AbstractTestCase<Pair<Integer, Long>, Pair<Integer, Long>>() {
      @Override
      protected Dataset<Pair<Integer, Long>> getOutput(
          Dataset<Pair<Integer, Long>> input) {
        input = AssignEventTime.of(input).using(Pair::getSecond).output();
        return CountByKey.of(input)
            .keyBy(Pair::getFirst)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();
      }

      @Override
      protected List<Pair<Integer, Long>> getInput() {
        return Arrays.asList(
                Pair.of(1, 200L), Pair.of(2, 500L), Pair.of(1, 800L),
                Pair.of(3, 1400L), Pair.of(3, 1200L), Pair.of(4, 1800L),
                Pair.of(5, 2100L), Pair.of(5, 2300L), Pair.of(5, 2700L),
                Pair.of(5, 3500L), Pair.of(5, 3300L), Pair.of(6, 3800L),
                Pair.of(7, 4400L), Pair.of(7, 4500L), Pair.of(10, 4600L),
                Pair.of(10, 5100L), Pair.of(9, 5200L), Pair.of(9, 5500L),
                Pair.of(9, 6300L), Pair.of(9, 6700L));
      }

      @Override
      public List<Pair<Integer, Long>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(1, 2L),
            Pair.of(2, 1L),
            Pair.of(3, 2L),
            Pair.of(4, 1L),
            Pair.of(5, 3L),
            Pair.of(5, 2L),
            Pair.of(6, 1L),
            Pair.of(7, 2L),
            Pair.of(10, 1L),
            Pair.of(10, 1L),
            Pair.of(9, 2L),
            Pair.of(9, 2L));
      }
    });
  }
}
