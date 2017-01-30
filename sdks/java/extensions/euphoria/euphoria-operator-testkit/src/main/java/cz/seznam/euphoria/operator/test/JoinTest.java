/**
 * Copyright 2016 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test operator {@code Join}.
 */
@Processing(Processing.Type.ALL)
public class JoinTest extends AbstractOperatorTest {

  static abstract class JoinTestCase<LEFT, RIGHT, OUT> implements TestCase<OUT> {
    @Override
    public Dataset<OUT> getOutput(Flow flow, boolean bounded) {
      Dataset<LEFT> left =
          flow.createInput(getLeftInput().asListDataSource(bounded));
      Dataset<RIGHT> right =
          flow.createInput(getRightInput().asListDataSource(bounded));
      return getOutput(left, right);
    }

    protected abstract Dataset<OUT> getOutput(
        Dataset<LEFT> left, Dataset<RIGHT> right);

    protected abstract Partitions<LEFT> getLeftInput();
    protected abstract Partitions<RIGHT> getRightInput();
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinOuter() throws Exception {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return Join.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Context<String> c) -> c.collect(l + "+" + r))
            .setPartitioner(e -> e % 2)
            .outer()
            .output();
      }

      @Override
      protected Partitions<Integer> getLeftInput() {
        return Partitions
            .add(1, 2, 3, 0)
            .add(4, 3, 2, 1)
            .build();
      }

      @Override
      protected Partitions<Long> getRightInput() {
        return Partitions
            .add(11L, 12L)
            .add(13L, 14L, 15L)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, String>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"), Pair.of(5, "null+15")),
            second);
      }
    });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinInner() throws Exception {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return Join.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Context<String> c) -> c.collect(l + "+" + r))
            .setPartitioner(e -> e % 2)
            .output();
      }

      @Override
      protected Partitions<Integer> getLeftInput() {
        return Partitions
            .add(1, 2, 3, 0)
            .add(4, 3, 2, 1)
            .build();
      }

      @Override
      protected Partitions<Long> getRightInput() {
        return Partitions
            .add(11L, 12L)
            .add(13L, 14L, 15L)
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, String>> partitions) {
        assertEquals(2, partitions.size());
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13")),
            second);
      }
    });
  }

  /**
   * Stable windowing for test purposes.
   */
  static class EvenOddWindowing
      implements Windowing<Either<Integer, Long>, IntWindow> {

    @Override
    public Set<IntWindow> assignWindowsToElement(
        WindowedElement<?, Either<Integer, Long>> input) {
      int element;
      Either<Integer, Long> unwrapped = input.getElement();
      if (unwrapped.isLeft()) {
        element = unwrapped.left();
      } else {
        element = (int) (long) unwrapped.right();
      }
      final int label = element % 2 == 0 ? 0 : element;
      return Collections.singleton(new IntWindow(label));
    }

    @Override
    public Trigger<IntWindow> getTrigger() {
      return NoopTrigger.get();
    }
  }

  @Test
  public void windowJoin() throws Exception {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return Join.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Context<String> c) -> {
              c.collect(l + "+" + r);
            })
            .setNumPartitions(2)
            .setPartitioner(e -> e % 2)
            .outer()
            .windowBy(new EvenOddWindowing())
            .output();
      }

      @Override
      protected Partitions<Integer> getLeftInput() {
        return Partitions.add(1, 2, 3, 0, 4, 3, 2, 1, 5, 6).build();
      }

      @Override
      protected Partitions<Long> getRightInput() {
        return Partitions.add(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L).build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }

      @Override
      public void validate(Partitions<Pair<Integer, String>> partitions) {
        assertEquals(2, partitions.size());
        // all even elements for one window
        // all odd elements are each element in separate window
        List<Pair<Integer, String>> first = partitions.get(0);
        assertUnorderedEquals(Arrays.asList(Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(6, "6+16"), Pair.of(8, "null+18")), first);
        List<Pair<Integer, String>> second = partitions.get(1);
        assertUnorderedEquals(Arrays.asList(
            Pair.of(1, "1+null"), Pair.of(1, "1+null"), Pair.of(1, "null+11"),
            Pair.of(3, "3+null"), Pair.of(3, "3+null"), Pair.of(3, "null+13"),
            Pair.of(5, "5+null"), Pair.of(5, "null+15"), Pair.of(7, "null+17")),
            second);
      }
    });
  }
}
