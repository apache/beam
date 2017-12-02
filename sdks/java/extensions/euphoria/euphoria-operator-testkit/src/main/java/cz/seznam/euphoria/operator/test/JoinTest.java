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
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.FullJoin;
import cz.seznam.euphoria.core.client.operator.InnerJoin;
import cz.seznam.euphoria.core.client.operator.LeftJoin;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.RightJoin;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.accumulators.SnapshotProvider;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
          flow.createInput(ListDataSource.of(bounded, getLeftInput()));
      Dataset<RIGHT> right =
          flow.createInput(ListDataSource.of(bounded, getRightInput()));
      return getOutput(left, right);
    }

    protected abstract Dataset<OUT> getOutput(Dataset<LEFT> left, Dataset<RIGHT> right);
    protected abstract List<LEFT> getLeftInput();
    protected abstract List<RIGHT> getRightInput();
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinFullOuter() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return FullJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Optional<Integer> l, Optional<Long> r, Collector<String> c) ->
                c.collect(l.orElse(null) + "+" + r.orElse(null)))
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(
            1, 2, 3, 0,
            4, 3, 2, 1);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(
            11L, 12L,
            13L, 14L, 15L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"), Pair.of(5, "null+15"));
      }
    });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinLeftOuter() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return LeftJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Optional<Long> r, Collector<String> c) ->
                c.collect(l + "+" + r.orElse(null)))
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(
            1, 2, 3, 0,
            4, 3, 2, 1);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(
            11L, 12L,
            13L, 14L, 15L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"));
      }
    });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinRightOuter() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return RightJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Optional<Integer> l, Long r, Collector<String> c) ->
                c.collect(l.orElse(null) + "+" + r))
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(
            1, 2, 3, 0,
            4, 3, 2, 1);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(
            11L, 12L,
            13L, 14L, 15L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"), Pair.of(5, "null+15"));
      }
    });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinInner() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return InnerJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Long r, Collector<String> c) -> {
                c.collect(l + "+" + r);
            })
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(
            1, 2, 3, 0,
            4, 3, 2, 1);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(
            11L, 12L,
            13L, 14L, 15L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(1, "1+11"), Pair.of(1, "1+11"),
            Pair.of(3, "3+13"), Pair.of(3, "3+13"));
      }
    });
  }

  /**
   * Stable windowing for test purposes.
   */
  static class EvenOddWindowing
      implements Windowing<Either<Integer, Long>, IntWindow> {

    @Override
    public Iterable<IntWindow> assignWindowsToElement(
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
  public void windowJoinFullOuter() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return FullJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Optional<Integer> l, Optional<Long> r, Collector<String> c) -> {
              c.collect(l.orElse(null) + "+" + r.orElse(null));
            })
            .windowBy(new EvenOddWindowing())
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(Pair.of(0, "0+null"),
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(6, "6+16"), Pair.of(8, "null+18"),
            Pair.of(1, "1+null"), Pair.of(1, "1+null"), Pair.of(1, "null+11"),
            Pair.of(3, "3+null"), Pair.of(3, "3+null"), Pair.of(3, "null+13"),
            Pair.of(5, "5+null"), Pair.of(5, "null+15"), Pair.of(7, "null+17"));
      }
    });
  }

  @Test
  public void windowJoinLeftOuter() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return LeftJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Integer l, Optional<Long> r, Collector<String> c) -> {
              c.collect(l + "+" + r.orElse(null));
            })
            .windowBy(new EvenOddWindowing())
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(0, "0+null"), Pair.of(2, "2+12"), Pair.of(2, "2+12"),
            Pair.of(4, "4+14"), Pair.of(6, "6+16"), Pair.of(1, "1+null"),
            Pair.of(1, "1+null"), Pair.of(3, "3+null"), Pair.of(3, "3+null"),
            Pair.of(5, "5+null"));
      }
    });
  }

  @Test
  public void windowJoinRightOuter() {
    execute(new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

      @Override
      protected Dataset<Pair<Integer, String>> getOutput(
          Dataset<Integer> left, Dataset<Long> right) {
        return RightJoin.of(left, right)
            .by(e -> e, e -> (int) (e % 10))
            .using((Optional<Integer> l, Long r, Collector<String> c) -> {
              c.collect(l.orElse(null) + "+" + r);
            })
            .windowBy(new EvenOddWindowing())
            .output();
      }

      @Override
      protected List<Integer> getLeftInput() {
        return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
      }

      @Override
      protected List<Long> getRightInput() {
        return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
      }

      @Override
      public List<Pair<Integer, String>> getUnorderedOutput() {
        return Arrays.asList(
            Pair.of(2, "2+12"), Pair.of(2, "2+12"), Pair.of(4, "4+14"),
            Pair.of(6, "6+16"), Pair.of(8, "null+18"), Pair.of(1, "null+11"),
            Pair.of(3, "null+13"), Pair.of(5, "null+15"), Pair.of(7, "null+17"));
      }
    });
  }

  // ~ all of the inputs fall into the same session window (on the same key)
  // ~ we expect the result to reflect this fact
  // ~ note: no early triggering
  @Test
  public void innerJoinOnSessionWindowingNoEarlyTriggering() {
    execute(new JoinTestCase<
        Pair<String, Long>,
        Pair<String, Long>,
        Triple<TimeInterval, String, String>>() {

      @Override
      protected List<Pair<String, Long>> getLeftInput() {
        return Arrays.asList(Pair.of("fi", 1L), Pair.of("fa", 2L));
      }

      @Override
      protected List<Pair<String, Long>> getRightInput() {
        return Arrays.asList(Pair.of("ha", 1L), Pair.of("ho", 4L));
      }

      @Override
      protected Dataset<Triple<TimeInterval, String, String>>
      getOutput(Dataset<Pair<String, Long>> left, Dataset<Pair<String, Long>> right) {
        left = AssignEventTime.of(left).using(Pair::getSecond).output();
        right = AssignEventTime.of(right).using(Pair::getSecond).output();
        Dataset<Pair<String, Triple<TimeInterval, String, String>>> joined =
            InnerJoin.of(left, right)
                .by(p -> "", p -> "")
                .using((Pair<String, Long> l, Pair<String, Long> r, Collector<Triple<TimeInterval, String, String>> c) ->
                    c.collect(Triple.of((TimeInterval) c.getWindow(), l.getFirst(), r.getFirst())))
                .windowBy(Session.of(Duration.ofMillis(10)))
                .output();
        return MapElements.of(joined).using(Pair::getSecond).output();
      }

      @Override
      public List<Triple<TimeInterval, String, String>> getUnorderedOutput() {
        TimeInterval expectedWindow = new TimeInterval(1, 14);
        return Arrays.asList(
            Triple.of(expectedWindow, "fi", "ha"),
            Triple.of(expectedWindow, "fi", "ho"),
            Triple.of(expectedWindow, "fa", "ha"),
            Triple.of(expectedWindow, "fa", "ho"));
      }
    });
  }

  @Test
  public void testJoinAccumulators() {
    execute(new JoinTestCase<
        Pair<String, Long>,
        Pair<String, Long>,
        Triple<TimeInterval, String, String>>() {

      @Override
      protected List<Pair<String, Long>> getLeftInput() {
        return Arrays.asList(Pair.of("fi", 1L), Pair.of("fa", 3L));
      }

      @Override
      protected List<Pair<String, Long>> getRightInput() {
        return Arrays.asList(Pair.of("ha", 1L), Pair.of("ho", 4L));
      }

      @Override
      protected Dataset<Triple<TimeInterval, String, String>>
      getOutput(Dataset<Pair<String, Long>> left, Dataset<Pair<String, Long>> right) {
        left = AssignEventTime.of(left).using(Pair::getSecond).output();
        right = AssignEventTime.of(right).using(Pair::getSecond).output();
        Dataset<Pair<String, Triple<TimeInterval, String, String>>> joined =
            InnerJoin.of(left, right)
                .by(p -> "", p -> "")
                .using((Pair<String, Long> l, Pair<String, Long> r, Collector<Triple<TimeInterval, String, String>> c) -> {
                  TimeInterval window = (TimeInterval) c.getWindow();
                  c.getCounter("cntr").increment(10);
                  c.getHistogram("hist-" + l.getFirst().charAt(1)).add(2345, 8);
                  c.collect(Triple.of(window, l.getFirst(), r.getFirst()));
                })
                .windowBy(Time.of(Duration.ofMillis(3)))
                .output();
        return MapElements.of(joined).using(Pair::getSecond).output();
      }

      @Override
      public List<Triple<TimeInterval, String, String>> getUnorderedOutput() {
        return Arrays.asList(
            Triple.of(new TimeInterval(0, 3), "fi", "ha"),
            Triple.of(new TimeInterval(3, 6), "fa", "ho"));
      }

      @Override
      public void validateAccumulators(SnapshotProvider snapshots) {
        Map<String, Long> counters = snapshots.getCounterSnapshots();
        assertEquals(Long.valueOf(20L), counters.get("cntr"));

        Map<String, Map<Long, Long>> histograms = snapshots.getHistogramSnapshots();
        Map<Long, Long> hist = histograms.get("hist-i");
        assertEquals(1, hist.size());
        assertEquals(Long.valueOf(8), hist.get(2345L));

        hist = histograms.get("hist-a");
        assertEquals(1, hist.size());
        assertEquals(Long.valueOf(8), hist.get(2345L));
      }
    });
  }
}
