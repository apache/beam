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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/** Tests capabilities of {@link Windowing}. */
@Processing(Processing.Type.ALL)
public class WindowingTest extends AbstractOperatorTest {

  static final AtomicBoolean ON_CLEAR_VALIDATED = new AtomicBoolean(false);

  @Test
  public void consecutiveWindowingTest_ReduceByKey() {
    execute(
        new AbstractTestCase<Triple<Instant, Type, String>, Triple<Instant, Type, Long>>() {

          @Override
          protected Dataset<Triple<Instant, Type, Long>> getOutput(
              Dataset<Triple<Instant, Type, String>> input) {

            input = AssignEventTime.of(input).using(t -> t.getFirst().toEpochMilli()).output();
            Dataset<ComparablePair<Type, String>> distinct =
                Distinct.of(input)
                    .mapped(t -> new ComparablePair<>(t.getSecond(), t.getThird()))
                    .windowBy(Time.of(Duration.ofHours(1)))
                    .output();

            Dataset<Pair<Type, Long>> reduced =
                ReduceByKey.of(distinct)
                    .keyBy(ComparablePair::getFirst)
                    .valueBy(p -> 1L)
                    .combineBy(Sums.ofLongs())
                    .windowBy(Time.of(Duration.ofHours(1)))
                    .output();

            // extract window end timestamp
            return FlatMap.of(reduced)
                .using(
                    (Pair<Type, Long> p, Collector<Triple<Instant, Type, Long>> ctx) -> {
                      long windowEnd = ((TimeInterval) ctx.getWindow()).getEndMillis();
                      ctx.collect(
                          Triple.of(Instant.ofEpochMilli(windowEnd), p.getFirst(), p.getSecond()));
                    })
                .output();
          }

          @Override
          protected List<Triple<Instant, Type, String>> getInput() {
            return Arrays.asList(
                // first window
                Triple.of(Instant.parse("2016-12-19T10:10:00.000Z"), Type.FRUIT, "banana"),
                Triple.of(Instant.parse("2016-12-19T10:20:00.000Z"), Type.FRUIT, "banana"),
                Triple.of(Instant.parse("2016-12-19T10:25:00.000Z"), Type.FRUIT, "orange"),
                Triple.of(Instant.parse("2016-12-19T10:35:00.000Z"), Type.FRUIT, "apple"),
                Triple.of(Instant.parse("2016-12-19T10:40:00.000Z"), Type.VEGETABLE, "carrot"),
                Triple.of(Instant.parse("2016-12-19T10:45:00.000Z"), Type.VEGETABLE, "cucumber"),
                Triple.of(Instant.parse("2016-12-19T10:45:00.000Z"), Type.VEGETABLE, "cucumber"),
                Triple.of(Instant.parse("2016-12-19T10:50:00.000Z"), Type.VEGETABLE, "apple"),

                // second window
                Triple.of(Instant.parse("2016-12-19T11:15:00.000Z"), Type.FRUIT, "banana"),
                Triple.of(Instant.parse("2016-12-19T11:15:00.000Z"), Type.FRUIT, "orange"),
                Triple.of(Instant.parse("2016-12-19T11:20:00.000Z"), Type.VEGETABLE, "carrot"),
                Triple.of(Instant.parse("2016-12-19T11:25:00.000Z"), Type.VEGETABLE, "carrot"));
          }

          @Override
          public List<Triple<Instant, Type, Long>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.FRUIT, 3L),
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.VEGETABLE, 3L),
                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.FRUIT, 2L),
                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.VEGETABLE, 1L));
          }
        });
  }

  @Test
  public void consecutiveWindowingTest_ReduceStateByKey() {
    execute(
        new AbstractTestCase<Triple<Instant, Type, String>, Triple<Instant, Type, Long>>() {

          @Override
          protected Dataset<Triple<Instant, Type, Long>> getOutput(
              Dataset<Triple<Instant, Type, String>> input) {
            // distinct implemented using raw ReduceStateByKey
            input = AssignEventTime.of(input).using(t -> t.getFirst().toEpochMilli()).output();
            Dataset<Pair<ComparablePair<Type, String>, Object>> pairs =
                ReduceStateByKey.of(input)
                    .keyBy(t -> new ComparablePair<>(t.getSecond(), t.getThird()))
                    .valueBy(t -> null)
                    .stateFactory(DistinctState::new)
                    .mergeStatesBy((t, os) -> {})
                    .windowBy(Time.of(Duration.ofHours(1)))
                    .output();

            Dataset<ComparablePair<Type, String>> distinct =
                MapElements.of(pairs).using(Pair::getFirst).output();

            Dataset<Pair<Type, Long>> reduced =
                ReduceByKey.of(distinct)
                    .keyBy(ComparablePair::getFirst)
                    .valueBy(p -> 1L)
                    .combineBy(Sums.ofLongs())
                    .windowBy(Time.of(Duration.ofHours(1)))
                    .output();

            // extract window timestamp
            return FlatMap.of(reduced)
                .using(
                    (Pair<Type, Long> p, Collector<Triple<Instant, Type, Long>> ctx) -> {
                      long windowEnd = ((TimeInterval) ctx.getWindow()).getEndMillis();
                      ctx.collect(
                          Triple.of(Instant.ofEpochMilli(windowEnd), p.getFirst(), p.getSecond()));
                    })
                .output();
          }

          @Override
          protected List<Triple<Instant, Type, String>> getInput() {
            return Arrays.asList(
                // first window
                Triple.of(Instant.parse("2016-12-19T10:10:00.000Z"), Type.FRUIT, "banana"),
                Triple.of(Instant.parse("2016-12-19T10:20:00.000Z"), Type.FRUIT, "banana"),
                Triple.of(Instant.parse("2016-12-19T10:25:00.000Z"), Type.FRUIT, "orange"),
                Triple.of(Instant.parse("2016-12-19T10:35:00.000Z"), Type.FRUIT, "apple"),
                Triple.of(Instant.parse("2016-12-19T10:40:00.000Z"), Type.VEGETABLE, "carrot"),
                Triple.of(Instant.parse("2016-12-19T10:45:00.000Z"), Type.VEGETABLE, "cucumber"),
                Triple.of(Instant.parse("2016-12-19T10:45:00.000Z"), Type.VEGETABLE, "cucumber"),
                Triple.of(Instant.parse("2016-12-19T10:50:00.000Z"), Type.VEGETABLE, "apple"),

                // second window
                Triple.of(Instant.parse("2016-12-19T11:15:00.000Z"), Type.FRUIT, "banana"),
                Triple.of(Instant.parse("2016-12-19T11:15:00.000Z"), Type.FRUIT, "orange"),
                Triple.of(Instant.parse("2016-12-19T11:20:00.000Z"), Type.VEGETABLE, "carrot"),
                Triple.of(Instant.parse("2016-12-19T11:25:00.000Z"), Type.VEGETABLE, "carrot"));
          }

          @SuppressWarnings("unchecked")
          @Override
          public List<Triple<Instant, Type, Long>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.FRUIT, 3L),
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.VEGETABLE, 3L),
                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.FRUIT, 2L),
                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.VEGETABLE, 1L));
          }
        });
  }

  /**
   * Validates a trigger's #onClear method operates in the right context of merged windows.
   *
   * <p>A trigger's lifecycle is guaranteed only on stream processing; batch processing has more
   * freedom and doesn't necessarily invoke the trigger#onClear method.
   */
  @Processing(Processing.Type.UNBOUNDED)
  @Test
  public void testSessionWindowingTriggerStateConsistency() {
    ON_CLEAR_VALIDATED.set(false);
    execute(
        new AbstractTestCase<Pair<Instant, String>, Triple<Instant, Instant, Integer>>(3) {
          @SuppressWarnings("unchecked")
          @Override
          protected Dataset<Triple<Instant, Instant, Integer>> getOutput(
              Dataset<Pair<Instant, String>> input) {
            CSession windowing =
                new CSession(Duration.ofMinutes(5)) {
                  @Override
                  public TriggerResult onElement(
                      long time, TimeInterval window, TriggerContext ctx) {
                    ValueStorage<Integer> str = ctx.getValueStorage(CSession.TR_STATE);
                    str.set(str.get() + 1);
                    return super.onElement(time, window, ctx);
                  }

                  @Override
                  public void onMerge(TimeInterval window, TriggerContext.TriggerMergeContext ctx) {
                    ctx.mergeStoredState(CSession.TR_STATE);
                    super.onMerge(window, ctx);
                  }

                  @Override
                  public TriggerResult onTimer(long time, TimeInterval window, TriggerContext ctx) {
                    assertTrState(window, ctx);
                    return super.onTimer(time, window, ctx);
                  }

                  @Override
                  public void onClear(TimeInterval window, TriggerContext ctx) {
                    // ~ 7 minutes is the size of the final target window
                    if (window.getDurationMillis() == Duration.ofMinutes(7).toMillis()) {
                      assertTrState(window, ctx);
                      if (!ON_CLEAR_VALIDATED.compareAndSet(false, true)) {
                        fail("!ON_CLEAR_VALIDATED!");
                      }
                    }
                    ctx.getValueStorage(TR_STATE).clear();
                    super.onClear(window, ctx);
                  }

                  private void assertTrState(TimeInterval window, TriggerContext ctx) {
                    ValueStorage<Integer> str = ctx.getValueStorage(CSession.TR_STATE);
                    assertEquals(3, str.get().intValue());
                  }
                };

            input = AssignEventTime.of(input).using(t -> t.getFirst().toEpochMilli()).output();
            Dataset<Pair<String, Integer>> pairs =
                ReduceByKey.of(input)
                    .keyBy(e -> "")
                    .valueBy(e -> 1)
                    .combineBy(Sums.ofInts())
                    .windowBy(windowing)
                    .output();

            // extract window timestamp
            return FlatMap.of(pairs)
                .using(
                    (Pair<String, Integer> in,
                        Collector<Triple<Instant, Instant, Integer>> out) -> {
                      long windowBegin = ((TimeInterval) out.getWindow()).getStartMillis();
                      long windowEnd = ((TimeInterval) out.getWindow()).getEndMillis();
                      out.collect(
                          Triple.of(
                              Instant.ofEpochMilli(windowBegin),
                              Instant.ofEpochMilli(windowEnd),
                              in.getSecond()));
                    })
                .output();
          }

          @Override
          protected List<Pair<Instant, String>> getInput() {
            return Arrays.asList(
                Pair.of(Instant.parse("2016-12-19T10:10:00.000Z"), "foo"),
                Pair.of(Instant.parse("2016-12-19T10:11:00.000Z"), "foo"),
                Pair.of(Instant.parse("2016-12-19T10:12:00.000Z"), "foo"));
          }

          @Override
          public List<Triple<Instant, Instant, Integer>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(
                    Instant.parse("2016-12-19T10:10:00.000Z"),
                    Instant.parse("2016-12-19T10:17:00.000Z"),
                    3));
          }
        });
    assertEquals(true, ON_CLEAR_VALIDATED.get());
  }

  @Test
  public void testTimeWindowingElementsAtBoundaries() {
    execute(
        new AbstractTestCase<Integer, Pair<TimeInterval, Integer>>() {

          @Override
          protected Dataset<Pair<TimeInterval, Integer>> getOutput(Dataset<Integer> input) {
            // interpret each input element as time and just count number of
            // elements inside each window
            Dataset<Integer> timed = AssignEventTime.of(input).using(e -> e * 1000L).output();
            Dataset<Integer> counts =
                ReduceWindow.of(timed)
                    .valueBy(e -> 1)
                    .combineBy(Sums.ofInts())
                    .windowBy(Time.of(Duration.ofHours(1)))
                    .output();

            return Util.extractWindow(counts);
          }

          @Override
          protected List<Integer> getInput() {
            return Arrays.asList(0, 1, 3599, 3599, 3600, 3600, 7199);
          }

          @SuppressWarnings("unchecked")
          @Override
          public List<Pair<TimeInterval, Integer>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(new TimeInterval(0, 3600000), 4),
                Pair.of(new TimeInterval(3600000, 7200000), 3));
          }
        });
  }

  /**
   * Just simple enum to be used during testing.
   */
  public enum Type {
    FRUIT,
    VEGETABLE
  }

  private static class DistinctState implements State<Object, Object> {

    private final ValueStorage<Object> storage;

    DistinctState(StateContext context, Collector<Object> collector) {
      this.storage =
          context
              .getStorageProvider()
              .getValueStorage(ValueStorageDescriptor.of("element", Object.class, null));
    }

    @Override
    public void add(Object element) {
      storage.set(element);
    }

    @Override
    public void flush(Collector<Object> context) {
      context.collect(storage.get());
    }

    @Override
    public void close() {
      storage.clear();
    }
  }

  /**
   * Pair of items where both items implement {@link Comparable}.
   * @param <T0> first item type
   * @param <T1> second item type
   */
  public static class ComparablePair<T0 extends Comparable<T0>, T1 extends Comparable<T1>>
      implements Comparable<ComparablePair<T0, T1>> {

    private final T0 first;
    private final T1 second;

    ComparablePair(T0 first, T1 second) {
      this.first = first;
      this.second = second;
    }

    public T0 getFirst() {
      return first;
    }

    public T1 getSecond() {
      return second;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ComparablePair) {
        ComparablePair<?, ?> that = (ComparablePair<?, ?>) o;
        return Objects.equals(this.first, that.first) && Objects.equals(this.second, that.second);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(first, second);
    }

    @Override
    public int compareTo(ComparablePair<T0, T1> o) {
      int result = getFirst().compareTo(o.getFirst());
      if (result == 0) {
        result = getSecond().compareTo(o.getSecond());
      }
      return result;
    }
  }

  static class CSession<T> implements MergingWindowing<T, TimeInterval>, Trigger<TimeInterval> {

    static final ValueStorageDescriptor<Integer> TR_STATE =
        ValueStorageDescriptor.of("quux", Integer.class, 0, (x, y) -> x + y);

    private final TimeTrigger trigger = new TimeTrigger();
    private final Session<T> wrap;

    CSession(Duration gap) {
      wrap = Session.of(gap);
    }

    @Override
    public Collection<Pair<Collection<TimeInterval>, TimeInterval>> mergeWindows(
        Collection<TimeInterval> actives) {
      return wrap.mergeWindows(actives);
    }

    @Override
    public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
      return wrap.assignWindowsToElement(el);
    }

    @Override
    public Trigger<TimeInterval> getTrigger() {
      return this;
    }

    @Override
    public TriggerResult onElement(long time, TimeInterval window, TriggerContext ctx) {
      return trigger.onElement(time, window, ctx);
    }

    @Override
    public TriggerResult onTimer(long time, TimeInterval window, TriggerContext ctx) {
      return trigger.onTimer(time, window, ctx);
    }

    @Override
    public void onClear(TimeInterval window, TriggerContext ctx) {
      trigger.onClear(window, ctx);
    }

    @Override
    public void onMerge(TimeInterval window, TriggerContext.TriggerMergeContext ctx) {
      trigger.onMerge(window, ctx);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CSession) {
        CSession other = (CSession) obj;
        return other.wrap.equals(wrap);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return wrap.hashCode();
    }
  }
}
