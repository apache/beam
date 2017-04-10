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
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
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
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Lists;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests capabilities of {@link Windowing}
 */
@Processing(Processing.Type.ALL)
public class WindowingTest extends AbstractOperatorTest {

  private enum Type {
    FRUIT, VEGETABLE
  }

  @Test
  public void consecutiveWindowingTest_ReduceByKey() {
    execute(new AbstractTestCase<Triple<Instant, Type, String>, Triple<Instant, Type, Long>>() {

      @Override
      protected Dataset<Triple<Instant, Type, Long>> getOutput(Dataset<Triple<Instant, Type, String>> input) {
        Dataset<ComparablePair<Type, String>> distinct =
                Distinct.of(input)
                        .mapped(t -> ComparablePair.of(t.getSecond(), t.getThird()))
                        .windowBy(Time.of(Duration.ofHours(1)), t -> t.getFirst().toEpochMilli())
                        .output();

        Dataset<Pair<Type, Long>> reduced = ReduceByKey.of(distinct)
                .keyBy(Pair::getFirst)
                .valueBy(p -> 1L)
                .combineBy(Sums.ofLongs())
                .windowBy(Time.of(Duration.ofHours(1)))
                .output();

        // extract window timestamp
        return FlatMap.of(reduced)
                .using((Pair<Type, Long> p, Context<Triple<Instant, Type, Long>> ctx) -> {
                  long windowEnd = ((TimeInterval) ctx.getWindow()).getEndMillis();
                  ctx.collect(Triple.of(Instant.ofEpochMilli(windowEnd), p.getFirst(), p.getSecond()));
                })
                .output();
      }

      @Override
      protected Partitions<Triple<Instant, Type, String>> getInput() {
        return Partitions.add(
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
                Triple.of(Instant.parse("2016-12-19T11:25:00.000Z"), Type.VEGETABLE, "carrot"))
                .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @SuppressWarnings("unchecked")
      @Override
      public void validate(Partitions partitions) {
        assertEquals(1, partitions.size());

        assertEquals(Sets.newHashSet(
            Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.FRUIT, 3L),
            Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.VEGETABLE, 3L),
            Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.FRUIT, 2L),
            Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.VEGETABLE, 1L)),
            Sets.newHashSet(partitions.get(0)));
      }
    });
  }

  @Test
  public void consecutiveWindowingTest_ReduceStateByKey() {
    execute(new AbstractTestCase<Triple<Instant, Type, String>, Triple<Instant, Type, Long>>() {

      @Override
      protected Dataset<Triple<Instant, Type, Long>> getOutput(Dataset<Triple<Instant, Type, String>> input) {
        // distinct implemented using raw ReduceStateByKey
        Dataset<Pair<ComparablePair<Type, String>, Object>> pairs =
                ReduceStateByKey.of(input)
                                .keyBy(t -> ComparablePair.of(t.getSecond(), t.getThird()))
                                .valueBy(t -> null)
                                .stateFactory(DistinctState::new)
                                .mergeStatesBy((t, os) -> {})
                                .windowBy(Time.of(Duration.ofHours(1)), (Triple<Instant, Type, String> t) -> t.getFirst().toEpochMilli())
                                .output();

        Dataset<ComparablePair<Type, String>> distinct = MapElements.of(pairs)
                .using(Pair::getFirst)
                .output();

        Dataset<Pair<Type, Long>> reduced = ReduceByKey.of(distinct)
                .keyBy(Pair::getFirst)
                .valueBy(p -> 1L)
                .combineBy(Sums.ofLongs())
                .windowBy(Time.of(Duration.ofHours(1)))
                .output();

        // extract window timestamp
        return FlatMap.of(reduced)
                .using((Pair<Type, Long> p, Context<Triple<Instant, Type, Long>> ctx) -> {
                  long windowEnd = ((TimeInterval) ctx.getWindow()).getEndMillis();
                  ctx.collect(Triple.of(Instant.ofEpochMilli(windowEnd), p.getFirst(), p.getSecond()));
                })
                .output();
      }

      @Override
      protected Partitions<Triple<Instant, Type, String>> getInput() {
        return Partitions.add(
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
                Triple.of(Instant.parse("2016-12-19T11:25:00.000Z"), Type.VEGETABLE, "carrot"))
                .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @SuppressWarnings("unchecked")
      @Override
      public void validate(Partitions partitions) {
        assertEquals(1, partitions.size());

        assertEquals(Sets.newHashSet(
            Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.FRUIT, 3L),
            Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.VEGETABLE, 3L),
            Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.FRUIT, 2L),
            Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.VEGETABLE, 1L)),
            Sets.newHashSet(partitions.get(0)));
      }
    });
  }

  private static class DistinctState extends State<Object, Object> {

    private final ValueStorage<Object> storage;

    public DistinctState(Context<Object> context, StorageProvider storageProvider) {
      super(context);
      this.storage = storageProvider.getValueStorage(
              ValueStorageDescriptor.of("element", Object.class, null));
    }

    @Override
    public void add(Object element) {
      storage.set(element);
    }

    @Override
    public void flush() {
      getContext().collect(storage.get());
    }
  }

  private static class ComparablePair<T0, T1>
          extends Pair<T0, T1>
          implements Comparable<ComparablePair<T0, T1>> {

    ComparablePair(T0 first, T1 second) {
      super(first, second);
      Preconditions.checkArgument(first instanceof Comparable,
              first.getClass() + " is required to implement Comparable");
      Preconditions.checkArgument(second instanceof Comparable,
              second.getClass() + " is required to implement Comparable");
    }

    public static <T0, T1> ComparablePair<T0, T1> of(T0 first, T1 second) {
      return new ComparablePair<>(first, second);
    }

    @Override
    public int compareTo(ComparablePair<T0, T1> o) {
      int result = compare(getFirst(), o.getFirst());
      if (result == 0) {
        result = compare(getSecond(), o.getSecond());
      }

      return result;
    }

    @SuppressWarnings("unchecked")
    private int compare(Object obj1, Object obj2) {
      return ((Comparable) obj1).compareTo(obj2);
    }
  }

  static class CSession<T> implements MergingWindowing<T, TimeInterval>, Trigger<TimeInterval> {

    static final ValueStorageDescriptor<Integer> TR_STATE
            = ValueStorageDescriptor.of("quux", Integer.class, 0, (x, y) -> x + y);

    private final TimeTrigger trigger = new TimeTrigger();
    private final Session<T> wrap;

    CSession(Duration gap) {
      wrap = Session.of(gap);
    }

    @Override
    public Collection<Pair<Collection<TimeInterval>, TimeInterval>> mergeWindows(Collection<TimeInterval> actives) {
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
  }

  static final AtomicBoolean ON_CLEAR_VALIDATED = new AtomicBoolean(false);

  /**
   * Validates a trigger's #onClear method operates in the right context of
   * merged windows.<p>
   *
   * A trigger's lifecycle is guaranteed only on stream processing; batch
   * processing has more freedom and doesn't necessarily invoke the
   * trigger#onClear method.
   */
  @Processing(Processing.Type.UNBOUNDED)
  @Test
  public void testSessionWindowingTriggerStateConsistency() {
    ON_CLEAR_VALIDATED.set(false);
    execute(new AbstractTestCase<Pair<Instant, String>, Triple<Instant, Instant, Integer>>() {
      @SuppressWarnings("unchecked")
      @Override
      protected Dataset<Triple<Instant, Instant, Integer>>
      getOutput(Dataset<Pair<Instant, String>> input) {
        CSession windowing = new CSession(Duration.ofMinutes(5)) {
          @Override
          public TriggerResult onElement(long time, TimeInterval window, TriggerContext ctx) {
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

        Dataset<Pair<String, Integer>> pairs =
                ReduceByKey.of(input)
                .keyBy(e -> "")
                .valueBy(e -> 1)
                .combineBy(Sums.ofInts())
                .windowBy(windowing, t -> t.getFirst().toEpochMilli())
                .output();

        // extract window timestamp
        return FlatMap.of(pairs)
                .using((Pair<String, Integer> in, Context<Triple<Instant, Instant, Integer>> out) -> {
                  long windowBegin = ((TimeInterval) out.getWindow()).getStartMillis();
                  long windowEnd = ((TimeInterval) out.getWindow()).getEndMillis();
                  out.collect(Triple.of(
                          Instant.ofEpochMilli(windowBegin),
                          Instant.ofEpochMilli(windowEnd),
                          in.getSecond()));
                })
                .output();
      }

      @Override
      protected Partitions<Pair<Instant, String>> getInput() {
        return Partitions.add(
                Pair.of(Instant.parse("2016-12-19T10:10:00.000Z"), "foo"),
                Pair.of(Instant.parse("2016-12-19T10:11:00.000Z"), "foo"),
                Pair.of(Instant.parse("2016-12-19T10:12:00.000Z"), "foo"))
                .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @SuppressWarnings("unchecked")
      @Override
      public void validate(Partitions partitions) {
        assertEquals(1, partitions.size());
        assertEquals(
                Lists.newArrayList(Triple.of(
                        Instant.parse("2016-12-19T10:10:00.000Z"),
                        Instant.parse("2016-12-19T10:17:00.000Z"),
                        3)),
                partitions.get(0));
      }
    });
    assertEquals(true, ON_CLEAR_VALIDATED.get());
  }
}
