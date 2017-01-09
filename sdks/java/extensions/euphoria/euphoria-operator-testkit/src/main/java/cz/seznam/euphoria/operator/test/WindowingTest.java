package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
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
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

/**
 * Tests capabilities of {@link Windowing}
 */
@Processing(Processing.Type.ALL)
public class WindowingTest extends AbstractOperatorTest {

  private enum Type {
    FRUIT, VEGETABLE
  }

  @Test
  public void consecutiveWindowingTest_ReduceByKey() throws Exception {
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

      @Override
      public void validate(Partitions partitions) {
        assertEquals(1, partitions.size());

        assertEquals(Sets.newHashSet(
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.FRUIT, 3L),
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.VEGETABLE, 3L),

                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.FRUIT, 2L),
                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.VEGETABLE, 1L)
                ),
                Sets.newHashSet(partitions.get(0)));
      }
    });
  }

  @Test
  public void consecutiveWindowingTest_ReduceStateByKey() throws Exception {
    execute(new AbstractTestCase<Triple<Instant, Type, String>, Triple<Instant, Type, Long>>() {

      @Override
      protected Dataset<Triple<Instant, Type, Long>> getOutput(Dataset<Triple<Instant, Type, String>> input) {
        // distinct implemented using raw ReduceStateByKey
        Dataset<Pair<ComparablePair<Type, String>, Object>> pairs =
                ReduceStateByKey.of(input)
                                .keyBy(t -> ComparablePair.of(t.getSecond(), t.getThird()))
                                .valueBy(t -> null)
                                .stateFactory(DistinctState::new)
                                .combineStateBy(it -> it.iterator().next())
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

      @Override
      public void validate(Partitions partitions) {
        assertEquals(1, partitions.size());

        assertEquals(Sets.newHashSet(
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.FRUIT, 3L),
                Triple.of(Instant.parse("2016-12-19T11:00:00.000Z"), Type.VEGETABLE, 3L),

                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.FRUIT, 2L),
                Triple.of(Instant.parse("2016-12-19T12:00:00.000Z"), Type.VEGETABLE, 1L)
                ),
                Sets.newHashSet(partitions.get(0)));
      }
    });
  }

  private static class DistinctState extends State<Object, Object> {

    private final ValueStorage<Object> storage;

    public DistinctState(Context<Object> context, StorageProvider storageProvider) {
      super(context, storageProvider);
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
}
