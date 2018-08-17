package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RSBKWindowingTest {

  private static class AccState<VALUE> extends State<VALUE, VALUE> {
    final ListStorage<VALUE> reducableValues;
    AccState(Context<VALUE> context,
             StorageProvider storageProvider)
    {
      super(context, storageProvider);
      reducableValues = storageProvider.getListStorage(
          ListStorageDescriptor.of("vals", (Class) Object.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(VALUE element) {
      reducableValues.add(element);
    }

    @Override
    public void flush() {
      for (VALUE value : reducableValues.get()) {
        getContext().collect(value);
      }
    }

    void add(AccState other) {
      this.reducableValues.addAll(other.reducableValues.get());
    }

    @Override
    public void close() {
      reducableValues.clear();
    }

    public static <VALUE> AccState<VALUE> combine(Iterable<AccState<VALUE>> xs) {
      Iterator<AccState<VALUE>> iter = xs.iterator();
      AccState<VALUE> first = iter.next();
      while (iter.hasNext()) {
        first.add(iter.next());
      }
      return first;
    }
  }

  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<WindowedPair<TimeInterval, String, Pair<String, Integer>>> output
        = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one",   1),
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   6),
                Pair.of("two",   7),
                Pair.of("three", 7),
                Pair.of("two",   8)))
            .withReadDelay(Duration.ofMillis(500))
            .withFinalDelay(Duration.ofMillis(1000));

    Flow f = Flow.create("test-windowing");
    ReduceStateByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> e)
        .stateFactory((StateFactory<Pair<String, Integer>, AccState<Pair<String, Integer>>>) AccState::new)
        .combineStateBy(AccState::combine)
        .windowBy(Time.of(Duration.ofMillis(5))
            // ~ event time
            .using((UnaryFunction<Pair<String, Integer>, Long>) what ->
                (long) what.getSecond()))
        .setNumPartitions(1)
        .outputWindowed()
        .persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .waitForCompletion(f);

    assertEquals(
        asList(
            "0: one/one/1", "0: one/one/2", "0: two/two/3",
            "5: two/two/6", "5: two/two/7", "5: two/two/8",
            "5: three/three/7"),
        output.getOutput(0)
            .stream()
            .map(p -> p.getWindowLabel().getStartMillis() + ": " + p.getFirst() + "/" + p.getSecond().getFirst() + "/" + p.getSecond().getSecond())
            .collect(Collectors.toList()));
  }

  @Test
  public void testEventWindowing_attachedWindowing() throws Exception {
    ListDataSink<WindowedPair<TimeInterval, String, Pair<String, Integer>>> output
        = ListDataSink.get(1);

    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one",   1),
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   6),
                Pair.of("two",   7),
                Pair.of("three", 7),
                Pair.of("two",   8)))
            .withReadDelay(Duration.ofMillis(500))
            .withFinalDelay(Duration.ofMillis(1000));

    Flow f = Flow.create("test-attached-windowing");

    Dataset<Pair<String, Pair<String, Integer>>> firstStep =
        ReduceStateByKey.of(f
            .createInput(source))
            .keyBy(Pair::getFirst)
            .valueBy(e -> e)
            .stateFactory((StateFactory<Pair<String, Integer>, AccState<Pair<String, Integer>>>) AccState::new)
            .combineStateBy(AccState::combine)
            .windowBy(Time.of(Duration.ofMillis(5))
                // ~ event time
                .using((UnaryFunction<Pair<String, Integer>, Long>) what ->
                    (long) what.getSecond()))
            .setNumPartitions(1)
            .output();

    Dataset<Pair<String, Integer>> secondStep =
        MapElements.of(firstStep).using(p -> p.getSecond()).output();

    ReduceStateByKey.of(secondStep)
        .keyBy(Pair::getFirst)
        .valueBy(e -> e)
        .stateFactory((StateFactory<Pair<String, Integer>, AccState<Pair<String, Integer>>>) AccState::new)
        .combineStateBy(AccState::combine)
        .windowBy(Time.of(Duration.ofMillis(5))
            // ~ event time
            .using((UnaryFunction<Pair<String, Integer>, Long>) what ->
                (long) what.getSecond()))
        .setNumPartitions(1)
        .outputWindowed()
        .persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .waitForCompletion(f);

    assertEquals(
        asList(
            "0: one/one/1", "0: one/one/2", "0: two/two/3",
            "5: two/two/6", "5: two/two/7", "5: two/two/8",
            "5: three/three/7"),
        output.getOutput(0)
            .stream()
            .map(p -> p.getWindowLabel().getStartMillis() + ": " + p.getFirst() + "/" + p.getSecond().getFirst() + "/" + p.getSecond().getSecond())
            .collect(Collectors.toList()));
  }

}
