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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RSBKWindowingTest {

  private static class AccState<VALUE> extends State<VALUE, VALUE> {
    final ListStorage<VALUE> reducableValues;
    @SuppressWarnings("unchecked")
    AccState(Context<VALUE> context,
             StorageProvider storageProvider)
    {
      super(context);
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

    void add(AccState<VALUE> other) {
      this.reducableValues.addAll(other.reducableValues.get());
    }

    @Override
    public void close() {
      reducableValues.clear();
    }

    public static <VALUE>
    void combine(AccState<VALUE> target, Iterable<AccState<VALUE>> others) {
      for (AccState<VALUE> other : others) {
        target.add(other);
      }
    }
  }

  @Test
  public void testEventWindowing() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Pair<String, Integer>>> output
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
    Dataset<Pair<String, Pair<String, Integer>>> reduced =
        ReduceStateByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> e)
        .stateFactory((StateFactory<Pair<String, Integer>, Pair<String, Integer>, AccState<Pair<String, Integer>>>) AccState::new)
        .mergeStatesBy(AccState::combine)
        .windowBy(Time.of(Duration.ofMillis(5)),
            // ~ event time
            (ExtractEventTime<Pair<String, Integer>>) what -> (long) what.getSecond())
        .setNumPartitions(1)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .submit(f)
        .get();

    assertEquals(
        asList(
            "0: one/one/1", "0: one/one/2", "0: two/two/3",
            "5: two/two/6", "5: two/two/7", "5: two/two/8",
            "5: three/three/7"),
        output.getOutput(0)
            .stream()
            .map(p -> p.getFirst().getStartMillis() + ": " + p.getSecond() + "/" + p.getThird().getFirst() + "/" + p.getThird().getSecond())
            .collect(Collectors.toList()));
  }

  @Test
  public void testEventWindowing_attachedWindowing() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Pair<String, Integer>>> output
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
        ReduceStateByKey.of(f.createInput(source))
            .keyBy(Pair::getFirst)
            .valueBy(e -> e)
            .stateFactory((StateFactory<Pair<String, Integer>, Pair<String, Integer>, AccState<Pair<String, Integer>>>) AccState::new)
            .mergeStatesBy(AccState::combine)
            .windowBy(Time.of(Duration.ofMillis(5)),
                // ~ event time
                (ExtractEventTime<Pair<String, Integer>>) what -> (long) what.getSecond())
            .setNumPartitions(1)
            .output();

    Dataset<Pair<String, Integer>> secondStep =
        MapElements.of(firstStep).using(Pair::getSecond).output();

    Dataset<Pair<String, Pair<String, Integer>>> reduced =
        ReduceStateByKey.of(secondStep)
        .keyBy(Pair::getFirst)
        .valueBy(e -> e)
        .stateFactory((StateFactory<Pair<String, Integer>, Pair<String, Integer>, AccState<Pair<String, Integer>>>) AccState::new)
        .mergeStatesBy(AccState::combine)
        .windowBy(Time.of(Duration.ofMillis(5)),
            // ~ event time
            (ExtractEventTime<Pair<String, Integer>>) what -> (long) what.getSecond())
        .setNumPartitions(1)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .submit(f)
        .get();

    assertEquals(
        asList(
            "0: one/one/1", "0: one/one/2", "0: two/two/3",
            "5: two/two/6", "5: two/two/7", "5: two/two/8",
            "5: three/three/7"),
        output.getOutput(0)
            .stream()
            .map(p -> p.getFirst().getStartMillis() + ": " + p.getSecond() + "/" + p.getThird().getFirst() + "/" + p.getThird().getSecond())
            .collect(Collectors.toList()));
  }

}
