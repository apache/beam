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
import cz.seznam.euphoria.core.client.dataset.asserts.DatasetAssert;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

public class RSBKWindowingTest {

  private static class AccState<VALUE> implements State<VALUE, VALUE> {
    final ListStorage<VALUE> reducableValues;
    @SuppressWarnings("unchecked")
    AccState(StorageProvider storageProvider) {
      reducableValues = storageProvider.getListStorage(
          ListStorageDescriptor.of("vals", (Class) Object.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(VALUE element) {
      reducableValues.add(element);
    }

    @Override
    public void flush(Collector<VALUE> context) {
      for (VALUE value : reducableValues.get()) {
        context.collect(value);
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
        = ListDataSink.get();

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
        ReduceStateByKey.of(f.createInput(source, Pair::getSecond))
        .keyBy(Pair::getFirst)
        .valueBy(e -> e)
        .stateFactory((StorageProvider storages, Collector<Pair<String, Integer>> ctx) -> new AccState<>(storages))
        .mergeStatesBy(AccState::combine)
        .windowBy(Time.of(Duration.ofMillis(5)))
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .submit(f)
        .get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of(new TimeInterval(0, 5), "one", Pair.of("one", 1)),
        Triple.of(new TimeInterval(0, 5), "one", Pair.of("one", 2)),
        Triple.of(new TimeInterval(0, 5), "two", Pair.of("two", 3)),
        Triple.of(new TimeInterval(5, 10), "two", Pair.of("two", 6)),
        Triple.of(new TimeInterval(5, 10), "two", Pair.of("two", 7)),
        Triple.of(new TimeInterval(5, 10), "two", Pair.of("two", 8)),
        Triple.of(new TimeInterval(5, 10), "three", Pair.of("three", 7)));
  }

  @Test
  public void testEventWindowing_attachedWindowing() throws Exception {
    ListDataSink<Triple<TimeInterval, String, Pair<String, Integer>>> output
        = ListDataSink.get();

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
        ReduceStateByKey.of(f.createInput(source, Pair::getSecond))
            .keyBy(Pair::getFirst)
            .valueBy(e -> e)
            .stateFactory((StorageProvider storages, Collector<Pair<String, Integer>> ctx) -> new AccState<>(storages))
            .mergeStatesBy(AccState::combine)
            .windowBy(Time.of(Duration.ofMillis(5)))
            .output();

    Dataset<Pair<String, Integer>> secondStep =
        FlatMap.of(firstStep)
        .using((UnaryFunctor<Pair<String, Pair<String, Integer>>, Pair<String, Integer>>)
            (elem, context) -> context.collect(elem.getSecond()))
        .eventTimeBy(e -> e.getSecond().getSecond())
        .output();

    Dataset<Pair<String, Pair<String, Integer>>> reduced =
        ReduceStateByKey.of(secondStep)
        .keyBy(Pair::getFirst)
        .valueBy(e -> e)
        .stateFactory((StorageProvider storages, Collector<Pair<String, Integer>> ctx) -> new AccState<>(storages))
        .mergeStatesBy(AccState::combine)
        .windowBy(Time.of(Duration.ofMillis(5)))
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(0))
        .setAutoWatermarkInterval(Duration.ofMillis(100))
        .submit(f)
        .get();

    DatasetAssert.unorderedEquals(
        output.getOutputs(),
        Triple.of(new TimeInterval(0, 5), "one", Pair.of("one", 1)),
        Triple.of(new TimeInterval(0, 5), "one", Pair.of("one", 2)),
        Triple.of(new TimeInterval(0, 5), "two", Pair.of("two", 3)),
        Triple.of(new TimeInterval(5, 10), "two", Pair.of("two", 6)),
        Triple.of(new TimeInterval(5, 10), "two", Pair.of("two", 7)),
        Triple.of(new TimeInterval(5, 10), "two", Pair.of("two", 8)),
        Triple.of(new TimeInterval(5, 10), "three", Pair.of("three", 7)));
  }

}
