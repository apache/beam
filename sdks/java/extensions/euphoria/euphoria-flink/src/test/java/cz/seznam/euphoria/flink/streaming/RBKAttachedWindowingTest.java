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
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RBKAttachedWindowingTest {

  @Test
  public void testAttachedWindow() throws Exception {
    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            asList(
                Pair.of("one",   1),
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   4),
                Pair.of("three", 5),
                Pair.of("one",   10),
                Pair.of("two",   11),
                Pair.of("one",   12),
                Pair.of("one",   13),
                Pair.of("quux",  13),
                Pair.of("quux",  13),
                Pair.of("foo",   21)))
        .withReadDelay(Duration.ofMillis(200))
        .withFinalDelay(Duration.ofSeconds(2));

    Flow f = Flow.create("test-attached-windowing");

    // ~ reduce using a specified windowing
    Dataset<Pair<String, Long>> uniq =
        ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofMillis(10)), e -> (long) e.getSecond())
        .setNumPartitions(2)
        .output();

    ListDataSink<Triple<TimeInterval, String, HashMap<String, Long>>> output
        = ListDataSink.get(1);

    // ~ reduce the output using attached windowing, i.e. producing
    // one output element per received window
    Dataset<Pair<String, HashMap<String, Long>>>
        reduced = ReduceByKey
        .of(uniq)
        .keyBy(e -> "")
        .valueBy(new ToHashMap<>(Pair::getFirst, Pair::getSecond))
        .combineBy(new MergeHashMaps<>())
        .setNumPartitions(1)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .setAllowedLateness(Duration.ofMillis(0))
        .submit(f)
        .get();

    // ~ we had two windows
    assertEquals(3, output.getOutput(0).size());
    assertEquals(
        Arrays.asList(
            Pair.of(new TimeInterval(0, 10),
                toMap(Pair.of("one", 2L), Pair.of("two", 2L), Pair.of("three", 1L))),
            Pair.of(new TimeInterval(10, 20),
                toMap(Pair.of("one", 3L), Pair.of("two", 1L), Pair.of("quux", 2L))),
            Pair.of(new TimeInterval(20, 30),
                toMap(Pair.of("foo", 1L)))),
        output.getOutput(0)
            .stream()
            .map(wp -> Pair.of(wp.getFirst(), wp.getThird()))
            .collect(Collectors.toList()));
  }

  @Test
  public void testAttachedWindowNonCombining() throws Exception {
    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            asList(
                Pair.of("one",   1),
                Pair.of("one",   2),
                Pair.of("two",   3),
                Pair.of("two",   4),
                Pair.of("three", 5),
                Pair.of("one",   10),
                Pair.of("two",   11),
                Pair.of("one",   12),
                Pair.of("one",   13),
                Pair.of("quux",  13),
                Pair.of("quux",  13),
                Pair.of("foo",   21)))
            .withReadDelay(Duration.ofMillis(200))
            .withFinalDelay(Duration.ofSeconds(2));

    Flow f = Flow.create("test-attached-windowing");

    // ~ reduce using a specified windowing
    Dataset<Pair<String, Long>> uniq =
        ReduceByKey.of(f.createInput(source))
            .keyBy(Pair::getFirst)
            .valueBy(e -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(Time.of(Duration.ofMillis(10)), e -> (long) e.getSecond())
            .setNumPartitions(2)
            .output();

    ListDataSink<Triple<TimeInterval, String, HashMap<String, Long>>> output
        = ListDataSink.get(1);

    // ~ reduce the output using attached windowing, i.e. producing
    // one output element per received window
    Dataset<Pair<String, HashMap<String, Long>>>
        reduced = ReduceByKey
        .of(uniq)
        .keyBy(e -> "")
        .valueBy(e -> e)
        .reduceBy(xs -> {
          HashMap<String, Long> m = new HashMap<>();
          for (Pair<String, Long> x : xs) {
            m.put(x.getFirst(), x.getSecond());
          }
          return m;
        })
        .setNumPartitions(1)
        .output();

    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .setAllowedLateness(Duration.ofMillis(0))
        .submit(f)
        .get();

    // ~ we had two windows
    assertEquals(3, output.getOutput(0).size());
    assertEquals(
        Arrays.asList(
            Pair.of(new TimeInterval(0, 10),
                toMap(Pair.of("one", 2L), Pair.of("two", 2L), Pair.of("three", 1L))),
            Pair.of(new TimeInterval(10, 20),
                toMap(Pair.of("one", 3L), Pair.of("two", 1L), Pair.of("quux", 2L))),
            Pair.of(new TimeInterval(20, 30),
                toMap(Pair.of("foo", 1L)))),
        output.getOutput(0)
            .stream()
            .map(wp -> Pair.of(wp.getFirst(), wp.getThird()))
            .collect(Collectors.toList()));
  }

  @SafeVarargs
  static <K, V> HashMap<K, V> toMap(Pair<K, V> ... ps) {
    HashMap<K, V> m = new HashMap<>();
    for (Pair<K, V> p : ps) {
      m.put(p.getKey(), p.getValue());
    }
    return m;
  }

  static class ToHashMap<K, V, E>
      implements UnaryFunction<E, HashMap<K, V>> {
    private final UnaryFunction<E, K> keyFn;
    private final UnaryFunction<E, V> valFn;

    ToHashMap(UnaryFunction<E, K> keyFn, UnaryFunction<E, V> valFn) {
      this.keyFn = keyFn;
      this.valFn = valFn;
    }

    @Override
    public HashMap<K, V> apply(E e) {
      HashMap<K, V> m = new HashMap<>();
      m.put(keyFn.apply(e), valFn.apply(e));
      return m;
    }
  }

  static class MergeHashMaps<K, V> implements CombinableReduceFunction<HashMap<K, V>> {
    @Override
    public HashMap<K, V> apply(Iterable<HashMap<K, V>> what) {
      Iterator<HashMap<K, V>> iter = what.iterator();
      HashMap<K, V> m = iter.next();
      while (iter.hasNext()) {
        HashMap<K, V> n = iter.next();
        for (Map.Entry<K, V> e : n.entrySet()) {
          m.put(e.getKey(), e.getValue());
        }
      }
      return m;
    }
  }

  private static final class Query {
    public long time;
    public String query;
    public String user;

    public Query() {}
    public Query(long time, String query, String user) {
      this.time = time;
      this.query = query;
      this.user = user;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Query) {
        Query that = (Query) o;
        return Objects.equals(query, that.query) && Objects.equals(user, that.user);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(query, user);
    }

    @Override
    public String toString() {
      return "Query{" +
          "time=" + time +
          ", query='" + query + '\'' +
          ", user='" + user + '\'' +
          '}';
    }
  } // ~ end of Query

  @Test
  public void testDistinctReduction() throws Exception {
    ListDataSource<Query> source =
        ListDataSource.unbounded(
            asList(
                new Query(0L, "one", "A"),
                new Query(1L, "one", "B"),
                new Query(2L, "one", "A"),
                new Query(2L, "two", "A"),
                new Query(2L, "two", "A"),
                new Query(3L, "two", "B"),
                new Query(3L, "two", "C"),
                new Query(3L, "two", "D"),
                new Query(3L, "three", "X"),

                new Query(6L, "one", "A"),
                new Query(6L, "one", "B"),
                new Query(7L, "one", "C"),
                new Query(7L, "two", "A"),
                new Query(7L, "two", "D"),

                new Query(11L, "one", "Q"),
                new Query(11L, "two", "P")))
        .withReadDelay(Duration.ofMillis(20))
        .withFinalDelay(Duration.ofSeconds(2));

    Flow f = Flow.create("test");

    // ~ distinct queries per user
    Dataset<Query> distinctByUser =
        Distinct.of(f.createInput(source))
            .mapped(e -> e)
            .setNumPartitions(1)
            .windowBy(Time.of(Duration.ofMillis(5)), e -> e.time)
            .output();

    // ~ count of query (effectively how many users used a given query)
    Dataset<Triple<TimeInterval, String, Long>> counted =
        Util.extractWindows(
            ReduceByKey.of(distinctByUser)
                .keyBy(e -> e.query)
                .valueBy(e -> 1L)
                .combineBy(Sums.ofLongs())
                .setNumPartitions(10)
                .output(),
            TimeInterval.class);

    // ~ reduced (in attached windowing mode) and partitioned by the window start time
    Dataset<Pair<Long, HashMap<String, Long>>> reduced =
        ReduceByKey.of(counted)
        .keyBy(e -> e.getFirst().getStartMillis())
        .valueBy(new ToHashMap<>(Triple::getSecond, Triple::getThird))
        .combineBy(new MergeHashMaps<>())
        // ~ partition by the input window start time
        .setNumPartitions(2)
        .setPartitioner(e -> (int) (e % 2))
        .output();

    ListDataSink<Triple<TimeInterval, Long, HashMap<String, Long>>> output =
        ListDataSink.get(2);
    Util.extractWindows(reduced, TimeInterval.class).persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .setAllowedLateness(Duration.ofMillis(0))
        .submit(f)
        .get();

    assertEquals(
        Arrays.asList(
            Pair.of(new TimeInterval(0, 5),
                toMap(Pair.of("one", 2L), Pair.of("two", 4L), Pair.of("three", 1L))),
            Pair.of(new TimeInterval(10, 15),
                toMap(Pair.of("one", 1L), Pair.of("two", 1L)))),
        output.getOutput(0)
            .stream()
            .map(wp -> Pair.of(wp.getFirst(), wp.getThird()))
            .collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(
            Pair.of(new TimeInterval(5, 10), toMap(Pair.of("one", 3L), Pair.of("two", 2L)))),
        output.getOutput(1)
            .stream()
            .map(wp -> Pair.of(wp.getFirst(), wp.getThird()))
            .collect(Collectors.toList()));
  }
}
