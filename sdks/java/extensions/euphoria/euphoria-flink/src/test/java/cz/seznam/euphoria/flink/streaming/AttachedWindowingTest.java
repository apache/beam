package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Lists;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AttachedWindowingTest {

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
        .withReadDelay(Duration.ofMillis(20))
        .withFinalDelay(Duration.ofSeconds(2));

    Flow f = Flow.create("test-attached-windowing");

    // ~ reduce using a specified windowing
    Dataset<Pair<String, Long>> uniq =
        ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofMillis(10))
            .using(e -> (long) e.getSecond()))
        .setNumPartitions(2)
        .output();

    ListDataSink<WindowedPair<Time.TimeInterval, String, HashMap<String, Long>>> output
        = ListDataSink.get(1);

    // ~ reduce the output using attached windowing, i.e. producing
    // one output element per received window
    Dataset<WindowedPair<Time.TimeInterval, String, HashMap<String, Long>>>
        reduced = ReduceByKey
        .of(uniq)
        .keyBy(e -> "")
        .valueBy(new ToHashMap<>())
        .combineBy(new MergeHashMaps<>())
        .setNumPartitions(1)
        .outputWindowed();
    reduced.persist(output);

    new TestFlinkExecutor()
        .setAutoWatermarkInterval(Duration.ofMillis(10))
        .setAllowedLateness(Duration.ofMillis(0))
        .waitForCompletion(f);

    // ~ we had two windows
    assertEquals(3, output.getOutput(0).size());
    assertEquals(
        Lists.newArrayList(
            Pair.of(new Time.TimeInterval(0, 10),
                toMap(Pair.of("one", 2L), Pair.of("two", 2L), Pair.of("three", 1L))),
            Pair.of(new Time.TimeInterval(10, 10),
                toMap(Pair.of("one", 3L), Pair.of("two", 1L), Pair.of("quux", 2L))),
            Pair.of(new Time.TimeInterval(20, 10),
                toMap(Pair.of("foo", 1L)))),
        output.getOutput(0)
            .stream()
            .map(wp -> Pair.of(wp.getWindowLabel(), wp.getSecond()))
            .collect(Collectors.toList()));
  }

  static <K, V> HashMap<K, V> toMap(Pair<K, V> ... ps) {
    HashMap<K, V> m = new HashMap<>();
    for (Pair<K, V> p : ps) {
      m.put(p.getKey(), p.getValue());
    }
    return m;
  }

  static <K, V> List<String> outputToString(List<Pair<String, HashMap<K, V>>> output) {
    ArrayList<String> xs = new ArrayList<>(output.size());

    for (Pair<String, HashMap<K, V>> p : output) {
      ArrayList<String> elems = new ArrayList<>();
      for (Map.Entry<K, V> e : p.getSecond().entrySet()) {
        elems.add(e.getKey() + "=" + e.getValue());
      }
      elems.sort(String::compareTo);
      elems.add(0, p.getFirst());
      xs.add(Joiner.on(", ").join(elems));
    }
    xs.sort(String::compareTo);
    return xs;
  }

  static class ToHashMap<K, V, P extends Pair<K, V>>
      implements UnaryFunction<P, HashMap<K, V>> {
    @Override
    public HashMap<K, V> apply(P what) {
      HashMap<K, V> m = new HashMap<>();
      m.put(what.getFirst(), what.getSecond());
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
                new Query(1L, "one", "A"),
                new Query(2L, "one", "B"),
                new Query(3L, "one", "A"),
                new Query(3L, "two", "A"),
                new Query(4L, "two", "A"),
                new Query(4L, "two", "B"),
                new Query(4L, "two", "C"),
                new Query(4L, "two", "D"),
                new Query(4L, "three", "X"),

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
            .setNumPartitions(1)
            .windowBy(Time.of(Duration.ofMillis(5))
                .using(e -> e.time))
            .output();

    // ~ count of query (effectively how many users used a given query)
    Dataset<WindowedPair<Time.TimeInterval, String, Long>> counted = ReduceByKey.of(distinctByUser)
        .keyBy(e -> e.query)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .setNumPartitions(10)
        .outputWindowed();

    // ~ reduced (in attached windowing mode) and partitioned by the window start time
    Dataset<WindowedPair<Time.TimeInterval, Long, HashMap<String, Long>>>
        reduced = ReduceByKey.of(counted)
        .keyBy(e -> e.getWindowLabel().getStartMillis())
        .valueBy(new ToHashMap<>())
        .combineBy(new MergeHashMaps<>())
        // ~ partition by the input window start time
        .setNumPartitions(2)
        .setPartitioner(e -> (int) (e % 2))
        .outputWindowed();

    ListDataSink<WindowedPair<Time.TimeInterval, Long, HashMap<String, Long>>> output =
        ListDataSink.get(2);
    reduced.persist(output);

    new TestFlinkExecutor()
        .setAllowedLateness(Duration.ofMillis(10))
        .setAllowedLateness(Duration.ofMillis(0))
        .waitForCompletion(f);

    assertEquals(
        Lists.newArrayList(
            Pair.of(new Time.TimeInterval(0, 5),
                toMap(Pair.of("one", 2L), Pair.of("two", 4L), Pair.of("three", 1L))),
            Pair.of(new Time.TimeInterval(10, 5),
                toMap(Pair.of("one", 1L), Pair.of("two", 1L)))),
        output.getOutput(0)
            .stream()
            .map(wp -> Pair.of(wp.getWindowLabel(), wp.getSecond()))
            .collect(Collectors.toList()));
    assertEquals(
        Lists.newArrayList(
            Pair.of(new Time.TimeInterval(5, 5), toMap(Pair.of("one", 3L), Pair.of("two", 2L)))),
        output.getOutput(1)
            .stream()
            .map(wp -> Pair.of(wp.getWindowLabel(), wp.getSecond()))
            .collect(Collectors.toList()));
  }
}
