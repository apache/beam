package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
                Pair.of("quux",  13)));

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

    ListDataSink<Pair<String, HashMap<String, Long>>> output = ListDataSink.get(1);

    // ~ reduce the output using attached windowing, i.e. producing
    // one output element per received window
    ReduceByKey.of(uniq)
        .keyBy(e -> "")
        .valueBy(new ToHashMap<>())
        .combineBy(new MergeHashMaps<>())
        .setNumPartitions(1)
        .output()
        .persist(output);

    new TestFlinkExecutor().waitForCompletion(f);

    // ~ we had two windows
    assertEquals(2, output.getOutput(0).size());
    assertEquals(
        // ~ the two windows might have been emitted at the same time making it
        // difficult to reason about the arrival order. we're interested in the
        // computation result, not the order, hence we compare against a stable
        // presentation of the computation
        asList(", one=2, three=1, two=2", ", one=3, quux=2, two=1"),
        outputToString(output.getOutput(0)));
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

  static class ToHashMap<K, V> implements UnaryFunction<Pair<K, V>, HashMap<K, V>> {
    @Override
    public HashMap<K, V> apply(Pair<K, V> what) {
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
                new Query(7L, "two", "D")));

    Flow f = Flow.create("test");

    // ~ distinct queries per user
    Dataset<Query> distinctByUser =
        Distinct.of(f.createInput(source))
            .setNumPartitions(1)
            .windowBy(Time.of(Duration.ofMillis(5))
                .using(e -> e.time))
            .output();

    // ~ count of query (effectively how many users used a given query)
    Dataset<Pair<String, Long>> counted = ReduceByKey.of(distinctByUser)
        .keyBy(e -> e.query)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .setNumPartitions(1)
        .output();

//    counted.persist(new StdoutSink<>(true, null));
//    new TestFlinkExecutor().waitForCompletion(f);

    ListDataSink<Pair<String, HashMap<String, Long>>> output = ListDataSink.get(1);
    ReduceByKey.of(counted)
        .keyBy(e -> "")
        .valueBy(new ToHashMap<>())
        .combineBy(new MergeHashMaps<>())
        .setNumPartitions(1)
        .output()
        .persist(output);

    new TestFlinkExecutor().waitForCompletion(f);

    assertEquals(
        asList(", one=2, three=1, two=4", ", one=3, two=2"),
        outputToString(output.getOutput(0)));
  }
}
