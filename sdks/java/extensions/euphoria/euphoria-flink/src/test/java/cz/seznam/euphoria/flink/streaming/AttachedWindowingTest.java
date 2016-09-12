package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Joiner;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        outputToString(output.getOutput(0)),
        asList(", one=2, three=1, two=2",
               ", one=3, quux=2, two=1"));
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

  static HashMap<String, Long> newMapFrom(Pair<String, Long> ... ps) {
    HashMap<String, Long> m = new HashMap<>();
    for (Pair<String, Long> p : ps) {
      m.put(p.getKey(), p.getValue());
    }
    return m;
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
}
