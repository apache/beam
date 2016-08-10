package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestConsecutiveBasicOpsTranslation {

  static class Item<K, V> implements java.io.Serializable {
    final K f0; final V f1;
    Item(K f0, V f1) { this.f0 = f0; this.f1 = f1; }
  }

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    ListDataSink<Triple<String, String, String>> output = ListDataSink.get(1);

    Dataset<Item<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                new Item<>("foo", "bar"),
                new Item<>("quux", "ibis"))));
    Dataset<Pair<String, String>> intermediate =
        MapElements.of(input)
            .using(e -> Pair.of(e.f0, e.f1))
            .output();
    MapElements.of(intermediate)
        .using(e -> Triple.of("uf", e.getFirst(), e.getSecond()))
        .output()
        .persist(output);

    new TestFlinkExecutor().waitForCompletion(f);

    assertEquals(
        output.getOutput(0),
        Arrays.asList(
            Triple.of("uf", "foo", "bar"),
            Triple.of("uf", "quux", "ibis")));
  }
}