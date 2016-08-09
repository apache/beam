package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

public class TestMapElementsThenDistinctTranslation {

  static class Item<K, V> implements java.io.Serializable {
    final K f0; final V f1;
    Item(K f0, V f1) { this.f0 = f0; this.f1 = f1; }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Item) {
        Item<?, ?> item = (Item<?, ?>) o;
        return Objects.equals(f0, item.f0) &&
            Objects.equals(f1, item.f1);
      }
      return false;
    }
    @Override
    public int hashCode() {
      return Objects.hash(f0, f1);
    }
  }

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    Dataset<Item<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                new Item<>("foo", "bar"),
                new Item<>("foo", "bar"))));
    Dataset<Pair<String, String>> intermediate =
        MapElements.of(input)
            .using(e -> Pair.of(e.f0, e.f1))
            .output();
    Distinct.of(intermediate)
        .windowBy(Windowing.Time.of(Duration.ofSeconds(5)))
        .output()
        .persist(new StdoutSink<>(false, null));

    new TestFlinkExecutor().waitForCompletion(f);
  }
}