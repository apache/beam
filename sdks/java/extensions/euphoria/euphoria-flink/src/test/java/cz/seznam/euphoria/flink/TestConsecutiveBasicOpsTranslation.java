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

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    ListDataSink<Triple<String, String, String>> output = ListDataSink.get(1);

    Dataset<Pair<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("foo", "bar"),
                Pair.of("quux", "ibis"))));
    Dataset<Pair<String, String>> intermediate =
        MapElements.of(input)
            .using(e -> Pair.of(e.getFirst(), e.getSecond()))
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