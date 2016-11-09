package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.util.Pair;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestDistinctBasic {

  @Test
  public void test() throws Exception {
    Flow f = Flow.create("Test");

    ListDataSink<Pair<String, String>> output = ListDataSink.get(1);

    Dataset<Pair<String, String>> input =
        f.createInput(ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("foo", "bar"),
                Pair.of("quux", "ibis"),
                Pair.of("foo", "bar"))));
    Distinct.of(input)
        .windowBy(Time.of(Duration.ofSeconds(1))
            .using(e -> 1L)) // ~ force event time
        .output()
        .persist(output);

    new TestFlinkExecutor().submit(f).get();

    assertEquals(
        Arrays.asList(
            Pair.of("foo", "bar"),
            Pair.of("quux", "ibis"))
            .stream().sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList()),
        output.getOutput(0)
            .stream()
            .sorted(Comparator.comparing(Pair::getFirst))
            .collect(Collectors.toList()));
  }
}