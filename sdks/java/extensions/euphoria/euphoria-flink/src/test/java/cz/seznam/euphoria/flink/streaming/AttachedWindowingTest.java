package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.flink.FlinkExecutor;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

public class AttachedWindowingTest {
  @Test
  public void testAttachedWindow() throws Exception {
    ListDataSource<Pair<String, Integer>> source =
        ListDataSource.unbounded(
            Arrays.asList(
                Pair.of("one", 1),
                Pair.of("one", 2),
                Pair.of("two", 3),
                Pair.of("two", 4),
                Pair.of("three", 5)))
            .withReadDelay(Duration.ofMillis(200));

    Flow f = Flow.create("test-attached-windowing");
    ReduceByKey.of(f.createInput(source))
        .keyBy(Pair::getFirst)
        .valueBy(e -> 1L)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10))
            .using(e -> System.currentTimeMillis()))
        .output()
    .persist(new StdoutSink<>(false, null));

    new TestFlinkExecutor().waitForCompletion(f);
  }
}
