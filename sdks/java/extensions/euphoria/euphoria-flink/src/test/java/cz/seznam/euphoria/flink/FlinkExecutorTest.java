package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class FlinkExecutorTest {

  @Test
  public void testFlatMap() throws Exception {

    Executor executor = new TestFlinkExecutor();

    Flow flow = Flow.create("xxx");

    Dataset<String> input =
            flow.createInput(ListDataSource.unbounded(Arrays.asList("a", "b")));

    Dataset<String> output = FlatMap.of(input)
            .using((String e, Collector<String> collector) -> collector.collect(e.toUpperCase()))
            .output();

    executor.waitForCompletion(flow);
  }

  @Test
  public void testUnion() throws Exception {

    Executor executor = new TestFlinkExecutor();

    Flow flow = Flow.create("xxx");

    Dataset<String> left =
            flow.createInput(ListDataSource.unbounded(Arrays.asList("a", "b")));
    Dataset<String> right =
            flow.createInput(ListDataSource.unbounded(Arrays.asList("a", "b")));


    Dataset<String> output = Union.of(left, right)
            .output();

    executor.waitForCompletion(flow);
  }
}