package cz.seznam.euphoria.fluent;

import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import org.junit.Test;

import java.util.Set;

import static java.util.Arrays.asList;

public class FluentTest {

  @Test
  public void testBasics() throws Exception {
    final long READ_DELAY_MS = 100L;
    ListDataSink<Set<String>> out = ListDataSink.get(1);
    Fluent.flow("Test")
        .read(ListDataSource.unbounded(
            asList("0-one 1-two 0-three 1-four 0-five 1-six 0-seven".split(" ")))
            .setSleepTime(READ_DELAY_MS))
        // ~ create windows of size three
        .apply(input -> ReduceByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet)
            .setNumPartitions(1)
            .windowBy(Count.of(3)))
        // ~ strip the needless key and flatten out the elements thereby
        // creating multiple elements in the output belonging to the same window
        .flatMap((Pair<String, Set<String>> e, Collector<String> c) ->
            e.getSecond().stream().forEachOrdered(c::collect))
        // ~ now spread the elements (belonging to the same window) over
        // multiple partitions
        .repartition(2, e -> '0' - e.charAt(0))
        // ~ now reduce all of the partitions to one
        .repartition(1)
        // ~ now process the single partition
        // ~ we now expect to reconstruct the same windowing
        // as the very initial step
        .apply(input -> ReduceByKey.of(input)
            .keyBy(e -> "")
            .valueBy(e -> e)
            .reduceBy((ReduceFunction<String, Set<String>>) Sets::newHashSet))
        // ~ strip the needless key
        .mapElements(Pair::getSecond)
        .persist(out)
        .execute(new InMemExecutor());
  }

}