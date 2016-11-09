package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static cz.seznam.euphoria.core.util.Util.sorted;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class JoinOperatorTest {

  private Executor executor;

  @Before
  public void setUp() {
    executor = new InMemExecutor();
  }

  @SuppressWarnings("unchecked")
  private void testJoin(boolean outer,
                        Windowing windowing,
                        Duration readDelay,
                        List<String> leftInput,
                        List<String> rightInput,
                        List<String> expectedOutput,
                        boolean makeOneArmLonger)
      throws Exception
  {
    Flow flow = Flow.create("Test");

    Dataset<String> first = flow.createInput(
        readDelay == null
            ? ListDataSource.bounded(leftInput)
            : ListDataSource.unbounded(leftInput).withReadDelay(readDelay));
    Dataset<String> second = flow.createInput(
        readDelay == null
            ? ListDataSource.bounded(rightInput)
            : ListDataSource.unbounded(rightInput).withReadDelay(readDelay));

    UnaryFunctor<String, Pair<String, Integer>> tokv = (s, c) -> {
      String[] parts = s.split("[\t ]+", 2);
      if (parts.length == 2) {
        c.collect(Pair.of(parts[0], Integer.valueOf(parts[1])));
      }
    };

    Dataset<Pair<String, Integer>> firstkv = FlatMap.of(first)
        .using(tokv)
        .output();
    Dataset<Pair<String, Integer>> secondkv = FlatMap.of(second)
        .using(tokv)
        .output();
    if (makeOneArmLonger) {
      secondkv = Filter.of(secondkv).by(e -> true).output();
      secondkv = MapElements.of(secondkv).using(e -> e).output();
    }

    Dataset<Pair<String, Object>> output = Join.of(firstkv, secondkv)
        .by(Pair::getFirst, Pair::getFirst)
        .using((l, r, c) ->
            c.collect((l == null ? 0 : l.getSecond()) + (r == null ? 0 : r.getSecond())))
        .applyIf(outer, b -> b.outer())
        .windowBy(windowing)
        .output();

    ListDataSink<String> out = ListDataSink.get(1);
    MapElements.of(output).using(p -> p.getFirst() + ", " + p.getSecond())
        .output().persist(out);

    executor.submit(flow).get();

    assertEquals(sorted(expectedOutput), sorted(out.getOutput(0)));
  }

  @Test
  public void testInnerJoinOnBatch() throws Exception {
    testJoin(false,
        Batch.get(),
        null,
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21"),
        false);
  }

  @Test
  public void testInnerJoinOnStreams() throws Exception {
    testJoin(false,
        Time.of(Duration.ofSeconds(1)),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "two, 21", "one, 55"),
        false);
  }

  @Test
  public void testOuterJoinOnBatch() throws Exception {
    testJoin(true,
        Batch.get(),
        null,
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList(
            "one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21", "three, 55"),
        false);
  }

  @Test
  public void testOuterJoinOnStream() throws Exception {
    testJoin(true,
        Time.of(Duration.ofSeconds(1)),
        Duration.ofSeconds(2),
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "two, 21", "one, 55", "one, 44", "three, 55", "one, 66"),
        false);
  }

  @Test
  public void testOneArmLongerJoin() throws Exception {
    testJoin(false,
        Batch.get(),
        null,
        asList("one 1",  "two 1", "one 22",  "one 44"),
        asList("one 10", "two 20", "one 33", "three 55", "one 66"),
        asList("one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21"),
        true);
  }
}
