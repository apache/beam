package cz.seznam.euphoria.beam;

import static java.util.Arrays.asList;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.FullJoin;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.LeftJoin;
import cz.seznam.euphoria.core.client.operator.RightJoin;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.testing.DatasetAssert;
import java.util.Optional;
import org.junit.Test;

/**
 * Simple test suite for Join operator.
 */
public class JoinTest {

  @Test
  public void simpleInnerJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left =
        ListDataSource.bounded(
            asList(
                Pair.of(1, "L v1"), Pair.of(1, "L v2"),
                Pair.of(2, "L v1"), Pair.of(2, "L v2"),
                Pair.of(3, "L v1")
            ));

    ListDataSource<Pair<Integer, Integer>> right =
        ListDataSource.bounded(
            asList(
                Pair.of(1, 1), Pair.of(1, 10),
                Pair.of(2, 20),
                Pair.of(4, 40)
            ));

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    BinaryFunctor<Pair<Integer, String>, Pair<Integer, Integer>, Pair<String, Integer>> joiner =
        (l, r, c) -> c.collect(Pair.of(l.getSecond(), r.getSecond()));

    Join.of(flow.createInput(left), flow.createInput(right))
        .by(Pair::getFirst, Pair::getFirst)
        .using(joiner)
        .output()
        .persist(output);

    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)), Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)), Pair.of(1, Pair.of("L v2", 10)),

        Pair.of(2, Pair.of("L v1", 20)), Pair.of(2, Pair.of("L v2", 20))
    );

  }

  @Test
  public void simpleLeftJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left =
        ListDataSource.bounded(
            asList(
                Pair.of(1, "L v1"), Pair.of(1, "L v2"),
                Pair.of(2, "L v1"), Pair.of(2, "L v2"),
                Pair.of(3, "L v1")
            ));

    ListDataSource<Pair<Integer, Integer>> right =
        ListDataSource.bounded(
            asList(
                Pair.of(1, 1), Pair.of(1, 10),
                Pair.of(2, 20),
                Pair.of(4, 40)
            ));

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    BinaryFunctor<Pair<Integer, String>, Optional<Pair<Integer, Integer>>, Pair<String, Integer>>
        joiner = (l, r, c) ->
        c.collect(Pair.of(l.getSecond(), r.orElse(Pair.of(null, null)).getSecond()));

    LeftJoin.of(flow.createInput(left), flow.createInput(right))
        .by(Pair::getFirst, Pair::getFirst)
        .using(joiner)
        .output()
        .persist(output);

    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)), Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)), Pair.of(1, Pair.of("L v2", 10)),

        Pair.of(2, Pair.of("L v1", 20)), Pair.of(2, Pair.of("L v2", 20)),

        Pair.of(3, Pair.of("L v1", null))
    );

  }

  @Test
  public void simpleRightJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left =
        ListDataSource.bounded(
            asList(
                Pair.of(1, "L v1"), Pair.of(1, "L v2"),
                Pair.of(2, "L v1"), Pair.of(2, "L v2"),
                Pair.of(3, "L v1")
            ));

    ListDataSource<Pair<Integer, Integer>> right =
        ListDataSource.bounded(
            asList(
                Pair.of(1, 1), Pair.of(1, 10),
                Pair.of(2, 20),
                Pair.of(4, 40)
            ));

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    BinaryFunctor<Optional<Pair<Integer, String>>, Pair<Integer, Integer>, Pair<String, Integer>>
        joiner = (l, r, c) ->
        c.collect(Pair.of(l.orElse(Pair.of(null, null)).getSecond(), r.getSecond()));

    RightJoin.of(flow.createInput(left), flow.createInput(right))
        .by(Pair::getFirst, Pair::getFirst)
        .using(joiner)
        .output()
        .persist(output);

    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)), Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)), Pair.of(1, Pair.of("L v2", 10)),

        Pair.of(2, Pair.of("L v1", 20)), Pair.of(2, Pair.of("L v2", 20)),

        Pair.of(4, Pair.of(null, 40))
    );

  }

  @Test
  public void simpleFullJoinTest() {
    final Flow flow = Flow.create();

    ListDataSource<Pair<Integer, String>> left =
        ListDataSource.bounded(
            asList(
                Pair.of(1, "L v1"), Pair.of(1, "L v2"),
                Pair.of(2, "L v1"), Pair.of(2, "L v2"),
                Pair.of(3, "L v1")
            ));

    ListDataSource<Pair<Integer, Integer>> right =
        ListDataSource.bounded(
            asList(
                Pair.of(1, 1), Pair.of(1, 10),
                Pair.of(2, 20),
                Pair.of(4, 40)
            ));

    ListDataSink<Pair<Integer, Pair<String, Integer>>> output = ListDataSink.get();

    BinaryFunctor<
        Optional<Pair<Integer, String>>, Optional<Pair<Integer, Integer>>, Pair<String, Integer>>
        joiner = (l, r, c) -> c.collect(Pair.of(
        l.orElse(Pair.of(null, null)).getSecond(), r.orElse(Pair.of(null, null)).getSecond()));

    FullJoin.of(flow.createInput(left), flow.createInput(right))
        .by(Pair::getFirst, Pair::getFirst)
        .using(joiner)
        .output()
        .persist(output);

    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);

    DatasetAssert.unorderedEquals(output.getOutputs(),
        Pair.of(1, Pair.of("L v1", 1)), Pair.of(1, Pair.of("L v1", 10)),
        Pair.of(1, Pair.of("L v2", 1)), Pair.of(1, Pair.of("L v2", 10)),

        Pair.of(2, Pair.of("L v1", 20)), Pair.of(2, Pair.of("L v2", 20)),

        Pair.of(3, Pair.of("L v1", null)),
        Pair.of(4, Pair.of(null, 40))
    );

  }
  
}
