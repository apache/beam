package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowingRequiredException;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinWindowEnforcement extends OperatorTest {

  static abstract class WindowEnforcementJoinTestCase<LEFT, RIGHT, OUT>
      extends JoinTest.JoinTestCase<LEFT, RIGHT, OUT> {
    protected Class<? extends Exception> expectFailure() {
      return null;
    }
  }

  @Override
  protected void runTestCase(Executor executor, Settings settings, TestCase tc)
      throws Exception {
    if (tc instanceof WindowEnforcementJoinTestCase) {
      Class expEx = ((WindowEnforcementJoinTestCase) tc).expectFailure();
      if (expEx != null) {
        try {
          super.runTestCase(executor, settings, tc);
        } catch (Exception e) {
          Throwable t = e;
          do {
            if (expEx.isAssignableFrom(t.getClass())) {
              // ~ good; the exc was expected
              return;
            }
            t = t.getCause();
          } while (t != null);
          Assert.fail("Expected " + expEx + " but got " + e);
          return;
        }
        Assert.fail("Expected " + expEx + " but got nothing");
        return;
      }
    }
    super.runTestCase(executor, settings, tc);
  }

  @Override
  protected List<TestCase> getTestCases() {
    Object[][] params = {
        /* left-windowing, right-windowing, join-windowing, expected-failure */
        {null, null, null, false},
        {Batch.get(), Batch.get(), null, false},
        {Batch.get(), null, null, false},
        {null, Batch.get(), null, false},
        {Time.of(Duration.ofMinutes(1)), null, null, true},
        {null, Time.of(Duration.ofMinutes(1)), null, true},
        {Time.of(Duration.ofMinutes(1)), Time.of(Duration.ofMinutes(1)), null, true},
        {Batch.get(), Time.of(Duration.ofMinutes(1)), null, true},
        {Time.of(Duration.ofMinutes(1)), Batch.get(), null, true},
        {Time.of(Duration.ofMinutes(1)), null, Time.of(Duration.ofHours(1)), false},
        {Batch.get(), Time.of(Duration.ofMinutes(1)), Time.of(Duration.ofMinutes(1)), false},
        {null, Time.of(Duration.ofMinutes(1)), Batch.get(), false},
        {Time.of(Duration.ofMinutes(1)), null, Count.of(10), false},
        {Time.of(Duration.ofMinutes(1)), Count.of(11), Batch.get(), false},
        {Time.of(Duration.ofMinutes(1)), Count.of(11), Time.of(Duration.ofMinutes(1)), false}
    };
    return Arrays.stream(params)
        .flatMap(ps ->
            Stream.of(
                testWindowValidity(false, (Windowing) ps[0], (Windowing) ps[1],
                    (Windowing) ps[2], (Boolean) ps[3]),
                testWindowValidity(true, (Windowing) ps[0], (Windowing) ps[1],
                    (Windowing) ps[2], (Boolean) ps[3])))
        .collect(Collectors.toList());
  }

  TestCase testWindowValidity(
      boolean batch,
      Windowing leftWindowing, Windowing rightWindowing, Windowing joinWindowing,
      boolean expectInvalidJoinFailure)
  {
    return new WindowEnforcementJoinTestCase<Object, Object, Pair<Object, Object>>() {
      @Override
      protected Class<? extends Exception> expectFailure() {
        if (expectInvalidJoinFailure) {
          return WindowingRequiredException.class;
        }
        return null;
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List partitions) {
        // ~ nothing to validate here
      }

      @Override
      protected Dataset<Pair<Object, Object>>
      getOutput(Dataset<Object> left, Dataset<Object> right) {
        // FIXME test more combinations

        // ~ prepare left input
        {
          ReduceByKey.DatasetBuilder4<Object, Object, Object, Object> leftBuilder =
              ReduceByKey.of(left)
                  .keyBy(e -> e)
                  .valueBy(e -> e)
                  .combineBy(xs -> xs.iterator().next());
          final Dataset<Pair<Object, Object>> leftWindowed;
          if (leftWindowing == null) {
            leftWindowed = leftBuilder.output();
          } else {
            leftWindowed = leftBuilder.windowBy(leftWindowing).output();
          }
          left = MapElements.of(leftWindowed)
              .using(Pair::getFirst)
              .output();
        }

        // ~ prepare right input
        {
          ReduceByKey.DatasetBuilder4<Object, Object, Object, Object> rightBuilder =
              ReduceByKey.of(right)
                  .keyBy(e -> e)
                  .valueBy(e -> e)
                  .combineBy(xs -> xs.iterator().next());
          final Dataset<Pair<Object, Object>> rightWindowed;
          if (rightWindowing == null) {
            rightWindowed = rightBuilder.output();
          } else {
            rightWindowed = rightBuilder.windowBy(rightWindowing).output();
          }
          right = MapElements.of(rightWindowed)
              .using(Pair::getFirst)
              .output();
        }

        Join.WindowingBuilder<Object, Object, Object, Object> joinBuilder =
            Join.of(left, right)
            .by(e -> e, e -> e)
            .using((l, r, c) -> c.collect(new Object()))
            .setPartitioner(e -> 0);
        if (joinWindowing == null) {
          return joinBuilder.output();
        } else {
          return joinBuilder.windowBy(joinWindowing).output();
        }
      }

      @Override
      protected Dataset<Object> getLeftInput(Flow flow) {
        return flow.createInput(ListDataSource.of(batch, new ArrayList<>()));
      }

      @Override
      protected Dataset<Object> getRightInput(Flow flow) {
        return flow.createInput(ListDataSource.of(batch, new ArrayList<>()));
      }
    };
  }

}
