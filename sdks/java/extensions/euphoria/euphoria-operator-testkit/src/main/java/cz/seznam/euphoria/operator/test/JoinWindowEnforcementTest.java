/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Count;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowingRequiredException;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Processing(Processing.Type.ALL)
public class JoinWindowEnforcementTest extends AbstractOperatorTest {

  @Parameterized.Parameters
  public static List<Object[]> testParameters() {
    Object[][] params = {
        /* left-windowing, right-windowing, join-windowing, expected-failure */
        {null, null, null, false},
        {GlobalWindowing.get(), GlobalWindowing.get(), null, false},
        {GlobalWindowing.get(), null, null, false},
        {null, GlobalWindowing.get(), null, false},
        {Time.of(Duration.ofMinutes(1)), null, null, true},
        {null, Time.of(Duration.ofMinutes(1)), null, true},
        {Time.of(Duration.ofMinutes(1)), Time.of(Duration.ofMinutes(1)), null, true},
        {GlobalWindowing.get(), Time.of(Duration.ofMinutes(1)), null, true},
        {Time.of(Duration.ofMinutes(1)), GlobalWindowing.get(), null, true},
        {Time.of(Duration.ofMinutes(1)), null, Time.of(Duration.ofHours(1)), false},
        {GlobalWindowing.get(), Time.of(Duration.ofMinutes(1)), Time.of(Duration.ofMinutes(1)), false},
        {null, Time.of(Duration.ofMinutes(1)), GlobalWindowing.get(), false},
        {Time.of(Duration.ofMinutes(1)), null, Count.of(10), false},
        {Time.of(Duration.ofMinutes(1)), Count.of(11), GlobalWindowing.get(), false},
        {Time.of(Duration.ofMinutes(1)), Count.of(11), Time.of(Duration.ofMinutes(1)), false}
    };
    return Arrays.asList(params);
  }

  private final Windowing leftWindowing;
  private final Windowing rightWindowing;
  private final Windowing joinWindowing;
  private final boolean expectFailure;

  public JoinWindowEnforcementTest(
      Windowing leftWindowing, Windowing rightWindowing, Windowing joinWindowing,
      boolean expectFailure) {
    this.leftWindowing = leftWindowing;
    this.rightWindowing = rightWindowing;
    this.joinWindowing = joinWindowing;
    this.expectFailure = expectFailure;
  }

  @Test
  public void testWindowValidity() throws Exception {
    JoinTest.JoinTestCase<Object, Object, Pair<Object, Object>>
        test = new JoinTest.JoinTestCase<Object, Object, Pair<Object,Object>>() {

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(Partitions<Pair<Object, Object>> partitions) {
        // ~ nothing to validate here
      }

      @SuppressWarnings("unchecked")
      @Override
      protected Dataset<Pair<Object, Object>>
      getOutput(Dataset<Object> left, Dataset<Object> right) {
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
      protected Partitions<Object> getLeftInput() {
        return Partitions.add(new ArrayList<>()).build();
      }

      @Override
      protected Partitions<Object> getRightInput() {
        return Partitions.add(new ArrayList<>()).build();
      }
    };
    Exception thrown = null;
    try {
      execute(test);
    } catch (Exception e) {
      thrown = e;
    }

    if (expectFailure) {
      expectedFailure(thrown, WindowingRequiredException.class);
    } else if (thrown != null) {
      throw thrown;
    }
  }

  private void expectedFailure(Exception actual, Class<?> expected) {
    if (actual == null) {
      Assert.fail("Expected " + expected + " but got nothing!");
    }

    Throwable t = actual;
    do {
      if (expected.isAssignableFrom(t.getClass())) {
        // ~ good; the expected was thrown by the test
        return;
      }
      t = t.getCause();
    } while (t != null);
    Assert.fail("Expected " + expected + " but got " + actual);
  }
}
