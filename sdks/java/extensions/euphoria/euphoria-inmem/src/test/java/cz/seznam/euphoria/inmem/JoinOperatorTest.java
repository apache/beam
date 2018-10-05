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
package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static cz.seznam.euphoria.inmem.Util.sorted;
import static java.util.Arrays.asList;

public class JoinOperatorTest {

  private Executor executor;

  @Before
  public void setUp() {
    InMemExecutor exec = new InMemExecutor();
    // ~ see https://github.com/seznam/euphoria/issues/129
    exec.setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler<>(1));
    executor = exec;
  }

  static final class I<E> {
    E _e;
    long _time;

    static <E> I<E>of(E e) {
      return of(e, 0L);
    }

    static <E> I<E>of(E e, long time) {
      I<E> i = new I<>();
      i._e = e;
      i._time = time;
      return i;
    }
  }

  @SuppressWarnings("unchecked")
  private void testJoin(boolean outer,
                        Windowing windowing,
                        boolean bounded,
                        List<I<String>> leftInput,
                        List<I<String>> rightInput,
                        List<String> expectedOutput,
                        boolean makeOneArmLonger)
      throws Exception
  {
    Flow flow = Flow.create("Test");

    Dataset<String> first =
        MapElements.of(
            flow.createInput(ListDataSource.of(bounded, leftInput), i -> i._time))
            .using(i -> i._e)
            .output();
    Dataset<String> second =
        MapElements.of(
            flow.createInput(ListDataSource.of(bounded, rightInput), i -> i._time))
            .using(i -> i._e)
            .output();

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

    Assert.assertEquals(sorted(expectedOutput), sorted(out.getOutput(0)));
  }

  @Test
  public void testInnerJoinOnBatch() throws Exception {
    testJoin(false,
        GlobalWindowing.get(),
        true,
        asList(I.of("one 1"),  I.of("two 1"), I.of("one 22"),  I.of("one 44")),
        asList(I.of("one 10"), I.of("two 20"), I.of("one 33"), I.of("three 55"), I.of("one 66")),
        asList("one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21"),
        false);
  }

  @Test
  public void testInnerJoinOnStreams() throws Exception {
    testJoin(false,
        Time.of(Duration.ofSeconds(1)),
        false,
        asList(I.of("one 1",  1), I.of("two 1",  600),  I.of("one 22", 1001), I.of("one 44",   2000)),
        asList(I.of("one 10", 1), I.of("two 20", 501), I.of("one 33",  1999), I.of("three 55", 2001), I.of("one 66", 3000)),
        asList("one, 11" ,  "two, 21", "one, 55"),
        false);
  }

  @Test
  public void testOuterJoinOnBatch() throws Exception {
    testJoin(true,
        GlobalWindowing.get(),
        true,
        asList(I.of("one 1"),  I.of("two 1"), I.of("one 22"),  I.of("one 44")),
        asList(I.of("one 10"), I.of("two 20"), I.of("one 33"), I.of("three 55"), I.of("one 66")),
        asList(
            "one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21", "three, 55"),
        false);
  }

  @Test
  public void testOuterJoinOnStream() throws Exception {
    testJoin(true,
        Time.of(Duration.ofMillis(1)),
        false,
        asList(I.of("one 1", 0),  I.of("two 1", 1),  I.of("one 22", 3),  I.of("one 44", 4)),
        asList(I.of("one 10", 0), I.of("two 20", 1), I.of("one 33", 3), I.of("three 55", 4), I.of("one 66", 5)),
        asList("one, 11",  "two, 21", "one, 55", "one, 44", "three, 55", "one, 66"),
        false);
  }

  @Test
  public void testOneArmLongerJoin() throws Exception {
    testJoin(false,
        GlobalWindowing.get(),
        true,
        asList(I.of("one 1"),  I.of("two 1"), I.of("one 22"),  I.of("one 44")),
        asList(I.of("one 10"), I.of("two 20"), I.of("one 33"), I.of("three 55"), I.of("one 66")),
        asList("one, 11",  "one, 34", "one, 67",
            "one, 32", "one, 55", "one, 88", "one, 54",
            "one, 77", "one, 110", "two, 21"),
        true);
  }
}
