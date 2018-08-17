/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.hint.SizeHint;
import cz.seznam.euphoria.core.client.util.Pair;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

/** Test operator Join.  */
public class JoinTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<Pair<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                })
            .output();

    assertEquals(flow, joined.getFlow());
    assertEquals(1, flow.size());

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(flow, join.getFlow());
    assertEquals("Join1", join.getName());
    assertNotNull(join.leftKeyExtractor);
    assertNotNull(join.rightKeyExtractor);
    assertEquals(joined, join.output());
    assertNull(join.getWindowing());
    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void testBuild_OutputValues() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<String> joined =
        Join.named("JoinValues")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                })
            .outputValues();

    assertEquals(flow, joined.getFlow());
    assertEquals(2, flow.size());

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(flow, join.getFlow());
    assertEquals("JoinValues", join.getName());
    assertNotNull(join.getLeftKeyExtractor());
    assertNotNull(join.getRightKeyExtractor());
    assertNull(join.getWindowing());
    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void testBuild_WithCounters() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<Pair<Integer, String>> joined =
        Join.named("Join1")
            .of(left, right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  c.getCounter("my-counter").increment();
                  c.collect(l + r);
                })
            .output();

    assertEquals(flow, joined.getFlow());
    assertEquals(1, flow.size());

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(flow, join.getFlow());
    assertEquals("Join1", join.getName());
    assertNotNull(join.leftKeyExtractor);
    assertNotNull(join.rightKeyExtractor);
    assertEquals(joined, join.output());
    assertNull(join.getWindowing());
    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Join.of(left, right)
        .by(String::length, String::length)
        .using(
            (String l, String r, Collector<String> c) -> {
              // no-op
            })
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertEquals("Join", join.getName());
  }

  @Test
  public void testBuild_LeftJoin() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    LeftJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using(
            (String l, Optional<String> r, Collector<String> c) -> {
              // no-op
            })
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(Join.Type.LEFT, join.getType());
  }

  @Test
  public void testBuild_RightJoin() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    RightJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using(
            (Optional<String> l, String r, Collector<String> c) -> {
              // no-op
            })
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(Join.Type.RIGHT, join.getType());
  }

  @Test
  public void testBuild_FullJoin() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    FullJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using(
            (Optional<String> l, Optional<String> r, Collector<String> c) ->
                c.collect(l.orElse(null) + r.orElse(null)))
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertEquals(Join.Type.FULL, join.getType());
  }

  @Test
  public void testBuild_Windowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Join.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> c.collect(l + r))
        .windowBy(Time.of(Duration.ofHours(1)))
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getWindowing() instanceof Time);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuild_Hints() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Dataset<String> outputDataset =
        Join.named("Join1")
            .of(
                MapElements.of(left)
                    .using(i -> i)
                    .output(new Util.TestHint(), new Util.TestHint2()),
                right)
            .by(String::length, String::length)
            .using(
                (String l, String r, Collector<String> c) -> {
                  // no-op
                })
            .outputValues(SizeHint.FITS_IN_MEMORY);

    assertTrue(outputDataset.getProducer().getHints().contains(SizeHint.FITS_IN_MEMORY));

    Join join = (Join) flow.operators().stream().filter(op -> op instanceof Join).findFirst().get();
    assertTrue(
        join.listInputs()
            .stream()
            .anyMatch(
                input -> ((Dataset) input).getProducer().getHints().contains(new Util.TestHint())));

    assertTrue(
        join.listInputs()
            .stream()
            .anyMatch(
                input ->
                    ((Dataset) input).getProducer().getHints().contains(new Util.TestHint2())));

    assertEquals(
        2,
        ((Dataset) join.listInputs().stream().findFirst().get()).getProducer().getHints().size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuild_Hints_afterWindowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    Join.named("Join1")
        .of(
            MapElements.of(left)
                .using(i -> i)
                .output(new Util.TestHint(), new Util.TestHint2(), new Util.TestHint2()),
            right)
        .by(String::length, String::length)
        .using(
            (String l, String r, Collector<String> c) -> {
              // no-op
            })
        .windowBy(Time.of(Duration.ofHours(1)))
        .output();

    Join join = (Join) flow.operators().stream().filter(op -> op instanceof Join).findFirst().get();
    assertTrue(
        join.listInputs()
            .stream()
            .anyMatch(
                input -> ((Dataset) input).getProducer().getHints().contains(new Util.TestHint())));

    assertTrue(
        join.listInputs()
            .stream()
            .anyMatch(
                input ->
                    ((Dataset) input).getProducer().getHints().contains(new Util.TestHint2())));

    assertEquals(
        2,
        ((Dataset) join.listInputs().stream().findFirst().get()).getProducer().getHints().size());

    assertTrue(join.getWindowing() instanceof Time);
  }
}
