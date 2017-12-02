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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.Assert.*;

public class JoinTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<Pair<Integer, String>> joined = InnerJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> {
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
  public void testBuild_WithCounters() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<Pair<Integer, String>> joined = InnerJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> {
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

    InnerJoin.of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> {
          // no-op
        })
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertEquals("InnerJoin", join.getName());
  }

  @Test
  public void testBuild_LeftJoin() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    LeftJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, Optional<String> r, Collector<String> c) -> {
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
        .using((Optional<String> l, String r, Collector<String> c) -> {
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
        .using((Optional<String> l, Optional<String> r, Collector<String> c) ->
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

    InnerJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> c.collect(l + r))
        .windowBy(Time.of(Duration.ofHours(1)))
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getWindowing() instanceof Time);
  }

  @Test
  public void testBuild_Hints() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    InnerJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> {
          // no-op
        })
        .withHints(Sets.newHashSet(new TestHint(), new TestHint2(), new TestHint2()))
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getHints().contains(new TestHint()));
    assertTrue(join.getHints().contains(new TestHint2()));
    assertEquals(2, join.getHints().size());
  }

  @Test
  public void testBuild_Hints_afterWindowing() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 1);
    Dataset<String> right = Util.createMockDataset(flow, 1);

    InnerJoin.named("Join1")
        .of(left, right)
        .by(String::length, String::length)
        .using((String l, String r, Collector<String> c) -> {
          // no-op
        })
        .windowBy(Time.of(Duration.ofHours(1)))
        .withHints(Sets.newHashSet(new TestHint(), new TestHint2(), new TestHint2()))
        .output();

    Join join = (Join) flow.operators().iterator().next();
    assertTrue(join.getHints().contains(new TestHint()));
    assertTrue(join.getHints().contains(new TestHint2()));
    assertEquals(2, join.getHints().size());
    assertTrue(join.getWindowing() instanceof Time);
  }

  private static class TestHint implements JoinHint {

  }

  private static class TestHint2 implements JoinHint {

  }
}