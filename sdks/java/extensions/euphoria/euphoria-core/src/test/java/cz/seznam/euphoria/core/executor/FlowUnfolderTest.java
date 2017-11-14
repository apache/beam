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
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.MockStreamDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.FlowUnfolder.InputOperator;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * {@code FlowUnfolder} test suite.
 */
public class FlowUnfolderTest {

  private Flow flow;
  private Dataset<Object> input;

  @Before
  public void before() throws Exception {
    flow = Flow.create(getClass().getSimpleName());
    input = flow.createInput(new MockStreamDataSource<>());

    Dataset<Object> mapped = MapElements.of(input).using(e -> e).output();
    Dataset<Pair<Object, Long>> reduced = ReduceByKey
        .of(mapped)
        .keyBy(e -> e).reduceBy(values -> 1L)
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    Dataset<Pair<Object, Long>> output = Join.of(mapped, reduced)
        .by(e -> e, Pair::getFirst)
        .using((Object l, Pair<Object, Long> r, Collector<Long> c) -> c.collect(r.getSecond()))
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    output.persist(new StdoutSink<>());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnfoldSimple() {
    Set<Class<? extends Operator<?, ?>>> allowed = new HashSet<>();
    allowed.addAll((List) Arrays.asList(MapElements.class, ReduceByKey.class, Join.class));
    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, allowed);
    // there are 4 nodes, one is the input node
    assertEquals(4, unfolded.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnfoldBasic() {
    DAG<Operator<?, ?>> unfolded = FlowUnfolder.unfold(flow, Executor.getBasicOps());
    // InputOperator -> Map
    // Map -> FlatMap
    // ReduceByKey -> ReduceStateByKey
    // Join -> Map, Map, Union, ReduceStateByKey
    assertEquals(7, unfolded.size());
    // single root - InputNode
    assertEquals(1, unfolded.getRoots().size());
    Node<Operator<?, ?>> root = unfolded.getRoots().stream().findFirst().get();
    assertEquals(InputOperator.class, root.get().getClass());
    // single input consumer - FlatMap
    assertEquals(1, root.getChildren().size());
    Node<Operator<?, ?>> map = root.getChildren().get(0);
    assertEquals(FlatMap.class, map.get().getClass());
    // FlatMap is consumed by ReduceStateByKey and the first FlatMap of expanded Join
    assertEquals(2, map.getChildren().size());
    java.util.Map<Class<? extends Operator>, Node<Operator<?, ?>>> childrenMap;
    childrenMap = toClassMap((List) map.getChildren());
    assertTrue(childrenMap.containsKey(FlatMap.class));
    assertTrue(childrenMap.containsKey(ReduceStateByKey.class));
    // the FlatMap path is then consumed by Union
    Node<Operator<?, ?>> firstFlatMap = childrenMap.get(FlatMap.class);
    Node<Operator<?, ?>> firstReduceStateByKey = childrenMap.get(
        ReduceStateByKey.class);

    Node<Operator<?, ?>> union =  getOnlyAndValidate(
        firstFlatMap.getChildren(), Union.class);
    // the union has single ReduceStateByKey consumer
    Node<Operator<?, ?>> secondReduceStateByKey = getOnlyAndValidate(
        union.getChildren(), ReduceStateByKey.class);
    // this is the output operator
    assertNotNull(secondReduceStateByKey.get().output().getOutputSink());
    assertEquals(StdoutSink.class,
        secondReduceStateByKey.get().output().getOutputSink().getClass());
    Node<Operator<?, ?>> secondFlatMap = getOnlyAndValidate(
        firstReduceStateByKey.getChildren(), FlatMap.class);
    // the second flatMap is the second input to the union
    assertTrue(union == getOnlyAndValidate(secondFlatMap.getChildren(), Union.class));


  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testUnfoldableFlow() {
    FlowUnfolder.unfold(flow, (Set) Sets.newHashSet(FlatMap.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleOutputsToSameSink() throws Exception {
    flow = Flow.create(getClass().getSimpleName());
    input = flow.createInput(new MockStreamDataSource<>());

    Dataset<Object> mapped = MapElements.of(input).using(e -> e).output();
    Dataset<Pair<Object, Long>> reduced = ReduceByKey
        .of(mapped)
        .keyBy(e -> e).reduceBy(values -> 1L)
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    Dataset<Pair<Object, Long>> output = Join.of(mapped, reduced)
        .by(e -> e, Pair::getFirst)
        .using((Object l, Pair<Object, Long> r, Collector<Long> c) -> {
          c.collect(r.getSecond());
        })
        .windowBy(Time.of(Duration.ofSeconds(1)))
        .output();

    ListDataSink<Pair<Object, Long>> sink = ListDataSink.get();
    output.persist(sink);
    reduced.persist(sink);
    FlowUnfolder.unfold(flow, Executor.getBasicOps());
  }

  private java.util.Map<Class<? extends Operator>, Node<Operator>>
  toClassMap(List<Node<Operator>> children) {
    return children.stream()
        .collect(Collectors.toMap(n -> n.get().getClass(), n -> n));
  }

  private Node<Operator<?, ?>> getOnlyAndValidate(
      List<Node<Operator<?, ?>>> nodes,
      Class<? extends Operator> childClass) {
    assertEquals(1, nodes.size());
    Node<Operator<?, ?>> child = nodes.iterator().next();
    assertEquals(childClass, child.get().getClass());
    return child;
  }

}
