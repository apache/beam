/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Datasets;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Time;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.MockStreamDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.StdoutSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.SingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.FlowUnfolder.InputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.Node;
import org.junit.Before;
import org.junit.Test;

/** {@code FlowUnfolder} test suite. */
public class FlowUnfolderTest {

  private Flow flow;
  private Dataset<Object> input;

  @Before
  public void setUp() throws Exception {
    flow = Flow.create(getClass().getSimpleName());
    input = flow.createInput(new MockStreamDataSource<>());

    Dataset<Object> mapped = MapElements.of(input).using(e -> e).output();
    Dataset<Pair<Object, Long>> reduced =
        ReduceByKey.of(mapped)
            .keyBy(e -> e)
            .reduceBy(values -> 1L)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();

    Dataset<Pair<Object, Long>> output =
        Join.of(mapped, reduced)
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
    Node<Operator<?, ?>> firstReduceStateByKey = childrenMap.get(ReduceStateByKey.class);

    Node<Operator<?, ?>> union = getOnlyAndValidate(firstFlatMap.getChildren(), Union.class);
    // the union has single ReduceStateByKey consumer
    Node<Operator<?, ?>> secondReduceStateByKey =
        getOnlyAndValidate(union.getChildren(), ReduceStateByKey.class);
    // this is the output operator
    assertNotNull(secondReduceStateByKey.get().output().getOutputSink());
    assertEquals(
        StdoutSink.class, secondReduceStateByKey.get().output().getOutputSink().getClass());
    Node<Operator<?, ?>> secondFlatMap =
        getOnlyAndValidate(firstReduceStateByKey.getChildren(), FlatMap.class);
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
    Dataset<Pair<Object, Long>> reduced =
        ReduceByKey.of(mapped)
            .keyBy(e -> e)
            .reduceBy(values -> 1L)
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();

    Dataset<Pair<Object, Long>> output =
        Join.of(mapped, reduced)
            .by(e -> e, Pair::getFirst)
            .using(
                (Object l, Pair<Object, Long> r, Collector<Long> c) -> {
                  c.collect(r.getSecond());
                })
            .windowBy(Time.of(Duration.ofSeconds(1)))
            .output();

    ListDataSink<Pair<Object, Long>> sink = ListDataSink.get();
    output.persist(sink);
    reduced.persist(sink);
    FlowUnfolder.unfold(flow, Executor.getBasicOps());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnfoldWithCustomInputOperator() {
    flow = Flow.create();
    MyInputOperator<Integer> source = flow.add(new MyInputOperator<>(flow));
    MySingleInputOperator<Integer, Integer> process =
        flow.add(new MySingleInputOperator<>(source.output()));
    DAG<Operator<?, ?>> dag = DAG.of(source);
    dag.add(process, source);
    DAG<Operator<?, ?>> unfolded =
        FlowUnfolder.unfold(
            flow,
            new HashSet<>(
                (List) Arrays.asList(MyInputOperator.class, MySingleInputOperator.class)));
    assertEquals(2, unfolded.size());
    assertEquals(1, unfolded.getRoots().size());
    assertEquals(1, unfolded.getLeafs().size());
    assertEquals(1, Iterables.getOnlyElement(unfolded.getLeafs()).getParents().size());
  }

  private java.util.Map<Class<? extends Operator>, Node<Operator>> toClassMap(
      List<Node<Operator>> children) {
    return children.stream().collect(Collectors.toMap(n -> n.get().getClass(), n -> n));
  }

  private Node<Operator<?, ?>> getOnlyAndValidate(
      List<Node<Operator<?, ?>>> nodes, Class<? extends Operator> childClass) {
    assertEquals(1, nodes.size());
    Node<Operator<?, ?>> child = nodes.iterator().next();
    assertEquals(childClass, child.get().getClass());
    return child;
  }

  static class MyInputOperator<T> extends Operator<Void, T> {

    Dataset<T> output = Datasets.createOutputFor(true, this);

    MyInputOperator(Flow flow) {
      super("MyInputOperator", flow);
    }

    @Override
    public Collection<Dataset<Void>> listInputs() {
      return Collections.emptyList();
    }

    @Override
    public Dataset<T> output() {
      return output;
    }
  }

  static class MySingleInputOperator<InputT, OutputT> extends SingleInputOperator<InputT, OutputT> {

    final Dataset<OutputT> output = Datasets.createOutputFor(true, this);

    MySingleInputOperator(Dataset<InputT> input) {
      super("MySingleInputOperator", input.getFlow(), input);
    }

    @Override
    public Dataset<OutputT> output() {
      return output;
    }
  }
}
