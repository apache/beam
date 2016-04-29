
package cz.seznam.euphoria.core.executor;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.MockStreamDataSourceFactory;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.Map;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.FlowUnfolder.InputOperator;
import cz.seznam.euphoria.core.util.Settings;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code FlowUnfolder} test suite.
 */
public class FlowUnfolderTest {

  Settings settings;
  Flow flow;
  Dataset<Object> input;

  @Before
  public void before() throws Exception {
    settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.mock",
        MockStreamDataSourceFactory.class);
    settings.setClass("euphoria.io.datasink.factory.stdout",
        StdoutSink.Factory.class);


    flow = Flow.create(getClass().getSimpleName(), settings);
    input = flow.createInput(URI.create("mock:///"));

    Dataset<Object> mapped = Map.of(input).by(e -> e).output();
    Dataset<Pair<Object, Long>> reduced = (Dataset<Pair<Object, Long>>) ReduceByKey
        .of(mapped)
        .keyBy(e -> e).reduceBy(values -> 1L)
        .windowBy(Windowing.Time.seconds(1))
        .output();

    Dataset<Pair<Object, Long>> output = Join.of(mapped, reduced)
        .by(e -> e, p -> p.getKey())
        .using((Object l, Pair<Object, Long> r, Collector<Long> c) -> {
          c.collect(r.getSecond());
        })
        .windowBy(Windowing.Time.seconds(1))
        .output();

    output.persist(URI.create("stdout:///"));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnfoldSimple() {
    Set<Class<? extends Operator<?, ?>>> allowed = new HashSet<>();
    allowed.addAll((List) Arrays.asList(Map.class, ReduceByKey.class, Join.class));
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
