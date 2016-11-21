package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.WindowWiseOperator;
import cz.seznam.euphoria.core.client.operator.WindowingRequiredException;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Validate invariants. Throw exceptions if any invariant is violated.
 */
class FlowValidator {

  /**
   * Validates the {@code DAG} representing a user defined flow just before
   * translation by {@link FlowUnfolder#unfold}.
   *
   * @param dag the user defined flow as a DAG
   *
   * @return the input dag if the validation succeeds
   *
   * @throws WindowingRequiredException if the given DAG contains operators
   *          that require explicit windowing strategies to make meaningful
   *          executions
   */
  static DAG<Operator<?, ?>> preTranslateValidate(DAG<Operator<?, ?>> dag) {
    checkJoinWindowing(dag);
    return dag;
  }

  /**
   * Validate the {@code DAG} after translation by {@link FlowUnfolder#unfold}.
   *
   * @return the input dag if the validation succeeds
   */
  static DAG<Operator<?, ?>> postTranslateValidate(DAG<Operator<?, ?>> dag) {
    checkSinks(dag);
    return dag;
  }

  /**
   * Validate that join operators' windowing semantics can be meaningfully
   * implemented. I.e. only instances which join "batched" windowed data sets
   * do not need to explicitly be provided with a user defined windowing strategy.
   */
  private static void checkJoinWindowing(DAG<Operator<?, ?>> dag) {
    List<Node<Operator<?, ?>>> joins = dag.traverse()
        .filter(node -> node.get() instanceof Join)
        .collect(Collectors.toList());
    for (Node<Operator<?, ?>> join : joins) {
      checkJoinWindowing((Node) join);
    }
  }

  private static void checkJoinWindowing(Node<Operator<?, ?>> node) {
    Preconditions.checkState(node.get() instanceof Join);

    // ~ if a windowing strategy is explicitly provided by the user, all is fine
    if (((Join) node.get()).getWindowing() != null) {
      return;
    }
    for (Node<Operator<?, ?>> parent : node.getParents()) {
      if (!isBatched(parent)) {
        throw new WindowingRequiredException(
            "Join operator requires either an explicit windowing" +
            " strategy or needs to be supplied with batched inputs.");
      }
    }
  }

  private static boolean isBatched(Node<Operator<?, ?>> node) {
    Operator<?, ?> operator = node.get();
    if (operator instanceof FlowUnfolder.InputOperator) {
      return true;
    }
    if (operator instanceof WindowWiseOperator) {
      Windowing windowing = ((WindowWiseOperator) operator).getWindowing();
      if (windowing != null) {
        return windowing instanceof Batch;
      }
    }
    List<Node<Operator<?, ?>>> parents = node.getParents();
    Preconditions.checkState(!parents.isEmpty(), "Non-input operator without parents?!");
    for (Node<Operator<?, ?>> parent : parents) {
      if (!isBatched(parent)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that no two output datasets use the same sink.
   * This is not supported, because we cannot clone the sink
   * (it can be used in client code after the flow has completed).
   */
  @SuppressWarnings("unchecked")
  private static void checkSinks(DAG<Operator<?, ?>> dag) {
    List<Pair<Dataset, DataSink>> outputs = dag.nodes()
        .filter(n -> n.output().getOutputSink() != null)
        .map(o -> Pair.of((Dataset) o.output(), (DataSink) o.output().getOutputSink()))
        .collect(Collectors.toList());
    
    Map<DataSink, Dataset> sinkDatasets = new HashMap<>();

    outputs.forEach(p -> {
      Dataset current = sinkDatasets.get(p.getSecond());
      if (current != null) {
        throw new IllegalArgumentException(
            "Operator " + current.getProducer().getName() + " and "
                + " operator " + p.getFirst().getProducer().getName()
                + " use the same sink " + p.getSecond());
      }
      sinkDatasets.put(p.getSecond(), p.getFirst());
    });
  }
}