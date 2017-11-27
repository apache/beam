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
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.WindowWiseOperator;
import cz.seznam.euphoria.core.client.operator.WindowingRequiredException;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;

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
  static DAG<Operator<?, ?>> preTranslate(DAG<Operator<?, ?>> dag) {
    checkJoinWindowing(dag);
    return dag;
  }

  /**
   * Validate the {@code DAG} after translation by {@link FlowUnfolder#unfold}.
   *
   * @return the input dag if the validation succeeds
   */
  static DAG<Operator<?, ?>> postTranslate(DAG<Operator<?, ?>> dag) {
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
      checkJoinWindowing(join);
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
        return windowing instanceof GlobalWindowing;
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