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

import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.WindowWiseOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.Node;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;

/** Validate invariants. Throw exceptions if any invariant is violated. */
class FlowValidator {

  /**
   * Validates the {@code DAG} representing a user defined flow just before translation by {@link
   * FlowUnfolder#unfold}.
   *
   * @param dag the user defined flow as a DAG
   * @return the input dag if the validation succeeds
   * @throws WindowingRequiredException if the given DAG contains operators that require explicit
   *     windowing strategies to make meaningful executions
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
   * Validate that join operators' windowing semantics can be meaningfully implemented. I.e. only
   * instances which join "batched" windowed data sets do not need to explicitly be provided with a
   * user defined windowing strategy.
   */
  private static void checkJoinWindowing(DAG<Operator<?, ?>> dag) {
    List<Node<Operator<?, ?>>> joins =
        dag.traverse().filter(node -> node.get() instanceof Join).collect(Collectors.toList());
    for (Node<Operator<?, ?>> join : joins) {
      checkJoinWindowing(join);
    }
  }

  private static void checkJoinWindowing(Node<Operator<?, ?>> node) {
    checkState(node.get() instanceof Join);

    // ~ if a windowing strategy is explicitly provided by the user, all is fine
    if (((Join) node.get()).getWindowing() != null) {
      return;
    }
    for (Node<Operator<?, ?>> parent : node.getParents()) {
      if (!isBatched(parent)) {
        throw new WindowingRequiredException(
            "Join operator requires either an explicit windowing"
                + " strategy or needs to be supplied with batched inputs.");
      }
    }
  }

  private static boolean isBatched(Node<Operator<?, ?>> node) {
    Operator<?, ?> operator = node.get();
    if (operator instanceof FlowUnfolder.InputOperator) {
      return true;
    }
    if (operator instanceof WindowWiseOperator) {
      WindowingDesc windowing = ((WindowWiseOperator) operator).getWindowing();
      if (windowing != null) {
        return windowing.getWindowFn() instanceof GlobalWindows;
      }
    }
    List<Node<Operator<?, ?>>> parents = node.getParents();
    checkState(!parents.isEmpty(), "Non-input operator without parents?!");
    for (Node<Operator<?, ?>> parent : parents) {
      if (!isBatched(parent)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that no two output datasets use the same sink. This is not supported, because we
   * cannot clone the sink (it can be used in client code after the flow has completed).
   */
  @SuppressWarnings("unchecked")
  private static void checkSinks(DAG<Operator<?, ?>> dag) {
    List<KV<Dataset, DataSink>> outputs =
        dag.nodes()
            .filter(n -> n.output().getOutputSink() != null)
            .map(o -> KV.of((Dataset) o.output(), (DataSink) o.output().getOutputSink()))
            .collect(Collectors.toList());

    Map<DataSink, Dataset> sinkDatasets = new HashMap<>();

    outputs.forEach(
        p -> {
          Dataset current = sinkDatasets.get(p.getValue());
          if (current != null) {
            throw new IllegalArgumentException(
                "Operator "
                    + current.getProducer().getName()
                    + " and "
                    + " operator "
                    + p.getKey().getProducer().getName()
                    + " use the same sink "
                    + p.getValue());
          }
          sinkDatasets.put(p.getValue(), p.getKey());
        });
  }
}
