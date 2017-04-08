/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.SingleInputOperator;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Keeps track of mapping between Euphoria {@link Dataset} and
 * Flink output {@link DataStream} or {@link org.apache.flink.api.java.DataSet}.
 *
 * @param <E> type of the underlying environment, i.e. streaming or batched
 * @param <D> the type of the datasets handled in this context,
 *            either {@code Dataset} (for batch mode) or {@code DataStream} (for
 *            stream mode.)
 */
public abstract class ExecutorContext<E, D> {

  private final E env;
  private final DAG<FlinkOperator<?>> dag;
  private final Map<FlinkOperator<?>, D> outputs;

  public ExecutorContext(E env, DAG<FlinkOperator<?>> dag) {
    this.env = env;
    this.dag = dag;
    this.outputs = new IdentityHashMap<>();
  }

  public E getExecutionEnvironment() {
    return this.env;
  }

  /**
   * Retrieves list of Flink {@link DataStream} inputs of given operator
   *
   * @param operator the operator to inspect
   *
   * @return a list of all the specified operator's input streams; never {@code null}
   *
   * @throws IllegalArgumentException if the given operator has no inputs registered yet
   *
   * @see #setOutput(FlinkOperator, Object)
   */
  public List<D> getInputStreams(FlinkOperator<?> operator) {
    return getInputOperators(operator).stream()
            .map(p -> {
              D pout = outputs.get(dag.getNode(p).get());
              if (pout == null) {
                throw new IllegalArgumentException(
                        "Output DataStream/DataSet missing for operator " + p.getName());
              }
              return pout;
            })
            .collect(toList());
  }

  /**
   * Retrieves a list of the producers of the given operator's inputs. This
   * corresponds to {@link #getInputStreams(FlinkOperator)}
   *
   * @param operator the operator whose input producers to retrieve
   *
   * @return a list of all the specified opertor's input producers; never {@code null}
   *
   * @see #getInputStreams(FlinkOperator)
   */
  public List<FlinkOperator<?>> getInputOperators(FlinkOperator<?> operator) {
    return dag.getNode(requireNonNull(operator)).getParents().stream()
            .map(Node::get)
            .collect(toList());
  }

  /**
   * Assumes the specified operator is a single-input-operator.
   *
   * @param operator the operator to inspect
   *
   * @return the given operator's single input data set/stream
   *
   * @throws RuntimeException if the operator has no or more than one input
   *
   * @see #getInputStreams(FlinkOperator)
   * @see #setOutput(FlinkOperator, Object)
   */
  public D getSingleInputStream(FlinkOperator<? extends SingleInputOperator> operator) {
    return Iterables.getOnlyElement(getInputStreams(operator));
  }

  public D getOutputStream(FlinkOperator<?> operator) {
    D out = outputs.get(operator);
    if (out == null) {
      throw new IllegalArgumentException("No output exists for operator " +
              operator.getName());
    }
    return out;
  }

  public void setOutput(FlinkOperator<?> operator, D output) {
    D prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
