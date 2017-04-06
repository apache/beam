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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps track of mapping between Euphoria {@link Operator} and
 * Spark output {@link JavaRDD}.
 */
public class SparkExecutorContext {

  private final JavaSparkContext env;
  private final DAG<Operator<?, ?>> dag;
  private final Map<Operator<?, ?>, JavaRDD<?>> outputs;

  public SparkExecutorContext(JavaSparkContext env, DAG<Operator<?, ?>> dag) {
    this.env = env;
    this.dag = dag;
    this.outputs = new IdentityHashMap<>();
  }

  public JavaSparkContext getExecutionEnvironment() {
    return this.env;
  }

  /**
   * Retrieve list of Spark {@link JavaRDD} inputs of given operator
   *
   * @param operator the operator whose input RDDs to return
   *
   * @return a list of input RDDs of the given operator; never {@code null}
   */
  public List<JavaRDD<?>> getInputs(Operator<?, ?> operator) {
    List<Node<Operator<?, ?>>> parents = dag.getNode(operator).getParents();
    List<JavaRDD<?>> inputs = new ArrayList<>(parents.size());
    for (Node<Operator<?, ?>> p : parents) {
      JavaRDD pout = outputs.get(dag.getNode(p.get()).get());
      if (pout == null) {
        throw new IllegalArgumentException(
                "Output DataStream/DataSet missing for operator " + p.get().getName());
      }
      inputs.add(pout);
    }
    return inputs;
  }

  /**
   * Retrieves a single Spark {@link JavaRDD} in case given operator has no more
   * than one input (single-input operator).
   *
   * @param operator the operator to inspect
   *
   * @return a single RDD represeting the operator only input
   *
   * @throws RuntimeException if the given operator has no or more than one inputs
   */
  public JavaRDD<?> getSingleInput(Operator<?, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  /**
   * Retrieves a Spark {@link JavaRDD} representing output of given operator
   *
   * @param operator the operator to inspect
   *
   * @return the given operator's output RDD
   *
   * @throws RuntimeException if the operator has no output RDD registered yet
   *
   * @see #setOutput(Operator, JavaRDD)
   */
  public JavaRDD<?> getOutput(Operator<?, ?> operator) {
    JavaRDD<?> out = outputs.get(operator);
    if (out == null) {
      throw new IllegalArgumentException("No output exists for operator " +
              operator.getName());
    }
    return out;
  }

  public void setOutput(Operator<?, ?> operator, JavaRDD<?> output) {
    JavaRDD<?> prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
              "Operator(" + operator.getName() + ") output already processed");
    }
  }
}
