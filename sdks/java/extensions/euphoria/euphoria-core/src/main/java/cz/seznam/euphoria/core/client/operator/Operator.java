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
import cz.seznam.euphoria.core.client.dataset.Datasets;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;

import java.io.Serializable;
import java.util.Collection;

/**
 * An operator base class. All operators inherit his class.
 */
public abstract class Operator<IN, OUT> implements Serializable {
  
  /** Name of the operator. */
  private final String name;
  /** Associated Flow. */
  private final Flow flow;

  protected Operator(String name, Flow flow) {
    this.name = name;
    this.flow = flow;
  }

  public final String getName() {
    return name;
  }

  public final Flow getFlow() {
    return flow;
  }

  /**
   * Retrieve basic operators that constitute this operator.
   * Override this method for all non basic operators.
   *
   * @return a DAG of basic operators this operator can be translated to
   */
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(this);
  }

  /** @return a collection of all input datasets */
  public abstract Collection<Dataset<IN>> listInputs();

  /**
   * Create a new dataset that will be output of this operator.
   * This is used when creating operator outputs.
   *
   * @param input an input associated with this operator
   *
   * @return a newly created dataset associated with this operator as its output
   */
  final Dataset<OUT> createOutput(final Dataset<IN> input) {
    Flow flow = input.getFlow();
    return Datasets.createOutputFor(flow, input, this);
  }

  /**
   * @return the output dataset
   */
  public abstract Dataset<OUT> output();
}
