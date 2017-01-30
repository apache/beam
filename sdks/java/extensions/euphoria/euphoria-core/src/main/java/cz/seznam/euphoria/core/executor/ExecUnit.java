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

package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * FIXME: this description is WRONG!
 * An {@code ExecUnit} is a series of transformation with no checkpointing.
 * {@code ExecUnit} has several inputs, several outputs and possibly
 * some intermediate datasets. Datasets might be shared across multiple
 * {@code ExecUnit}s.
 */
public class ExecUnit {

  /** All inputs to this exec unit. */
  final List<Dataset<?>> inputs = new ArrayList<>();
  /** All outputs of this exec unit. */
  final List<Dataset<?>> outputs = new ArrayList<>();
  /** All dag consisting this exec unit. */
  final DAG<Operator<?, ?>> operators;

  /** Split Flow into series of execution units. */
  public static List<ExecUnit> split(DAG<Operator<?, ?>> unfoldedFlow) {
    // FIXME: what exactly and for is unit?
    return Arrays.asList(new ExecUnit(unfoldedFlow));
  }


  private ExecUnit(DAG<Operator<?, ?>> operators) {
    this.operators = operators;
  }


  /** Retrieve leaf operators. */
  public Collection<Node<Operator<?, ?>>> getLeafs() {
    return operators.getLeafs();
  }


  /** Retrieve the DAG of operators. */
  public DAG<Operator<?, ?>> getDAG() {
    return operators;
  }


  /** Retrieve all inputs of this unit. */
  public Collection<Dataset<?>> getInputs() {
    return inputs;
  }


  /** Retrieve all outputs of this unit. */
  public Collection<Dataset<?>> getOutputs() {
    return outputs;
  }
  

  /** Retrieve exec paths for this unit. */
  public Collection<ExecPath> getPaths() {
    Collection<Node<Operator<?, ?>>> leafs = operators.getLeafs();
    return leafs.stream()
        .map(l -> ExecPath.of(operators.parentSubGraph(l.get())))
        .collect(Collectors.toList());
  }

}
