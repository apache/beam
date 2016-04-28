

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
  final DAG<Operator<?, ?, ?>> operators;

  /** Split Flow into series of execution units. */
  public static List<ExecUnit> split(DAG<Operator<?, ?, ?>> unfoldedFlow) {
    // FIXME: what exactly and for is unit?
    return Arrays.asList(new ExecUnit(unfoldedFlow));
  }


  private ExecUnit(DAG<Operator<?, ?, ?>> operators) {
    this.operators = operators;
  }


  /** Retrieve leaf operators. */
  public Collection<Node<Operator<?, ?, ?>>> getLeafs() {
    return operators.getLeafs();
  }


  /** Retrieve the DAG of operators. */
  public DAG<Operator<?, ?, ?>> getDAG() {
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
    Collection<Node<Operator<?, ?, ?>>> leafs = operators.getLeafs();
    return leafs.stream()
        .map(l -> ExecPath.of(operators.parentSubGraph(l.get())))
        .collect(Collectors.toList());
  }

}
