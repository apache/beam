
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * A series of transformations with single output operator.
 */
public class ExecPath {

  /** A DAG of operators. */
  private final DAG<Operator<?, ?, ?>> operators;
  

  private ExecPath(DAG<Operator<?, ?, ?>> operators) {
    this.operators = operators;
  }


  /**
   * Create new ExecPath.
   */
  static ExecPath of(DAG<Operator<?, ?, ?>> operators) {
    return new ExecPath(operators);
  }

  public DAG<Operator<?, ?, ?>> operators() {
    return operators;
  }

}
