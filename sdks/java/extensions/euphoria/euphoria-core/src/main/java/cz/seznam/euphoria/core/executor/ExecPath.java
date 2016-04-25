
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * A series of transformations with single output operator.
 */
public class ExecPath {

  /** A DAG of operators. */
  private final DAG<Operator<?, ?, ?>> dag;
  

  private ExecPath(DAG<Operator<?, ?, ?>> dag) {
    this.dag = dag;
  }


  /**
   * Create new ExecPath.
   */
  static ExecPath of(DAG<Operator<?, ?, ?>> dag) {
    return new ExecPath(dag);
  }

  public DAG<Operator<?, ?, ?>> dag() {
    return dag;
  }

}
