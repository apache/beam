
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
   */
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(this);
  }

  /** List all input datasets. */
  public abstract Collection<Dataset<IN>> listInputs();

  /**
   * Create a new dataset that will be output of this operator.
   * This is used when creating operator outputs.
   */
  protected final Dataset<OUT> createOutput(final Dataset<IN> input) {
    Flow flow = input.getFlow();
    return Datasets.createOutputFor(flow, input, this);
  }

  /**
   * Retrieve output dataset.
   */
  public abstract Dataset<OUT> output();

}
