
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.List;

/**
 * Operator with single input.
 */
public abstract class MultipleInputsOperator<IN, OUT, TYPE extends Dataset<OUT>>
    extends Operator<IN, OUT, TYPE> {

  public MultipleInputsOperator(String name, Flow flow) {
    super(name, flow);
  }

  /** Retrieve inputs of this operator. */
  public abstract List<Dataset<IN>> getInputs();

}
