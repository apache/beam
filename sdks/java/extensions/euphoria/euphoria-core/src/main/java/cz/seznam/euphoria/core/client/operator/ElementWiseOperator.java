
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working element-wise, with no context between elements.
 * No windowing scheme is needed to be defined on input.
 */
public abstract class ElementWiseOperator<IN, OUT>
    extends SingleInputOperator<IN, OUT> {

  protected final Dataset<OUT> output;

  protected ElementWiseOperator(String name, Flow flow, Dataset<IN> input) {
    super(name, flow, input);
    this.output = createOutput(input);
  }
  
  @Override
  public Dataset<OUT> output() {
    return output;
  }
}
