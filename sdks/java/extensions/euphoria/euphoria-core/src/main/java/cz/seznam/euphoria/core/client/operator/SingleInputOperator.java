
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Arrays;
import java.util.Collection;

/**
 * Operator with single input.
 */
public abstract class SingleInputOperator<IN, OUT, TYPE extends Dataset<OUT>>
    extends Operator<IN, OUT, TYPE> {

  final Dataset<IN> input;

  protected SingleInputOperator(String name, Flow flow, Dataset<IN> input) {
    super(name, flow);
    this.input = input;
  }

  /** Retrieve input of this operator. */
  public Dataset<IN> input() {
    return input;
  }

  /** List all inputs (single input). */
  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(input);
  }



}
