
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Arrays;
import java.util.Collection;

/**
 * Operator having two inputs.
 */
public abstract class TwoInputOperator<IN, OUT, TYPE extends Dataset<OUT>>
    extends Operator<IN, OUT, TYPE> {

  final Dataset<IN> left;
  final Dataset<IN> right;

  TwoInputOperator(String name, Flow flow, Dataset<IN> left, Dataset<IN> right) {
    super(name, flow);
    this.left = left;
    this.right= right;
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(left, right);
  }

}
