
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Union of two datasets of same type.
 */
public class Union<IN, TYPE extends Dataset<IN>>
    extends TwoInputOperator<IN, IN, TYPE> implements Output<IN, TYPE> {


  public static <IN> Union<IN, Dataset<IN>> of(
      Dataset<IN> left, Dataset<IN> right) {
    return new Union<>(left.getFlow(), left, right);
  }

  public static <IN> Union<IN, PCollection<IN>> of(
      PCollection<IN> left, PCollection<IN> right) {
    return new Union<>(left.getFlow(), left, right);
  }

  final TYPE output;

  @SuppressWarnings("unchecked")
  public Union(Flow flow, TYPE left, TYPE right) {
    super("Union", flow, left, right);
    if (left.getFlow() != right.getFlow()) {
      throw new IllegalArgumentException("Pass two datasets from the same flow.");
    }
    output = (TYPE) createOutput(left);
  }

  @Override
  public TYPE output() {
    return output;
  }

}
