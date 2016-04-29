
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Union of two datasets of same type.
 */
// TODO: Should extend some kind of DoubleInputOperator?
public class Union<IN> extends Operator<IN, IN> {


  public static <IN> Union<IN> of(
      Dataset<IN> left, Dataset<IN> right) {
    Flow flow = left.getFlow();
    return flow.add(new Union<>(flow, left, right));
  }

  final Dataset<IN> left;
  final Dataset<IN> right;
  final Dataset<IN> output;

  @SuppressWarnings("unchecked")
  public Union(Flow flow, Dataset<IN> left, Dataset<IN> right) {
    super("Union", flow);
    this.left = Objects.requireNonNull(left);
    this.right = Objects.requireNonNull(right);

    if (left.getFlow() != right.getFlow()) {
      throw new IllegalArgumentException("Pass two datasets from the same flow.");
    }
    this.output = createOutput(left);
  }

  @Override
  public Dataset<IN> output() {
    return output;
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(left, right);
  }
}
