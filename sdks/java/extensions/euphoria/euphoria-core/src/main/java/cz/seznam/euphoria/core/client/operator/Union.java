
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Basic;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Union of two datasets of same type.
 */
@Basic
public class Union<IN> extends Operator<IN, IN> implements OutputBuilder<IN> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> OutputBuilder<IN> of(Dataset<IN> left, Dataset<IN> right)
    {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }

      return new OutputBuilder<>(name, left, right);
    }
  }

  public static class OutputBuilder<IN>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<IN> {
    private final String name;
    private final Dataset<IN> left;
    private final Dataset<IN> right;
    OutputBuilder(String name, Dataset<IN> left, Dataset<IN> right) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
    }

    @Override
    public Dataset<IN> output() {
      Flow flow = left.getFlow();
      Union<IN> union = new Union<>(name, flow, left, right);
      flow.add(union);
      return union.output();
    }
  }

  public static <IN> OutputBuilder<IN> of(Dataset<IN> left, Dataset<IN> right) {
    return new OfBuilder("Union").of(left, right);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final Dataset<IN> left;
  final Dataset<IN> right;
  final Dataset<IN> output;

  @SuppressWarnings("unchecked")
  Union(String name, Flow flow, Dataset<IN> left, Dataset<IN> right) {
    super(name, flow);
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
