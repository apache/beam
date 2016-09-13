package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;

import java.io.Serializable;
import java.util.Objects;

/**
 * Composite operator using two {@link Filter} operators to split
 * a {@link Dataset} into two subsets using provided {@link UnaryPredicate}.
 */
public class Split<IN> {

  static final String DEFAULT_NAME = "Split";
  static final String POSITIVE_FILTER_SUFFIX = "-positive";
  static final String NEGATIVE_FILTER_SUFFIX = "-negative";

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<IN>(DEFAULT_NAME, input);
  }

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = Objects.requireNonNull(name);
    }

    public <IN> Split.UsingBuilder<IN> of(Dataset<IN> input) {
      return new Split.UsingBuilder<>(name, input);
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public Split.OutputBuilder<IN> using(UnaryPredicate<IN> predicate) {
      return new Split.OutputBuilder<>(name, input, predicate);
    }
  }

  public static class OutputBuilder<IN> implements Serializable {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryPredicate<IN> predicate;

    OutputBuilder(String name, Dataset<IN> input, UnaryPredicate<IN> predicate) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.predicate = Objects.requireNonNull(predicate);
    }

    public Output<IN> output() {
      Flow flow = input.getFlow();

      Filter<IN> positiveFilter = new Filter<>(
          name + POSITIVE_FILTER_SUFFIX, flow, input, predicate);
      flow.add(positiveFilter);

      Filter<IN> negativeFilter = new Filter<>(
          name + NEGATIVE_FILTER_SUFFIX, flow, input,
          (UnaryPredicate<IN>) what -> !predicate.apply(what)
      );
      flow.add(negativeFilter);

      return new Output<>(positiveFilter.output(), negativeFilter.output());
    }
  }

  /**
   * Pair of positive and negative output as a result of the {@link Split} operator
   */
  public static class Output<T> {
    private final Dataset<T> positive;
    private final Dataset<T> negative;

    private Output(Dataset<T> positive, Dataset<T> negative) {
      this.positive = Objects.requireNonNull(positive);
      this.negative = Objects.requireNonNull(negative);
    }
    /**
     * @return positive split result
     */
    public Dataset<T> positive() {
      return positive;
    }
    /**
     * @return negative split result
     */
    public Dataset<T> negative() {
      return negative;
    }
  }

}
