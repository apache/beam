

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

import java.util.Objects;

/**
 * Flat map operator on dataset.
 */
public class FlatMap<IN, OUT> extends ElementWiseOperator<IN, OUT> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> UsingBuilder<IN> of(Dataset<IN> input) {
      return new UsingBuilder<>(name, input);
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunctor<IN, OUT> functor) {
      return new OutputBuilder<>(name, input, functor);
    }
  }

  public static class OutputBuilder<IN, OUT> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunctor<IN, OUT> functor;

    OutputBuilder(String name, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
      this.name = name;
      this.input = input;
      this.functor = functor;
    }

    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      FlatMap<IN, OUT> map = new FlatMap<>(name, flow, input, functor);
      flow.add(map);

      return map.output();
    }
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("FlatMap", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final UnaryFunctor<IN, OUT> functor;

  FlatMap(String name, Flow flow, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
    super(name, flow, input);
    this.functor = functor;
  }

  public UnaryFunctor<IN, OUT> getFunctor() {
    return functor;
  }


}
