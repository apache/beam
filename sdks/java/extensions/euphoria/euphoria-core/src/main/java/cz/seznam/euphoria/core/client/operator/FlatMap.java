

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Flat map operator on dataset.
 */
public class FlatMap<IN, OUT> extends ElementWiseOperator<IN, OUT> {
  
  public static class Builder<IN> {
    final Dataset<IN> input;
    Builder(Dataset<IN> input) {
      this.input = input;
    }
    public <OUT> FlatMap<IN, OUT> by(UnaryFunctor<IN, OUT> functor) {
      Flow flow = input.getFlow();
      FlatMap<IN, OUT> flatMap = new FlatMap<>(
          input.getFlow(), input, functor);
      return flow.add(flatMap);
    }
  }

  public static <IN> Builder<IN> of(Dataset<IN> input) {
    return new Builder<>(input);
  }

  private final UnaryFunctor<IN, OUT> functor;

  public FlatMap(Flow flow, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
    super("FlatMap", flow, input);
    this.functor = functor;
  }

  public UnaryFunctor<IN, OUT> getFunctor() {
    return functor;
  }


}
