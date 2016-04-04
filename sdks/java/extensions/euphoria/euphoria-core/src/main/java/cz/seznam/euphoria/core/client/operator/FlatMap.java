

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Flat map operator on dataset.
 */
public class FlatMap<IN, OUT, TYPE extends Dataset<OUT>>
    extends ElementWiseOperator<IN, OUT, TYPE> {
  
  public static class Builder<IN> {
    final Dataset<IN> input;
    Builder(Dataset<IN> input) {
      this.input = input;
    }
    public <OUT> FlatMap<IN, OUT, Dataset<OUT>> by(UnaryFunctor<IN, OUT> functor) {
      Flow flow = input.getFlow();
      FlatMap<IN, OUT, Dataset<OUT>> flatMap = new FlatMap<>(
          input.getFlow(), input, functor);
      return flow.add(flatMap);
    }
  }

  public static class BuilderBatch<IN> {
    final PCollection<IN> input;
    BuilderBatch(PCollection<IN> input) {
      this.input = input;
    }
    public <OUT> FlatMap<IN, OUT, PCollection<OUT>> by(UnaryFunctor<IN, OUT> functor) {
      Flow flow = input.getFlow();
      FlatMap<IN, OUT, PCollection<OUT>> flatMap = new FlatMap<>(flow, input, functor);
      return flow.add(flatMap);
    }
  }


  public static <IN> Builder<IN> of(Dataset<IN> input) {
    return new Builder<>(input);
  }

  public static <IN> BuilderBatch<IN> of(PCollection<IN> input) {
    return new BuilderBatch<>(input);
  }


  private final UnaryFunctor<IN, OUT> functor;

  public FlatMap(Flow flow, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
    super("FlatMap", flow, input);
    this.functor = functor;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TYPE output() {
    return (TYPE) super.output();
  }


  public UnaryFunctor<IN, OUT> getFunctor() {
    return functor;
  }


}
