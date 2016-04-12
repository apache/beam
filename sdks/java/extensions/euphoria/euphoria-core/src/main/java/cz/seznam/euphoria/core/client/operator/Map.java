
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;

/**
 * Count preserving element transformation operation.
 */
public class Map<IN, OUT, TYPE extends Dataset<OUT>>
    extends ElementWiseOperator<IN, OUT, TYPE> {

  public static class Builder1<IN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <OUT> Map<IN, OUT, Dataset<OUT>> by(UnaryFunction<IN, OUT> mapper) {
      Flow flow = input.getFlow();
      Map<IN, OUT, Dataset<OUT>> map = new Map<>(flow, input, mapper);
      return flow.add(map);
    }
  }
  public static class BuilderBatch1<IN> {
    final PCollection<IN> input;
    BuilderBatch1(PCollection<IN> input) {
      this.input = input;
    }
    public <OUT> Map<IN, OUT, PCollection<OUT>> by(UnaryFunction<IN, OUT> mapper) {
      Flow flow = input.getFlow();
      Map<IN, OUT, PCollection<OUT>> map = new Map<>(flow, input, mapper);
      return flow.add(map);
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  public static <IN> BuilderBatch1<IN> of(PCollection<IN> input) {
    return new BuilderBatch1<>(input);
  }

  final UnaryFunction<IN, OUT> mapper;

  Map(Flow flow, Dataset<IN> input, UnaryFunction<IN, OUT> mapper) {
    super("Map", flow, input);
    this.mapper = mapper;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TYPE output() {
    return super.output();
  }

  /**
   * This is not a basic operator. It can be straightforwardly implemented
   * by using {@code FlatMap} operator.
   * @return the operator chain representing this operation including FlatMap
   */
  @Override
  public DAG<Operator<?, ?, ?>> getBasicOps() {
    return DAG.of(
        // do not use the client API here, because it modifies the Flow!
        new FlatMap<IN, OUT, TYPE>(getFlow(), input,
            (i, c) -> c.collect(mapper.apply(i))));
  }


}
