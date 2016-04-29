
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;

/**
 * Count preserving element transformation operation.
 */
public class Map<IN, OUT> extends ElementWiseOperator<IN, OUT> {

  public static class Builder1<IN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <OUT> Map<IN, OUT> by(UnaryFunction<IN, OUT> mapper) {
      Flow flow = input.getFlow();
      Map<IN, OUT> map = new Map<>(flow, input, mapper);
      return flow.add(map);
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }


  final UnaryFunction<IN, OUT> mapper;

  Map(Flow flow, Dataset<IN> input, UnaryFunction<IN, OUT> mapper) {
    super("Map", flow, input);
    this.mapper = mapper;
  }

  /**
   * This is not a basic operator. It can be straightforwardly implemented
   * by using {@code FlatMap} operator.
   * @return the operator chain representing this operation including FlatMap
   */
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(
        // do not use the client API here, because it modifies the Flow!
        new FlatMap<IN, OUT>(getFlow(), input,
            (i, c) -> c.collect(mapper.apply(i))));
  }


}
