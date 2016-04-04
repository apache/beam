

package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator working with some state element-wise.
 */
public class StateAwareElementWiseOperator<IN, KEY, OUT, TYPE extends Dataset<OUT>>
    extends ElementWiseOperator<IN, OUT, TYPE> implements StateAware<IN, KEY> {

  private final UnaryFunction<IN, KEY> keyExtractor;
  private final Partitioning<KEY> partitioning;

  protected StateAwareElementWiseOperator(
          String name, Flow flow, Dataset<IN> input,
          UnaryFunction<IN, KEY> keyExtractor,
          Partitioning<KEY> partitioning) {

    super(name, flow, input);
    this.keyExtractor = keyExtractor;
    this.partitioning = partitioning;
  }

  @Override
  public UnaryFunction<IN, KEY> getKeyExtractor() {
    return keyExtractor;
  }

  @Override
  public Partitioning<KEY> getPartitioning() {
    return partitioning;
  }

}
