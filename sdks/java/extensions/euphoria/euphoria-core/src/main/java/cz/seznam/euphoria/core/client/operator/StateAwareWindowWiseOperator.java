
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator with internal state.
 */
public abstract class StateAwareWindowWiseOperator<
    IN, KEY, OUT, W extends Window<?, W>, TYPE extends Dataset<OUT>,
    OP extends StateAwareWindowWiseOperator<IN, KEY, OUT, W, TYPE, OP>>
    extends WindowWiseOperator<IN, OUT, W, TYPE> implements StateAware<IN, KEY> {


  protected final UnaryFunction<IN, KEY> keyExtractor;
  protected Partitioning<KEY> partitioning;


  protected StateAwareWindowWiseOperator(
          String name, Flow flow, Windowing<IN, ?, W> windowing,
          UnaryFunction<IN, KEY> keyExtractor,
          Partitioning<KEY> partitioning)
  {
    super(name, flow, windowing);
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

  @SuppressWarnings("unchecked")
  public OP partitionBy(Partitioning<KEY> partitioning) {
    this.partitioning = partitioning;
    return (OP) this;
  }


}
