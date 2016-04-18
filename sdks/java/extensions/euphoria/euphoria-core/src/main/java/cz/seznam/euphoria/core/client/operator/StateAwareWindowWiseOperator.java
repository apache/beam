
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
    IN, WIN, KIN, KEY, OUT, W extends Window<?>, TYPE extends Dataset<OUT>,
    OP extends StateAwareWindowWiseOperator<IN, WIN, KIN, KEY, OUT, W, TYPE, OP>>
    extends WindowWiseOperator<IN, WIN, OUT, W, TYPE> implements StateAware<KIN, KEY> {


  protected final UnaryFunction<KIN, KEY> keyExtractor;
  protected Partitioning<KEY> partitioning;


  protected StateAwareWindowWiseOperator(
          String name, Flow flow, Windowing<WIN, ?, W> windowing,
          UnaryFunction<KIN, KEY> keyExtractor,
          Partitioning<KEY> partitioning)
  {
    super(name, flow, windowing);
    this.keyExtractor = keyExtractor;
    this.partitioning = partitioning;
  }


  @Override
  public UnaryFunction<KIN, KEY> getKeyExtractor() {
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
