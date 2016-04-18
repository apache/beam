
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * A {@code PStream} as a result of GroupByKey operation.
 */
public abstract class GroupedOutputPStream<KEY, T>
    extends OutputPStream<Pair<KEY, T>>
    implements GroupedDataset<KEY, T> {

  public GroupedOutputPStream(
      Flow flow, Operator<?, Pair<KEY, T>, PStream<Pair<KEY, T>>> producer) {
    super(flow, producer);
  }

}
