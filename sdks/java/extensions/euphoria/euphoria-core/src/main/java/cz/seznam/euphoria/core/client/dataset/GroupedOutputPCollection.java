
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * An {@code OutputPCollection} as a result of GroupBy operator.
 */
public abstract class GroupedOutputPCollection<KEY, T>
    extends OutputPCollection<Pair<KEY, T>>
    implements GroupedDataset<KEY, T> {

  public GroupedOutputPCollection(
      Flow flow, Operator<?, Pair<KEY, T>, PCollection<Pair<KEY, T>>> producer) {
    super(flow, producer);
  }


}
