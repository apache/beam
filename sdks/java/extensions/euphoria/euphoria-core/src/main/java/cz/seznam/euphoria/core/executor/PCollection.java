
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.FlowAwareDataset;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * A batch dataset.
 */
abstract class PCollection<T> extends FlowAwareDataset<T> {

  public PCollection(Flow flow) {
    super(flow);
  }

  @Override
  public final boolean isBounded() {
    return true;
  }

}
