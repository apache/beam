

package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.FlowAwareDataset;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * A general stream.
 */
abstract class PStream<T> extends FlowAwareDataset<T> {

  public PStream(Flow flow) {
    super(flow);
  }
 
  @Override
  public final boolean isBounded() {
    return false;
  }

}
