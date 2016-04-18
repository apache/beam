

package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * A general stream.
 */
public abstract class PStream<T> extends FlowAwareDataset<T> {

  public PStream(Flow flow) {
    super(flow);
  }
 
  @Override
  public final boolean isBounded() {
    return false;
  }

}
