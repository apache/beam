

package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * A general stream.
 */
public abstract class PStream<T> implements Dataset<T> {

  private final Flow flow;


  public PStream(Flow flow) {
    this.flow = flow;
  }

  
  @Override
  public Flow getFlow() {
    return flow;
  }


  @Override
  public final boolean isBounded() {
    return false;
  }

}
