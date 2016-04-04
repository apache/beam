
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * A batch dataset.
 */
public abstract class PCollection<T> implements Dataset<T> {


  private final Flow flow;


  public PCollection(Flow flow) {
    this.flow = flow;
  }

  @Override
  public final boolean isBounded() {
    return true;
  }

  @Override
  public Flow getFlow() {
    return flow;
  }



}
