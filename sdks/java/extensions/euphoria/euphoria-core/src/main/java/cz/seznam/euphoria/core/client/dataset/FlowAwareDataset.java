
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;

/**
 * A dataset registered in flow.
 */
public abstract class FlowAwareDataset<T> implements Dataset<T> {

  private final Flow flow;

  public FlowAwareDataset(Flow flow) {
    this.flow = flow;
  }

  @Override
  public Flow getFlow() {
    return flow;
  }

  @Override
  public Collection<Operator<?, ?>> getConsumers() {
    return flow.getConsumersOf(this);
  }


}
