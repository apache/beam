
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import java.util.Collection;

/**
 * A batch dataset.
 */
public abstract class PCollection<T> extends FlowAwareDataset<T> {

  public PCollection(Flow flow) {
    super(flow);
  }

  @Override
  public final boolean isBounded() {
    return true;
  }

}
