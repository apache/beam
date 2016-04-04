
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * A {@code Dataset} that can be persisted to output sink.
 */
public abstract class PersistableDataset<T> implements Dataset<T> {
  
  private final Flow flow;
  private final Operator<?, T, Dataset<T>> producer;

  private DataSink<T> outputSink = null;
  
  protected PersistableDataset(Flow flow, Operator<?, T, Dataset<T>> producer) {
    this.flow = flow;
    this.producer = producer;
  }

  @Override
  public Flow getFlow() {
    return flow;
  }

  @Override
  public DataSource<T> getSource() {
    return null;
  }

  @Override
  public Operator<?, T, Dataset<T>> getProducer() {
    return producer;
  }

  @Override
  public void persist(DataSink<T> sink) {
    if (outputSink != null) {
      throw new IllegalStateException(
          "persist() called twice on this dataset. Dataset can only be "
              + "persisted once.");
    }
    this.outputSink = sink;
  }



}
