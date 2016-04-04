
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * Stream on input of a flow.
 */
public abstract class InputPStream<T> extends PStream<T> {

  private final DataSource<T> source;

  public InputPStream(Flow flow, DataSource<T> source) {
    super(flow);
    this.source = source;
  }


  @Override
  public DataSource<T> getSource() {
    return source;
  }

  @Override
  public Operator<?, T, PStream<T>> getProducer() {
    return null;
  }

  
  @Override
  public void persist(DataSink<T> sink) {
    throw new UnsupportedOperationException("Do not persist inputs!");
  }


  @Override
  public void checkpoint(DataSink<T> sink) {
    throw new UnsupportedOperationException("Do not checkpoint inputs!");
  }


}
