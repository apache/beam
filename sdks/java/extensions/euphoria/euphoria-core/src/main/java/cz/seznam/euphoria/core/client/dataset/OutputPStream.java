
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * A stream dataset.
 */
public abstract class OutputPStream<T> extends PStream<T>{

  private final Operator<?, T, PStream<T>> producer;
  private DataSink<T> outputSink;
  private DataSink<T> checkpointSink;
  
  protected OutputPStream(Flow flow, Operator<?, T, PStream<T>> producer) {
    super(flow);
    this.producer = producer;
  }

  @Override
  public DataSource<T> getSource() {
    return null;
  }

  @Override
  public Operator<?, T, PStream<T>> getProducer() {
    return producer;
  }


  @Override
  public void persist(DataSink<T> sink)
  {
    outputSink = sink;
  }

  @Override
  public void checkpoint(DataSink<T> sink)
  {
    checkpointSink = sink;
  }

  @Override
  public DataSink<T> getOutputSink() {
    return outputSink;

  }
  @Override
  public DataSink<T> getCheckpointSink() {
    return checkpointSink;
  }


}
