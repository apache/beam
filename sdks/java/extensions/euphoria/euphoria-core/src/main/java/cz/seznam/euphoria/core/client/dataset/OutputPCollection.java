
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * {@code PCollection} that is output of some operator.
 */
public abstract class OutputPCollection<T> extends PCollection<T> {

  private final Operator<?, T, PCollection<T>> producer;
  private DataSink<T> outputSink = null;
  private DataSink<T> checkpointSink = null;

  public OutputPCollection(Flow flow, Operator<?, T, PCollection<T>> producer) {
    super(flow);
    this.producer = producer;
  }

  @Override
  public DataSource<T> getSource() {
    return null;
  }

  @Override
  public Operator<?, T, PCollection<T>> getProducer() {
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
