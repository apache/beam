
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;

/**
 * {@code PCollection} that is output of some operator.
 */
abstract class OutputDataset<T> implements Dataset<T> {

  private final Flow flow;
  private final Operator<?, T> producer;
  private final boolean bounded;

  private DataSink<T> outputSink = null;
  private DataSink<T> checkpointSink = null;

  public OutputDataset(Flow flow, Operator<?, T> producer, boolean bounded) {
    this.flow = flow;
    this.producer = producer;
    this.bounded = bounded;
  }

  @Override
  public DataSource<T> getSource() {
    return null;
  }

  @Override
  public Operator<?, T> getProducer() {
    return producer;
  }

  @Override
  public void persist(DataSink<T> sink) {
    outputSink = sink;
  }

  @Override
  public void checkpoint(DataSink<T> sink) {
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

  @Override
  public Flow getFlow() {
    return flow;
  }

  @Override
  public boolean isBounded() {
    return bounded;
  }

  @Override
  public Collection<Operator<?, ?>> getConsumers() {
    return flow.getConsumersOf(this);
  }


}
