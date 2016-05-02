
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.Collection;

/**
 * {@code PCollection} that is input of a {@code Flow}.
 */
abstract class InputDataset<T> implements Dataset<T> {

  private final Flow flow;
  private final DataSource<T> source;
  private final boolean bounded;

  public InputDataset(Flow flow, DataSource<T> source, boolean bounded) {
    this.flow = flow;
    this.source = source;
    this.bounded = bounded;
  }

  @Override
  public DataSource<T> getSource() {
    return source;
  }

  @Override
  public Operator<?, T> getProducer() {
    return null;
  }

  @Override
  public void persist(DataSink<T> sink) {
    throw new UnsupportedOperationException(
        "The input dataset is already stored.");
  }

  @Override
  public void checkpoint(DataSink<T> sink) {
    throw new UnsupportedOperationException("Do not checkpoint inputs.");
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
