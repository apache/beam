
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;

/**
 * {@code PCollection} that is input of a {@code Flow}.
 */
public abstract class InputPCollection<T> extends PCollection<T> {

  private final DataSource<T> source;

  public InputPCollection(Flow flow, DataSource<T> source) {
    super(flow);
    this.source = source;
  }

  @Override
  public DataSource<T> getSource() {
    return source;
  }

  @Override
  public Operator<?, T, PCollection<T>> getProducer() {
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

}
