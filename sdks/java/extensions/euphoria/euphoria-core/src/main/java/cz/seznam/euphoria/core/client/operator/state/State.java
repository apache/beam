
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.Operator;

import java.io.Closeable;
import java.io.Serializable;

/**
 * A state for stateful operations.
 */
public abstract class State<IN, OUT> implements Closeable, Serializable {

  /** Operator associated with this state. */
  private final Operator<?, ?> associatedOperator;
  /** Collector of output of this state. */
  private final Collector<OUT> collector;
  /** Provider of state storage. */
  private final StateStorageProvider storageProvider;

  /** Add element to this state. */
  public abstract void add(IN element);

  /**
   * Flush the state to output. Invoked when window this
   * state is part of gets disposed/triggered.
   */
  public abstract void flush();

  protected State(
      Operator<?, ?> associatedOperator,
      Collector<OUT> collector,
      StateStorageProvider storageProvider) {
    
    this.associatedOperator = associatedOperator;
    this.collector = collector;
    this.storageProvider = storageProvider;
  }

  public Operator<?, ?> getAssociatedOperator() {
    return associatedOperator;
  }

  public Collector<OUT> getCollector() {
    return collector;
  }

  public StateStorageProvider getStorageProvider() {
    return storageProvider;
  }

  /**
   * Closes this state. Invoked after {@link #flush()} and before
   * this state gets disposed.
   */
  public void close() {
    // ~ no-op by default
  }

}
