
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.io.Collector;

import java.io.Closeable;

/**
 * A state for stateful operations.
 */
public abstract class State<IN, OUT> implements Closeable {

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

  protected State(Collector<OUT> collector, StateStorageProvider storageProvider) {
    this.collector = collector;
    this.storageProvider = storageProvider;
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
