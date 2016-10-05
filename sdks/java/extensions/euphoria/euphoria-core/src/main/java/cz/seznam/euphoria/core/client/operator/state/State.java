
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.io.Context;

import java.io.Closeable;

/**
 * A state for stateful operations.
 */
public abstract class State<IN, OUT> implements Closeable {

  /** Collector of output of this state. */
  private final Context<OUT> context;
  /** Provider of state storage. */
  private final StorageProvider storageProvider;

  /** Add element to this state. */
  public abstract void add(IN element);

  /**
   * Flush the state to output. Invoked when window this
   * state is part of gets disposed/triggered.
   */
  public abstract void flush();

  protected State(
      Context<OUT> context,
      StorageProvider storageProvider) {
    
    this.context = context;
    this.storageProvider = storageProvider;
  }

  public Context<OUT> getContext() {
    return context;
  }

  public StorageProvider getStorageProvider() {
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
