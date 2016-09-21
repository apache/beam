
package cz.seznam.euphoria.core.client.operator.state;

/**
 * State storage for single value.
 */
public interface ValueStorage<T> extends Storage<T> {

  /** Set the value in this state. */
  void set(T value);

  /** Retrieve the value. */
  T get();

}
