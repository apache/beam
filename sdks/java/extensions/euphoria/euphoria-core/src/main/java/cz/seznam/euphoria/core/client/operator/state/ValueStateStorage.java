
package cz.seznam.euphoria.core.client.operator.state;

/**
 * State storage for single value.
 */
public interface ValueStateStorage<T> extends StateStorage<T> {

  /** Set the value in this state. */
  void set(T value);

  /** Retrieve the value. */
  T get();

}
