
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.State;

import java.io.Serializable;
import java.util.Set;

/**
 * A bunch of elements joined into window.
 */
public interface Window<KEY, W extends Window<KEY, W>> extends Serializable {

  /**
   * Retrieve key of this window. Windows are grouped into groups based
   * on this key. Windows with different keys will never be merged with other
   * windows.
   */
  KEY getKey();


  /**
   * Register a function to be called by the triggering when a window
   * completion event occurs
   * @param triggering the registering service
   * @param evict the callback to be called when the trigger fires
   */
  void registerTrigger(Triggering triggering, UnaryFunction<W, Void> evict);


  /**
   * Add a state to this window.
   */
  void addState(State<?, ?> state);


  /**
   * Retrieve all states associated with this window.
   */
  Set<State<?, ?>> getStates();


  /**
   * Flush all states.
   */
  default void flushAll() {
    getStates().stream().forEach(State::flush);
  }

}
