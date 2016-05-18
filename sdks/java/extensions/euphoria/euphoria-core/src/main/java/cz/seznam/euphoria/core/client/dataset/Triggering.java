
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Triggering conditions that have to be met in order to fire an event.
 */
public interface Triggering extends Serializable {

  /**
   * Fire specific trigger on given time.
   * Schedule the given trigger at the given stamp.
   * The trigger will be fired as close to the time as possible.
   * @return true if the trigger was activated, false if time of the activation
   * already passed
   */
  boolean scheduleAt(long stamp, Trigger trigger);

  /**
   * Update the internal timestamp by processed element (optional operation).
   */
  default void updateProcessed(long stamp) {
    // nop
  }

  /**
   * Close all triggers and destroy the triggering.
   */
  void close();
  
}
