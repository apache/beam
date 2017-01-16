
package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * Schedules and fires registered triggers according to internal time.
 */
public interface TriggerScheduler<W extends Window, K> {

  /**
   * Fire specific trigger on given time.
   * Schedule the given trigger at the given stamp.
   * The trigger will be fired as close to the time as possible.
   *
   * @return true if the triggerable has been scheduled,
   *          false if the time already passed
   */
  boolean scheduleAt(long stamp, KeyedWindow<W, K> window, Triggerable<W, K> trigger);

  /**
   * Retrieve current timestamp this triggering is on.
   * This can be either a real system timestamp or the last
   * timestamp updated by call to `updateStamp'.
   */
  long getCurrentTimestamp();

  /**
   * Cancel all scheduled tasks
   */
  void cancelAll();

  /**
   * Cancel previously registered timer
   */
  void cancel(long stamp, KeyedWindow<W, K> window);

  /**
   * Update the internal timestamp (optional operation).
   */
  default void updateStamp(long stamp) {
    // nop
  }

  /**
   * Close all triggers and destroy the triggering.
   */
  void close();

}
