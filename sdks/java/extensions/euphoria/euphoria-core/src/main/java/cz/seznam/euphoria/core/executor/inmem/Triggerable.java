package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

@FunctionalInterface
public interface Triggerable<W extends Window, K> {

  /**
   * This method is invoked with the timestamp for which the trigger was scheduled.
   * <p>
   * If the triggering is delayed for whatever reason (trigger timer was blocked, JVM stalled due
   * to a garbage collection), the timestamp supplied to this function will still be the original
   * timestamp for which the trigger was scheduled.
   *
   * @param timestamp The timestamp for which the trigger event was scheduled.
   */
  void fire(long timestamp, KeyedWindow<W, K> window);
}
