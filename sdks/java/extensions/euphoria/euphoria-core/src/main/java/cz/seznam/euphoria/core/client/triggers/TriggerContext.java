package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.Window;

/**
 * A context is given to {@link Trigger} methods to allow them to register
 * timer callbacks.
 */
public interface TriggerContext {

  /**
   * Fire specific trigger on given time.
   * Schedule the given trigger at the given stamp.
   * The trigger will be fired as close to the time as possible.
   * @return {@code true} when trigger was successfully scheduled
   */
  boolean scheduleTriggerAt(long stamp, Window w, Trigger trigger);

  /**
   * Return current timestamp from runtime (may be different from real
   * clock time).
   */
  long getCurrentTimestamp();
}
