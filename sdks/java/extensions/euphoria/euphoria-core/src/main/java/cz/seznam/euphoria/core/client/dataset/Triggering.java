
package cz.seznam.euphoria.core.client.dataset;

import java.io.Serializable;

/**
 * Triggering conditions that have to be met in order to fire an event.
 */
public interface Triggering extends Serializable {

  /** Fire specific trigger on given time. */
  void scheduleOnce(long duration, Trigger trigger);

  /** Fire specific trigger each timeout milliseconds. */
  void schedulePeriodic(long timeout, Trigger trigger);
  

}
