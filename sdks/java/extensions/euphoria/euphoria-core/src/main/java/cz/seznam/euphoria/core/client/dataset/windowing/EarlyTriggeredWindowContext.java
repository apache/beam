package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.PeriodicTimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.time.Duration;

/**
 * Decorates {@link Window} with early triggering ability.
 */
abstract class EarlyTriggeredWindowContext<GROUP, LABEL>
    extends WindowContext<GROUP, LABEL> {

  private final Trigger earlyTrigger;

  /**
   * @param interval Window is periodically triggered on given time interval.
   * @param endOfWindow End of window timestamp (early trigger won't be triggered
   *                    after that time)
   */
  public EarlyTriggeredWindowContext(
      WindowID<GROUP, LABEL> windowID,
      Duration interval, long endOfWindow) {
    super(windowID);
    if (interval != null) {
      // ~ last early trigger needs to be fired no later than end of window
      long lastFireTime = (endOfWindow - 1) - (endOfWindow - 1) % interval.toMillis();

      this.earlyTrigger = new PeriodicTimeTrigger(interval.toMillis(), lastFireTime);
    } else {
      this.earlyTrigger = null;
    }
  }

  boolean isEarlyTriggered() {
    return earlyTrigger != null;
  }

  public Trigger getEarlyTrigger() {
    return earlyTrigger;
  }
}
