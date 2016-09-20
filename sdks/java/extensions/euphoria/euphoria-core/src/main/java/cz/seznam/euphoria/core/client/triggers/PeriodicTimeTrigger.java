package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that is periodically fired based on given time interval.
 */
public class PeriodicTimeTrigger implements Trigger {

  private static final Logger LOG = LoggerFactory.getLogger(PeriodicTimeTrigger.class);

  private final long interval;
  private final long startTime;
  private final long lastFireTime;

  public PeriodicTimeTrigger(long interval, long startTime, long lastFireTime) {
    this.interval = interval;
    this.startTime = startTime;
    this.lastFireTime = lastFireTime;
  }

  @Override
  public TriggerResult schedule(WindowContext w, TriggerContext ctx) {
    if (scheduleNext(startTime, w, ctx)) {
      return TriggerResult.NOOP;
    }

    return TriggerResult.PASSED;
  }

  @Override
  public TriggerResult onTimeEvent(long time, WindowContext w, TriggerContext ctx) {
    LOG.debug("Firing PeriodicTimeTrigger, time {}, window: {}", time, w.getWindowID());

    // ~ reschedule the trigger
    scheduleNext(time, w, ctx);

    return TriggerResult.FLUSH;
  }

  /**
   * @return {@code false} when end of window reached
   */
  private boolean scheduleNext(long currentTime, WindowContext w, TriggerContext ctx) {
    long fire = currentTime;
    while ((fire += interval) <= lastFireTime) {
      if (ctx.scheduleTriggerAt(fire, w, this)) {
        return true;
      }
    }
    return false;
  }
}
