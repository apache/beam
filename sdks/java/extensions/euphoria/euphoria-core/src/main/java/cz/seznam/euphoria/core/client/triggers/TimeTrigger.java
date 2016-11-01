package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that fires once the time passes the end of the window.
 */
public class TimeTrigger implements Trigger<TimeInterval> {

  private static final Logger LOG = LoggerFactory.getLogger(TimeTrigger.class);

  @Override
  public boolean isStateful() {
    return false;
  }

  @Override
  public TriggerResult onElement(long time, TimeInterval window, TriggerContext ctx) {
    return registerTimer(window, ctx);
  }

  @Override
  public TriggerResult onTimer(long time, TimeInterval window, TriggerContext ctx) {
    if (time == window.getEndMillis()) {
      LOG.debug("Firing TimeTrigger, time {}, window: {}", time, window);
      return TriggerResult.FLUSH_AND_PURGE;
    }
    return TriggerResult.NOOP;
  }

  @Override
  public void onClear(TimeInterval window, TriggerContext ctx) {
    ctx.deleteTimer(window.getEndMillis(), window);
  }

  @Override
  public TriggerResult onMerge(TimeInterval window, TriggerContext.TriggerMergeContext ctx) {
    return registerTimer(window, ctx);
  }

  private TriggerResult registerTimer(TimeInterval window, TriggerContext ctx) {
    if (ctx.registerTimer(window.getEndMillis(), window)) {
      return TriggerResult.NOOP;
    }

    // if the time already passed discard the late coming element
    return TriggerResult.PURGE;
  }
}
