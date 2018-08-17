package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Trigger} that fires once the time passes the end of the window.
 */
public class TimeTrigger implements Trigger {

  private static final Logger LOG = LoggerFactory.getLogger(TimeTrigger.class);

  private final long end;

  public TimeTrigger(long end) {
    this.end = end;
  }

  @Override
  public TriggerResult init(Window w, TriggerContext ctx) {
    if (ctx.scheduleTriggerAt(end, w, this)) {
      return TriggerResult.NOOP;
    }

    return TriggerResult.PASSED;
  }

  @Override
  public TriggerResult onTimeEvent(long time, Window w, TriggerContext ctx) {
    LOG.debug("Firing TimeTrigger, time {}, window: {}", time, w.getLabel());
    return TriggerResult.FLUSH_AND_PURGE;
  }
}
