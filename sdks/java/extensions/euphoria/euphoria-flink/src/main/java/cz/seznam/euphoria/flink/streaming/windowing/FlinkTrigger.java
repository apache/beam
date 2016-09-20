
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

import java.util.List;
import java.util.Objects;


public class FlinkTrigger<T> extends Trigger<T, FlinkWindowID> {

  private final Windowing windowing;

  public FlinkTrigger(Windowing windowing) {
    this.windowing = Objects.requireNonNull(windowing);
  }

  @Override
  public TriggerResult onElement(T element,
                                 long timestamp,
                                 FlinkWindowID window,
                                 TriggerContext ctx)  {

    @SuppressWarnings("unchecked")
    WindowContext eCtx = windowing.createWindowContext(window.getWindowID());

    @SuppressWarnings("unchecked")
    List<cz.seznam.euphoria.core.client.triggers.Trigger> eTriggers =
        eCtx.createTriggers();

    TriggerResult tr = TriggerResult.PURGE;
    for (cz.seznam.euphoria.core.client.triggers.Trigger t : eTriggers) {
      cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult sched =
          t.schedule(eCtx, new TriggerContextWrapper(ctx));
      if (sched != cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult.PASSED) {
        tr = translateResult(sched);
      }
    }
    return tr;
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        FlinkWindowID window,
                                        TriggerContext ctx) {
    return onTimeEvent(time, window, ctx);
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   FlinkWindowID window,
                                   TriggerContext ctx) {

    return onTimeEvent(time, window, ctx);
  }

  private TriggerResult onTimeEvent(long time,
                                    FlinkWindowID window,
                                    TriggerContext ctx) {

    @SuppressWarnings("unchecked")
    WindowContext windowCtx = windowing.createWindowContext(window.getWindowID());
    TriggerContextWrapper triggerContext = new TriggerContextWrapper(ctx);
    TriggerResult r = TriggerResult.CONTINUE;
    for (cz.seznam.euphoria.core.client.triggers.Trigger trigger :
        (List< cz.seznam.euphoria.core.client.triggers.Trigger>) windowCtx.createTriggers()) {
      r = TriggerResult.merge(r, translateResult(trigger.onTimeEvent(time, windowCtx, triggerContext)));
    }
    return r;
  }

  private TriggerResult translateResult(
      cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult euphoriaResult) {

    switch (euphoriaResult) {
      case FLUSH:
        return TriggerResult.FIRE;
      case FLUSH_AND_PURGE:
        return TriggerResult.FIRE_AND_PURGE;
      case NOOP:
        return TriggerResult.CONTINUE;
      case PURGE:
        return TriggerResult.PURGE;
      case PASSED:
        return TriggerResult.CONTINUE;
      default:
        throw new IllegalStateException("Unknown result:" + euphoriaResult.name());
    }
  }

  @Override
  public void clear(FlinkWindowID window, TriggerContext ctx) throws Exception {
    // ~ our flink-window-ids windows have maxTimestamp == Long.MAX_VALUE; need to
    // clean-up the registered clean-up trigger to avoid mem-leak in long running
    // streams
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }
}
