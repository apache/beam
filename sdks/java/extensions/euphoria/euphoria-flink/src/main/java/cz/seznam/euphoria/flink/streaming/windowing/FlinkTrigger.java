
package cz.seznam.euphoria.flink.streaming.windowing;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;


public class FlinkTrigger<T> extends Trigger<T, FlinkWindow> {

  private final WindowingMode mode;

  public FlinkTrigger(WindowingMode mode) {
    this.mode = mode;
  }

  @Override
  public TriggerResult onElement(T element,
                                 long timestamp,
                                 FlinkWindow window,
                                 TriggerContext ctx)  {
    
    // initialize window triggers if this is a first element
    if (window.isOpened()) {
      return TriggerResult.CONTINUE;
    } else {
      window.open();

      TriggerResult tr = TriggerResult.PURGE;
      for (cz.seznam.euphoria.core.client.triggers.Trigger t : window.getTriggers()) {
        cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult sched =
            t.schedule(window.getWindowContext(), new TriggerContextWrapper(mode, ctx));
        if (sched != cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult.PASSED) {
          tr = translateResult(sched);
        }
      }
      return tr;
    }
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        FlinkWindow window,
                                        TriggerContext ctx) {
    
    return onTimeEvent(time, window, ctx);
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   FlinkWindow window,
                                   TriggerContext ctx) {

    return onTimeEvent(time, window, ctx);
  }

  private TriggerResult onTimeEvent(long time,
                                    FlinkWindow window,
                                    TriggerContext ctx) {
    // check all registered triggers
    return window.getTriggers().stream()
            // get results from all triggers
            .map(t -> t.onTimeEvent(time,
                    window.getWindowContext(),
                    new TriggerContextWrapper(mode, ctx)))
            // remap results to Flink results
            .map(this::translateResult)
            // merge results
            .reduce(TriggerResult::merge)
            .orElse(TriggerResult.CONTINUE);

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
}
