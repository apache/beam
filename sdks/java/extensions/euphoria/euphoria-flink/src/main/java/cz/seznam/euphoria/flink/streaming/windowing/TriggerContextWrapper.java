package cz.seznam.euphoria.flink.streaming.windowing;


import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

/**
 * Adapts Flink {@link Trigger.TriggerContext} to be used as trigger context in
 * Euphoria API
 */
public class TriggerContextWrapper implements TriggerContext {

  private final Trigger.TriggerContext flinkContext;

  public TriggerContextWrapper(Trigger.TriggerContext flinkContext) {
    this.flinkContext = flinkContext;
  }

  @Override
  public boolean scheduleTriggerAt(long stamp,
                                   WindowContext w,
                                   cz.seznam.euphoria.core.client.triggers.Trigger trigger)
  {
    if (stamp <= flinkContext.getCurrentWatermark()) {
      return false;
    }
    flinkContext.registerEventTimeTimer(stamp);
    return true;
  }

  @Override
  public long getCurrentTimestamp() {
    return flinkContext.getCurrentWatermark();
  }
}
