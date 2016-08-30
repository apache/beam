package cz.seznam.euphoria.flink.streaming.windowing;


import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

/**
 * Adapts Flink {@link Trigger.TriggerContext} to be used as trigger context in
 * Euphoria API
 */
public class TriggerContextWrapper implements TriggerContext {

  private final WindowingMode mode;
  private final Trigger.TriggerContext flinkContext;

  public TriggerContextWrapper(WindowingMode mode,
                               Trigger.TriggerContext flinkContext)
  {
    this.mode = mode;
    this.flinkContext = flinkContext;
  }

  @Override
  public boolean scheduleTriggerAt(long stamp,
                                   WindowContext w,
                                   cz.seznam.euphoria.core.client.triggers.Trigger trigger)
  {
    if (mode == WindowingMode.EVENT) {
      if (stamp <= flinkContext.getCurrentWatermark()) return false;
      flinkContext.registerEventTimeTimer(stamp);
    } else {
      long x = flinkContext.getCurrentProcessingTime();
      if (stamp <= flinkContext.getCurrentProcessingTime()) return false;
      flinkContext.registerProcessingTimeTimer(stamp);
    }

    return true;
  }

  @Override
  public long getCurrentTimestamp() {
    if (mode == WindowingMode.EVENT) {
      return flinkContext.getCurrentWatermark();
    }
    else {
      return flinkContext.getCurrentProcessingTime();
    }
  }
}
