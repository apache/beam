package cz.seznam.euphoria.flink.streaming.windowing;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

public class AttachedWindowTrigger<GROUP, LABEL, T>
    extends Trigger<T, AttachedWindow<GROUP, LABEL>>
{
  @Override
  public TriggerResult onElement(T element,
                                 long timestamp,
                                 AttachedWindow<GROUP, LABEL> window,
                                 TriggerContext ctx)
      throws Exception
  {
    if (window.getEmissionWatermark() < ctx.getCurrentWatermark()) {
      return TriggerResult.FIRE_AND_PURGE;
    } else {
      ctx.registerEventTimeTimer(window.getEmissionWatermark());
      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onProcessingTime(long time, AttachedWindow<GROUP, LABEL> window, TriggerContext ctx)
      throws Exception
  {
    throw new UnsupportedOperationException("processing time not supported!");
  }

  @Override
  public TriggerResult onEventTime(long time, AttachedWindow<GROUP, LABEL> window, TriggerContext ctx)
      throws Exception
  {
    if (window.getEmissionWatermark() == time) {
      return TriggerResult.FIRE_AND_PURGE;
    } else {
      // attached windows are registered _only_ for the maxTimestamp()
      throw new IllegalStateException("Invalid timer for attached window");
    }
  }

  @Override
  public void clear(AttachedWindow<GROUP, LABEL> window, TriggerContext ctx)
      throws Exception
  {
    ctx.deleteEventTimeTimer(window.getEmissionWatermark());
  }
}
