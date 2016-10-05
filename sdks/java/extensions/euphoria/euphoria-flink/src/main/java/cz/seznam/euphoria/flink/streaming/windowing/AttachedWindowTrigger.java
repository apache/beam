package cz.seznam.euphoria.flink.streaming.windowing;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

public class AttachedWindowTrigger<LABEL, T>
    extends Trigger<T, AttachedWindow<LABEL>>
{
  @Override
  public TriggerResult onElement(T element,
                                 long timestamp,
                                 AttachedWindow<LABEL> window,
                                 TriggerContext ctx)
      throws Exception
  {
    ctx.registerEventTimeTimer(window.getEmissionWatermark());
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time, AttachedWindow<LABEL> window, TriggerContext ctx)
      throws Exception
  {
    throw new UnsupportedOperationException("processing time not supported!");
  }

  @Override
  public TriggerResult onEventTime(long time, AttachedWindow<LABEL> window, TriggerContext ctx)
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
  public void clear(AttachedWindow<LABEL> window, TriggerContext ctx)
      throws Exception
  {
    // ~ attached-windows are purged only when their trigger fires in which case
    // the trigger itself get's clear; however, attached windows have
    // maxTimestamp == Long.MAX_VALUE and we need to clean-up the registered
    // clean-up trigger to avoid mem-leak in long running streams
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }
}
