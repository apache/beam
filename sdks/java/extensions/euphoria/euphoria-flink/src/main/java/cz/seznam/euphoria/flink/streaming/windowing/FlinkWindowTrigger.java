package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;


public class FlinkWindowTrigger<WID extends Window, T> extends Trigger<T, FlinkWindow<WID>> {

  private final cz.seznam.euphoria.core.client.triggers.Trigger<T, WID> euphoriaTrigger;

  public FlinkWindowTrigger(
          cz.seznam.euphoria.core.client.triggers.Trigger<T, WID> trigger) {
    this.euphoriaTrigger = trigger;
  }

  @Override
  public TriggerResult onElement(
      T element, long timestamp, FlinkWindow<WID> window, TriggerContext ctx)
      throws Exception {
    return trackEmissionWatermark(
        window, ctx, onElementImpl(element, timestamp, window, ctx));
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        FlinkWindow<WID> window,
                                        TriggerContext ctx) throws Exception {
    return trackEmissionWatermark(window, ctx, onTimeEvent(time, window, ctx));
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   FlinkWindow<WID> window,
                                   TriggerContext ctx) throws Exception {

    return trackEmissionWatermark(window, ctx, onTimeEvent(time, window, ctx));
  }

  private TriggerResult onElementImpl(
      T element, long timestamp, FlinkWindow<WID> window, TriggerContext ctx)
      throws Exception {

    // pass onElement event to the original euphoria trigger
    return translateResult(
            euphoriaTrigger.onElement(
                    timestamp, element, window.getWindowID(), new TriggerContextWrapper(ctx)));
  }

  private TriggerResult onTimeEvent(long time,
                                    FlinkWindow<WID> window,
                                    TriggerContext ctx) throws Exception {

    if (time >= window.maxTimestamp()) {
      // fire all windows at the final watermark
      return TriggerResult.FIRE_AND_PURGE;
    }

    // pass onTimeEvent to the original euphoria trigger
    return translateResult(
            euphoriaTrigger.onTimeEvent(
                    time, window.getWindowID(), new TriggerContextWrapper(ctx)));
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
      default:
        throw new IllegalStateException("Unknown result:" + euphoriaResult.name());
    }
  }

  private TriggerResult trackEmissionWatermark(FlinkWindow<WID> window,
                                               TriggerContext ctx,
                                               TriggerResult r)
  {
    if (r.isFire()) {
      window.setEmissionWatermark(ctx.getCurrentWatermark() + 1);
    }
    return r;
  }

  @Override
  public void clear(FlinkWindow<WID> window, TriggerContext ctx) throws Exception {
    euphoriaTrigger.onClear(window.getWindowID(), new TriggerContextWrapper(ctx));

    // ~ our flink-window-ids windows have maxTimestamp == Long.MAX_VALUE; need to
    // clean-up the registered clean-up trigger to avoid mem-leak in long running
    // streams
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }

  @Override
  public boolean canMerge() {
    return true;
  }

  @Override
  public TriggerResult onMerge(FlinkWindow<WID> window, OnMergeContext ctx)
      throws Exception {
    return translateResult(euphoriaTrigger.onMerge(
        window.getWindowID(), new TriggerMergeContextWrapper(ctx)));
  }
}
