package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

import java.util.Objects;


public class FlinkWindowTrigger<GROUP, LABEL, T> extends Trigger<T, FlinkWindow<GROUP, LABEL>> {

  private final Windowing<T, GROUP, LABEL, ?> windowing;
  private final ValueStateDescriptor<WindowContext<GROUP, LABEL>> windowState;

  public FlinkWindowTrigger(Windowing windowing, ExecutionConfig cfg) {
    this.windowing = Objects.requireNonNull(windowing);
    this.windowState = new ValueStateDescriptor<>(
        "window-context", new KryoSerializer(WindowContext.class, cfg), null);
  }

  @Override
  public TriggerResult onElement(
      T element, long timestamp, FlinkWindow<GROUP, LABEL> window, TriggerContext ctx)
      throws Exception {
    return trackEmissionWatermark(
        window, ctx, onElementImpl(element, timestamp, window, ctx));
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        FlinkWindow<GROUP, LABEL> window,
                                        TriggerContext ctx) throws Exception {
    return trackEmissionWatermark(window, ctx, onTimeEvent(time, window, ctx));
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   FlinkWindow<GROUP, LABEL> window,
                                   TriggerContext ctx) throws Exception {

    return trackEmissionWatermark(window, ctx, onTimeEvent(time, window, ctx));
  }

  private TriggerResult onElementImpl(
      T element, long timestamp, FlinkWindow<GROUP, LABEL> window, TriggerContext ctx)
      throws Exception {

    ValueState<WindowContext<GROUP, LABEL>> state = ctx.getPartitionedState(windowState);
    WindowContext<GROUP, LABEL> wContext = state.value();
    if (wContext == null) {
      wContext = windowing.createWindowContext(window.getWindowID());

      List<cz.seznam.euphoria.core.client.triggers.Trigger> triggers;
      triggers = wContext.createTriggers();
      // if all registered triggers are "PASSED", then we have to purge the window
      // if no triggers exist, than we have to CONTINUE
      TriggerResult tr = triggers.isEmpty() ? TriggerResult.CONTINUE : TriggerResult.PURGE;
      for (cz.seznam.euphoria.core.client.triggers.Trigger t : triggers) {
        cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult sched;
        sched = t.schedule(wContext, new TriggerContextWrapper(ctx));
        if (sched != cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult.PASSED) {
          tr = translateResult(sched);
        }
      }
      if (!tr.isPurge()) {
        state.update(wContext);
      }
      return tr;
    } else {
      return TriggerResult.CONTINUE;
    }
  }

  private TriggerResult onTimeEvent(long time,
                                    FlinkWindow<GROUP, LABEL> window,
                                    TriggerContext ctx) throws Exception {

    ValueState<WindowContext<GROUP, LABEL>> state = ctx.getPartitionedState(windowState);
    WindowContext<GROUP, LABEL> wContext = state.value();
    if (wContext == null) {
      wContext = windowing.createWindowContext(window.getWindowID());
    }

    if (time == Long.MAX_VALUE) {
      // fire all windows at the final watermark
      return TriggerResult.FIRE_AND_PURGE;
    }

    @SuppressWarnings("unchecked")
    TriggerContextWrapper triggerContext = new TriggerContextWrapper(ctx);
    TriggerResult r = TriggerResult.CONTINUE;
    for (cz.seznam.euphoria.core.client.triggers.Trigger trigger : wContext.createTriggers()) {
      r = TriggerResult.merge(r, translateResult(trigger.onTimeEvent(time, wContext, triggerContext)));
    }
    if (!r.isPurge()) {
      state.update(wContext);
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

  private TriggerResult trackEmissionWatermark(FlinkWindow<GROUP, LABEL> window,
                                               TriggerContext ctx,
                                               TriggerResult r)
  {
    if (r.isFire()) {
      window.setEmissionWatermark(ctx.getCurrentWatermark() + 1);
    }
    return r;
  }

  @Override
  public void clear(FlinkWindow window, TriggerContext ctx) throws Exception {
    // ~ clear our partitions state - if any
    ctx.getPartitionedState(windowState).clear();
    // ~ our flink-window-ids windows have maxTimestamp == Long.MAX_VALUE; need to
    // clean-up the registered clean-up trigger to avoid mem-leak in long running
    // streams
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }
}
