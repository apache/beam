
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;

import java.util.List;
import java.util.Objects;


public class FlinkTrigger<T> extends Trigger<T, FlinkWindowID> {

  private final Windowing windowing;
  private final ValueStateDescriptor<WindowContext> windowState;

  public FlinkTrigger(Windowing windowing, ExecutionConfig cfg) {
    this.windowing = Objects.requireNonNull(windowing);
    this.windowState = new ValueStateDescriptor<>(
        "window-context", new KryoSerializer(WindowContext.class, cfg), null);
  }

  @Override
  public TriggerResult onElement(T element,
                                 long timestamp,
                                 FlinkWindowID window,
                                 TriggerContext ctx) throws Exception {

    ValueState<WindowContext> state = ctx.getPartitionedState(windowState);
    WindowContext wContext = state.value();
    if (wContext == null) {
      wContext = windowing.createWindowContext(window.getWindowID());

      @SuppressWarnings("unchecked")
      List<cz.seznam.euphoria.core.client.triggers.Trigger> eTriggers =
          wContext.createTriggers();

      TriggerResult tr = TriggerResult.PURGE;
      for (cz.seznam.euphoria.core.client.triggers.Trigger t : eTriggers) {
        cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult sched =
            t.schedule(wContext, new TriggerContextWrapper(ctx));
        if (sched != cz.seznam.euphoria.core.client.triggers.Trigger.TriggerResult.PASSED) {
          tr = translateResult(sched);
        }
      }
      if (!tr.isPurge()) {
        state.update(wContext);
      }
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        FlinkWindowID window,
                                        TriggerContext ctx) throws Exception {
    return onTimeEvent(time, window, ctx);
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   FlinkWindowID window,
                                   TriggerContext ctx) throws Exception {

    return onTimeEvent(time, window, ctx);
  }

  private TriggerResult onTimeEvent(long time,
                                    FlinkWindowID window,
                                    TriggerContext ctx) throws Exception {

    ValueState<WindowContext> state = ctx.getPartitionedState(windowState);
    WindowContext wContext = state.value();
    if (wContext == null) {
      wContext = windowing.createWindowContext(window.getWindowID());
    }

    @SuppressWarnings("unchecked")
    TriggerContextWrapper triggerContext = new TriggerContextWrapper(ctx);
    TriggerResult r = TriggerResult.CONTINUE;
    for (cz.seznam.euphoria.core.client.triggers.Trigger trigger :
        (List< cz.seznam.euphoria.core.client.triggers.Trigger>) wContext.createTriggers()) {
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

  @Override
  public void clear(FlinkWindowID window, TriggerContext ctx) throws Exception {
    // ~ clear our partitions state - if any
    ctx.getPartitionedState(windowState).clear();
    // ~ our flink-window-ids windows have maxTimestamp == Long.MAX_VALUE; need to
    // clean-up the registered clean-up trigger to avoid mem-leak in long running
    // streams
    ctx.deleteEventTimeTimer(window.maxTimestamp());
  }
}
