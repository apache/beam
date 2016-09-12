package cz.seznam.euphoria.flink.streaming.windowing;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Sets
 * {@link cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow#setEmissionWatermark(long)}
 * when the underlying windows are fired.
 */
public class EmissionWindowTrigger<T, W extends Window>
    extends Trigger<T, EmissionWindow<W>>
{
  private final Trigger<T, W> inner;

  public EmissionWindowTrigger(Trigger<T, W> inner) {
    this.inner = inner;
  }

  @Override
  public TriggerResult onElement(T element, long timestamp,
                                 EmissionWindow<W> window,
                                 TriggerContext ctx)
      throws Exception
  {
    return trackEmissionWatermark(window, ctx,
        inner.onElement(element, timestamp, window.getInner(), ctx));
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        EmissionWindow<W> window,
                                        TriggerContext ctx)
      throws Exception
  {
    return trackEmissionWatermark(window, ctx,
        inner.onProcessingTime(time, window.getInner(), ctx));
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   EmissionWindow<W> window,
                                   TriggerContext ctx)
      throws Exception
  {
    return trackEmissionWatermark(window, ctx,
        inner.onEventTime(time, window.getInner(), ctx));
  }

  @Override
  public boolean canMerge() {
    return inner.canMerge();
  }

  @Override
  public TriggerResult onMerge(EmissionWindow<W> window,
                               OnMergeContext ctx)
      throws Exception
  {
    return trackEmissionWatermark(window, ctx, inner.onMerge(window.getInner(), ctx));
  }

  @Override
  public void clear(EmissionWindow<W> window, TriggerContext ctx)
      throws Exception
  {
    inner.clear(window.getInner(), ctx);
  }

  private TriggerResult trackEmissionWatermark(EmissionWindow<W> window,
                                               TriggerContext ctx,
                                               TriggerResult r)
  {
    if (r.isFire()) {
      window.setEmissionWatermark(ctx.getCurrentWatermark() + 1);
    }
    return r;
  }
}