/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;


public class FlinkWindowTrigger<WID extends Window, T> extends Trigger<T, FlinkWindow<WID>> {

  private final cz.seznam.euphoria.core.client.triggers.Trigger<WID> euphoriaTrigger;

  public FlinkWindowTrigger(
          cz.seznam.euphoria.core.client.triggers.Trigger<WID> trigger) {
    this.euphoriaTrigger = trigger;
  }

  @Override
  public TriggerResult onElement(
      T element, long timestamp, FlinkWindow<WID> window, TriggerContext ctx)
      throws Exception {
    return trackEmissionWatermark(
        window, onElementImpl(element, timestamp, window, ctx),
        ctx.getCurrentWatermark());
  }

  @Override
  public TriggerResult onProcessingTime(long time,
                                        FlinkWindow<WID> window,
                                        TriggerContext ctx) throws Exception {
    return trackEmissionWatermark(window, onTimeEvent(time, window, ctx), time);
  }

  @Override
  public TriggerResult onEventTime(long time,
                                   FlinkWindow<WID> window,
                                   TriggerContext ctx) throws Exception {

    return trackEmissionWatermark(window, onTimeEvent(time, window, ctx), time);
  }

  private TriggerResult onElementImpl(
      T element, long timestamp, FlinkWindow<WID> window, TriggerContext ctx)
      throws Exception {

    // pass onElement event to the original euphoria trigger
    return translateResult(
            euphoriaTrigger.onElement(
                    timestamp, window.getWindowID(), new TriggerContextWrapper(ctx)));
  }

  private TriggerResult onTimeEvent(long time,
                                    FlinkWindow<WID> window,
                                    TriggerContext ctx) throws Exception {

    if (time >= window.maxTimestamp()) {
      // fire all windows at the final watermark
      return TriggerResult.FIRE_AND_PURGE;
    }

    // pass onTimer to the original euphoria trigger
    return translateResult(
            euphoriaTrigger.onTimer(
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

  private TriggerResult trackEmissionWatermark(
      FlinkWindow<WID> window, TriggerResult r, long watermark)
  {
    if (r.isFire()) {
      window.setEmissionWatermark(watermark);
      window.overrideMaxTimestamp(watermark);
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
