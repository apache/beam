/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.util.TimeDomain;

import org.joda.time.Instant;

import java.util.List;

/**
 * A trigger that is equivalent to {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}.
 * See {@link Repeatedly#forever} and {@link AfterWatermark#pastEndOfWindow} for more details.
 *
 * @param <W> The type of windows being triggered/encoded.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class DefaultTrigger<W extends BoundedWindow> extends Trigger<W>{

  private static final long serialVersionUID = 0L;

  private DefaultTrigger() {
    super(null);
  }

  /**
   * Returns the default trigger.
   */
  public static <W extends BoundedWindow> DefaultTrigger<W> of() {
    return new DefaultTrigger<W>();
  }

  @Override
  public TriggerResult onElement(OnElementContext c) throws Exception {
    c.timers().setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
    return TriggerResult.CONTINUE;
  }

  @Override
  public MergeResult onMerge(OnMergeContext c) throws Exception {
    c.timers().setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
    return MergeResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(OnTimerContext c) throws Exception {
    return TriggerResult.FIRE;
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.timers().deleteTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return window.maxTimestamp();
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    // Semantically, all default triggers are identical
    return other instanceof DefaultTrigger;
  }

  @Override
  public Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return this;
  }
}
