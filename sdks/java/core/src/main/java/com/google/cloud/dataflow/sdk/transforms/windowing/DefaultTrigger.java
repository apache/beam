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
  public void onElement(OnElementContext c) throws Exception {
    // If the end of the window has already been reached, then we are already ready to fire
    // and do not need to set a wake-up timer.
    if (!endOfWindowReached(c)) {
      c.setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
    }
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    // If the end of the window has already been reached, then we are already ready to fire
    // and do not need to set a wake-up timer.
    if (!endOfWindowReached(c)) {
      c.setTimer(c.window().maxTimestamp(), TimeDomain.EVENT_TIME);
    }
  }

  @Override
  public void clear(TriggerContext c) throws Exception { }

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

  @Override
  public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
    return endOfWindowReached(context);
  }

  private boolean endOfWindowReached(Trigger<W>.TriggerContext context) {
    return context.currentEventTime() != null
        && context.currentEventTime().isAfter(context.window().maxTimestamp());
  }

  @Override
  public void onFire(Trigger<W>.TriggerContext context) throws Exception { }
}
