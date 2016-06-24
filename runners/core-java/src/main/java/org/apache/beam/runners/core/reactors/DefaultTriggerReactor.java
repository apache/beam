/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.core.reactors;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.util.TimeDomain;

/**
 * A trigger that is equivalent to {@code Repeatedly.forever(AfterWatermark.pastEndOfWindow())}. See
 * {@link RepeatedlyReactor#forever} and {@link AfterWatermarkReactor#pastEndOfWindow} for more
 * details.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class DefaultTriggerReactor extends TriggerReactor {

  private DefaultTriggerReactor() {
    super(null);
  }

  /**
   * Returns the default trigger.
   */
  public static DefaultTriggerReactor of() {
    return new DefaultTriggerReactor();
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
  public boolean isCompatible(TriggerReactor other) {
    // Semantically, all default triggers are identical
    return other instanceof DefaultTriggerReactor;
  }

  @Override
  public boolean shouldFire(TriggerReactor.TriggerContext context) throws Exception {
    return endOfWindowReached(context);
  }

  private boolean endOfWindowReached(TriggerReactor.TriggerContext context) {
    return context.currentEventTime() != null
        && context.currentEventTime().isAfter(context.window().maxTimestamp());
  }

  @Override
  public void onFire(TriggerReactor.TriggerContext context) throws Exception { }
}
