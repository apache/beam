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

import com.google.cloud.dataflow.sdk.util.ExecutableTrigger;

import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

/**
 * Repeat a trigger, either until some condition is met or forever.
 *
 * <p>For example, to fire after the end of the window, and every time late data arrives:
 * <pre> {@code
 *     Repeatedly.forever(AfterWatermark.isPastEndOfWindow());
 * } </pre>
 *
 * <p>{@code Repeatedly.forever(someTrigger)} behaves like an infinite
 * {@code AfterEach.inOrder(someTrigger, someTrigger, someTrigger, ...)}.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
public class Repeatedly<W extends BoundedWindow> extends Trigger<W> {

  private static final int REPEATED = 0;

  /**
   * Create a composite trigger that repeatedly executes the trigger {@code toRepeat}, firing each
   * time it fires and ignoring any indications to finish.
   *
   * <p>Unless used with {@link Trigger#orFinally} the composite trigger will never finish.
   *
   * @param repeated the trigger to execute repeatedly.
   */
  public static <W extends BoundedWindow> Repeatedly<W> forever(Trigger<W> repeated) {
    return new Repeatedly<W>(repeated);
  }

  private Repeatedly(Trigger<W> repeated) {
    super(Arrays.asList(repeated));
  }


  @Override
  public void onElement(OnElementContext c) throws Exception {
    getRepeated(c).invokeOnElement(c);
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    getRepeated(c).invokeOnMerge(c);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    // This trigger fires once the repeated trigger fires.
    return subTriggers.get(REPEATED).getWatermarkThatGuaranteesFiring(window);
  }

  @Override
  public Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return new Repeatedly<W>(continuationTriggers.get(REPEATED));
  }

  @Override
  public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
    return getRepeated(context).invokeShouldFire(context);
  }

  @Override
  public void onFire(TriggerContext context) throws Exception {
    getRepeated(context).invokeOnFire(context);

    if (context.trigger().isFinished(REPEATED)) {
      context.trigger().setFinished(false, REPEATED);
      getRepeated(context).invokeClear(context);
    }
  }

  private ExecutableTrigger<W> getRepeated(TriggerContext context) {
    return context.trigger().subTrigger(REPEATED);
  }
}
