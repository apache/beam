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
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

import java.util.Map.Entry;

/**
 * {@link Trigger}s that fire based on properties of the elements in the current pane.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@link Trigger}
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterPane<W extends BoundedWindow> extends OnceTrigger<W>{

  private static final long serialVersionUID = 0L;

  private static final CodedTupleTag<Integer> ELEMENTS_IN_PANE_TAG =
      CodedTupleTag.of("elements-in-pane", VarIntCoder.of());

  private final int countElems;

  private AfterPane(int countElems) {
    super(null);
    this.countElems = countElems;
  }

  /**
   * Creates a trigger that fires when the pane contains at least {@code countElems} elements.
   */
  public static <W extends BoundedWindow> AfterPane<W> elementCountAtLeast(int countElems) {
    return new AfterPane<>(countElems);
  }

  @Override
  public TriggerResult onElement(TriggerContext<W> c, OnElementEvent<W> e) throws Exception {
    Integer count = c.lookup(ELEMENTS_IN_PANE_TAG, e.window());
    if (count == null) {
      count = 0;
    }
    count++;

    c.store(ELEMENTS_IN_PANE_TAG, e.window(), count);
    return count >= countElems ? TriggerResult.FIRE_AND_FINISH : TriggerResult.CONTINUE;
  }

  @Override
  public MergeResult onMerge(TriggerContext<W> c, OnMergeEvent<W> e) throws Exception {
    // If we've already received enough elements and finished in some window, then this trigger
    // is just finished.
    if (e.finishedInAnyMergingWindow(c.current())) {
      return MergeResult.ALREADY_FINISHED;
    }

    // Otherwise, compute the sum of elements in all the active panes
    int count = 0;
    for (Entry<W, Integer> old : c.lookup(ELEMENTS_IN_PANE_TAG, e.oldWindows()).entrySet()) {
      if (old.getValue() != null) {
        count += old.getValue();
      }
    }

    // And determine the final status from that.
    if (count >= countElems) {
      return MergeResult.FIRE_AND_FINISH;
    } else {
      c.store(ELEMENTS_IN_PANE_TAG, e.newWindow(), count);
      return MergeResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    c.remove(ELEMENTS_IN_PANE_TAG, window);
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    return (other instanceof AfterPane)
        && countElems == ((AfterPane<?>) other).countElems;
  }

  @Override
  public Instant getWatermarkCutoff(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }
}
