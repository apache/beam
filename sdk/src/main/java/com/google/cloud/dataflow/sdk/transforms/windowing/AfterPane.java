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

import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.AtMostOnceTrigger;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;

import org.joda.time.Instant;

import java.util.Map.Entry;

/**
 * Triggers that fire based on properties of the elements in the current pane.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code Trigger}
 */
public class AfterPane<W extends BoundedWindow> implements AtMostOnceTrigger<W>{

  private static final long serialVersionUID = 0L;

  private static final CodedTupleTag<Integer> ELEMENTS_IN_PANE_TAG =
      CodedTupleTag.of("elements-in-pane", VarIntCoder.of());

  private final int countElems;

  private AfterPane(int countElems) {
    this.countElems = countElems;
  }

  /**
   * Creates a trigger that fires when the pane contains at least {@code countElems} elements.
   */
  public static <W extends BoundedWindow> AfterPane<W> elementCountAtLeast(int countElems) {
    return new AfterPane<>(countElems);
  }

  @Override
  public TriggerResult onElement(
      TriggerContext<W> c, Object value, Instant timestamp, W window, WindowStatus status)
      throws Exception {
    Integer count = c.lookup(ELEMENTS_IN_PANE_TAG, window);
    if (count == null) {
      count = 0;
    }
    count++;

    if (count >= countElems) {
      return TriggerResult.FIRE_AND_FINISH;
    }

    c.store(ELEMENTS_IN_PANE_TAG, window, count);
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onMerge(
      TriggerContext<W> c, Iterable<W> oldWindows, W newWindow) throws Exception {
    int count = 0;
    for (Entry<W, Integer> old : c.lookup(ELEMENTS_IN_PANE_TAG, oldWindows).entrySet()) {
      if (old.getValue() != null) {
        count += old.getValue();
        c.remove(ELEMENTS_IN_PANE_TAG, old.getKey());
      }
    }

    // Don't break early because we want to clean up the old keyed state.
    if (count >= countElems) {
      return TriggerResult.FIRE_AND_FINISH;
    }
    c.store(ELEMENTS_IN_PANE_TAG, newWindow, count);
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onTimer(TriggerContext<W> c, TriggerId<W> triggerId) {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    c.remove(ELEMENTS_IN_PANE_TAG, window);
  }

  @Override
  public boolean willNeverFinish() {
    return false;
  }
}
