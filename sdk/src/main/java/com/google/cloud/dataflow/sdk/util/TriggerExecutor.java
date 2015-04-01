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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PartitioningWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.util.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.util.Trigger.WindowStatus;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Manages the execution of a trigger.
 *
 * @param <K>
 * @param <VI>
 * @param <VO>
 * @param <W> The type of windows this operates on.
 */
public class TriggerExecutor<K, VI, VO, W extends BoundedWindow> implements TriggerContext<W> {

  private final WindowFn<Object, W> windowFn;
  private final Trigger<Object, W> trigger;
  private final WindowingInternals<?, KV<K, VO>> windowingInternals;
  private final AbstractWindowSet<K, VI, VO, W> windowSet;
  private final TimerManager timerManager;
  private final MergeContext mergeContext;
  private KeyedState keyedState;

  /**
   * Methods that the system must provide in order for us to implement triggers.
   */
  public interface TimerManager {

    /**
     * Writes out a timer to be fired when the watermark reaches the given
     * timestamp.  Timers are identified by their name, and can be moved
     * by calling {@code setTimer} again, or deleted with {@link #deleteTimer}.
     */
    void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain);

    /**
     * Deletes the given timer.
     */
    void deleteTimer(String timer, Trigger.TimeDomain domain);

    /**
     * @return the current timestamp in the {@link Trigger.TimeDomain#PROCESSING_TIME}.
     */
    Instant currentProcessingTime();
  }

  public TriggerExecutor(
      WindowFn<Object, W> windowFn,
      TimerManager timerManager,
      Trigger<Object, W> trigger,
      KeyedState keyedState,
      WindowingInternals<?, KV<K, VO>> windowingInternals,
      AbstractWindowSet<K, VI, VO, W> windowSet) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.keyedState = keyedState;
    this.windowingInternals = windowingInternals;
    this.windowSet = windowSet;
    this.timerManager = timerManager;
    this.mergeContext = new MergeContext();
  }

  public void onElement(WindowedValue<VI> value) throws Exception {
    for (BoundedWindow window : value.getWindows()) {
      @SuppressWarnings("unchecked")
      W w = (W) window;

      WindowStatus status = windowSet.put(w, value.getValue(), value.getTimestamp());

      trigger.onElement(this, value.getValue(), w, status);
    }
  }

  public void onTimer(String timerTag) throws Exception {
    // Attempt to merge windows before continuing; that may remove the current window from
    // consideration.
    windowFn.mergeWindows(mergeContext);

    W window = WindowUtils.windowFromString(timerTag, windowFn.windowCoder());

    // Make sure the window still exists before passing the timer to the trigger.

    // The WindowSet used with PartitioningWindowFn doesn't support contains, but it will never
    // merge windows in a way that causes the timer to no longer be applicable.
    if ((windowFn instanceof PartitioningWindowFn) || windowSet.contains(window)) {
      trigger.onTimer(this, window);
    }
  }

  @Override
  public void emitWindow(W window) throws Exception {
    // Emit the (current) final values for the window
    KV<K, VO> value = KV.of(windowSet.getKey(), windowSet.finalValue(window));

    // Remove the window from management (assume it is "done")
    windowSet.remove(window);

    // Output the windowed value.
    windowingInternals.outputWindowedValue(value, window.maxTimestamp(), Arrays.asList(window));
  }

  private class MergeContext extends WindowFn<Object, W>.MergeContext {

    @SuppressWarnings("cast")
    public MergeContext() {
      ((WindowFn<Object, W>) windowFn).super();
    }

    @Override
    public Collection<W> windows() {
      return windowSet.windows();
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      windowSet.merge(toBeMerged, mergeResult);
      trigger.onMerge(TriggerExecutor.this, toBeMerged, mergeResult);
    }
  }

  @Override
  public void setTimer(W window, Instant timestamp, TimeDomain domain) throws IOException {
    timerManager.setTimer(
        WindowUtils.windowToString(window, windowFn.windowCoder()),
        timestamp,
        domain);
  }

  @Override
  public void deleteTimer(W window, TimeDomain domain) throws IOException {
    timerManager.deleteTimer(
        WindowUtils.windowToString(window, windowFn.windowCoder()),
        domain);
  }

  private <T> CodedTupleTag<T> perWindowTag(CodedTupleTag<T> tag, W window) throws IOException {
    return CodedTupleTag.of(
        tag.getId() + WindowUtils.windowToString(window, windowFn.windowCoder()),
        tag.getCoder());
  }

  @Override
  public <T> void store(CodedTupleTag<T> tag, W window, T value) throws IOException {
    keyedState.store(perWindowTag(tag, window), value);
  }

  @Override
  public <T> void remove(CodedTupleTag<T> tag, W window) throws IOException {
    keyedState.remove(perWindowTag(tag, window));
  }

  @Override
  public <T> T lookup(CodedTupleTag<T> tag, W window) throws IOException {
    return keyedState.lookup(perWindowTag(tag, window));
  }


  @Override
  public <T> Iterable<T> lookup(CodedTupleTag<T> tag, Iterable<W> windows) throws IOException {
    List<CodedTupleTag<T>> tags = new ArrayList<>();
    for (W window : windows) {
      tags.add(perWindowTag(tag, window));
    }
    CodedTupleTagMap tagMap = keyedState.lookup(tags);

    List<T> result = new ArrayList<>();
    for (CodedTupleTag<T> windowTag : tags) {
      result.add(tagMap.get(windowTag));
    }
    return result;
  }

  @Override
  public Instant currentProcessingTime() {
    return timerManager.currentProcessingTime();
  }
}
