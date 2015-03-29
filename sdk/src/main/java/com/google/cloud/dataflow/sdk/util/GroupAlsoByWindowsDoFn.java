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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresKeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.TriggerExecutor.TimerManager;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * DoFn that merges windows and groups elements in those windows, optionally
 * combining values.
 *
 * @param <K> key type
 * @param <VI> input value element type
 * @param <VO> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public abstract class GroupAlsoByWindowsDoFn<K, VI, VO, W extends BoundedWindow>
    extends DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>
    implements RequiresKeyedState {

  /**
   * Create a {@link GroupAlsoByWindowsDoFn} without a combine function. Depending on the
   * {@code windowFn} this will either use iterators or window sets to implement the grouping.
   *
   * @param windowFn The window function to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W>
  createForIterable(final WindowFn<?, W> windowFn, final Coder<V> inputCoder) {
    if (windowFn instanceof NonMergingWindowFn) {
      return new GroupAlsoByWindowsViaIteratorsDoFn<K, V, W>();
    } else {
      return new GABWViaWindowSetDoFn<K, V, Iterable<V>, W>(windowFn) {
        @Override
        AbstractWindowSet<K, V, Iterable<V>, W> createWindowSet(K key,
            DoFn<?, KV<K, Iterable<V>>>.ProcessContext context) throws Exception {
          return new BufferingWindowSet<K, V, W>(key, windowFn, inputCoder, context);
        }
      };
    }
  }

  /**
   * Construct a {@link GroupAlsoByWindowsDoFn} using the {@code combineFn} if available.
   */
  public static <K, VI, VO, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, VI, VO, W>
  create(
      final WindowFn<?, W> windowFn,
      final KeyedCombineFn<K, VI, ?, VO> combineFn,
      final Coder<K> keyCoder,
      final Coder<VI> inputCoder) {
    Preconditions.checkNotNull(combineFn);
    return new GABWViaWindowSetDoFn<K, VI, VO, W>(windowFn) {
      @Override
      AbstractWindowSet<K, VI, VO, W> createWindowSet(
          K key,
          DoFn<?, KV<K, VO>>.ProcessContext context) throws Exception {
        return new CombiningWindowSet<>(key, windowFn, combineFn, keyCoder, inputCoder, context);
      }
    };
  }

  private abstract static class GABWViaWindowSetDoFn<K, VI, VO, W extends BoundedWindow>
     extends GroupAlsoByWindowsDoFn<K, VI, VO, W> {

    private WindowFn<Object, W> windowFn;

    public GABWViaWindowSetDoFn(WindowFn<?, W> windowFn) {
      @SuppressWarnings("unchecked")
      WindowFn<Object, W> noWildcard = (WindowFn<Object, W>) windowFn;
      this.windowFn = noWildcard;
    }

    abstract AbstractWindowSet<K, VI, VO, W> createWindowSet(
        K key, DoFn<?, KV<K, VO>>.ProcessContext context)
        throws Exception;

    @Override
    public void processElement(
        DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>.ProcessContext c) throws Exception {
      K key = c.element().getKey();
      AbstractWindowSet<K, VI, VO, W> windowSet = createWindowSet(key, c);

      BatchTimerManager timerManager = new BatchTimerManager();
      TriggerExecutor<K, VI, VO, W> triggerExecutor = new TriggerExecutor<>(
          windowFn, timerManager,
          new DefaultTrigger<W>(),
          c.windowingInternals(), windowSet);

      for (WindowedValue<VI> e : c.element().getValue()) {
        // First, handle anything that needs to happen for this element
        triggerExecutor.onElement(e.getValue(), e.getWindows());

        // Then, since elements are sorted by their timestamp, advance the watermark and fire any
        // timers that need to be fired.
        advanceWatermark(timerManager, triggerExecutor, e.getTimestamp());
      }

      // Finish any pending windows by advance the watermark to infinity.
      advanceWatermark(timerManager, triggerExecutor, new Instant(Long.MAX_VALUE));

      windowSet.persist();
    }

    private void advanceWatermark(BatchTimerManager timerManager,
        TriggerExecutor<?, ?, ?, ?> triggerExecutor, Instant instant) throws Exception {
      while (timerManager.readyToFire(instant)) {
        BatchTimer tagToFire = timerManager.tagToFire();
        triggerExecutor.onTimer(tagToFire.tag);
      }
    }
  }

  private static class BatchTimer implements Comparable<BatchTimer> {

    private final String tag;
    private final Instant time;

    public BatchTimer(String tag, Instant time) {
      this.tag = tag;
      this.time = time;
    }

    @Override
    public String toString() {
      return time + ": " + tag;
    }

    @Override
    public int compareTo(BatchTimer o) {
      return time.compareTo(o.time);
    }

    @Override
    public int hashCode() {
      return Objects.hash(time, tag);
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof BatchTimer) {
        BatchTimer that = (BatchTimer) other;
        return Objects.equals(this.time, that.time)
            && Objects.equals(this.tag, that.tag);
      }
      return false;
    }
  }

  private static class BatchTimerManager implements TimerManager {

    // Sort the windows by their end timestamps so that we can efficiently
    // ask for the next window that will be completed.
    private PriorityQueue<BatchTimer> timers = new PriorityQueue<>(11);
    private Map<String, BatchTimer> tagToTimer = new HashMap<>();

    @Override
    public void setTimer(String tag, Instant timestamp) {
      BatchTimer newTimer = new BatchTimer(tag, timestamp);

      BatchTimer oldTimer = tagToTimer.put(tag, newTimer);
      if (oldTimer != null) {
        timers.remove(oldTimer);
      }
      timers.add(newTimer);
    }

    @Override
    public void deleteTimer(String tag) {
      timers.remove(tagToTimer.get(tag));
      tagToTimer.remove(tag);
    }

    /**
     * Determine if there if the next timer is ready to fire at the given timestamp.
     */
    public boolean readyToFire(Instant timestamp) {
      BatchTimer firstTimer = timers.peek();
      return firstTimer != null && timestamp.isAfter(firstTimer.time);
    }

    /**
     * Get the tag to fire.
     */
    public BatchTimer tagToFire() {
      BatchTimer timer = timers.remove();
      tagToTimer.remove(timer.tag);
      return timer;
    }

    @Override
    public String toString() {
      return timers.toString();
    }
  }
}
