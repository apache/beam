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
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

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
   * @param windowingStrategy The window function and trigger to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W>
  createForIterable(WindowingStrategy<?, W> windowingStrategy, Coder<V> inputCoder) {
    if (windowingStrategy.getWindowFn() instanceof NonMergingWindowFn
        && windowingStrategy.getTrigger() instanceof DefaultTrigger) {
      return new GroupAlsoByWindowsViaIteratorsDoFn<K, V, W>();
    } else {
      return new GABWViaWindowSetDoFn<>(
          windowingStrategy, BufferingWindowSet.<K, V, W>factory(inputCoder));
    }
  }

  /**
   * Construct a {@link GroupAlsoByWindowsDoFn} using the {@code combineFn} if available.
   */
  public static <K, VI, VA, VO, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, VI, VO, W>
  create(
      final WindowingStrategy<?, W> windowingStrategy,
      final KeyedCombineFn<K, VI, VA, VO> combineFn,
      final Coder<K> keyCoder,
      final Coder<VI> inputCoder) {
    Preconditions.checkNotNull(combineFn);
    return new GABWViaWindowSetDoFn<>(
        windowingStrategy, CombiningWindowSet.<K, VI, VA, VO, W>factory(
            combineFn, keyCoder, inputCoder));
  }

  private static class GABWViaWindowSetDoFn<K, VI, VO, W extends BoundedWindow>
     extends GroupAlsoByWindowsDoFn<K, VI, VO, W> {

    private WindowFn<Object, W> windowFn;
    private AbstractWindowSet.Factory<K, VI, VO, W> windowSetFactory;
    private Trigger<W> trigger;

    public GABWViaWindowSetDoFn(WindowingStrategy<?, W> windowingStrategy,
        AbstractWindowSet.Factory<K, VI, VO, W> factory) {
      @SuppressWarnings("unchecked")
      WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
      this.windowFn = noWildcard.getWindowFn();
      this.trigger = noWildcard.getTrigger();
      this.windowSetFactory = factory;
    }

    @Override
    public void processElement(
        DoFn<KV<K, Iterable<WindowedValue<VI>>>, KV<K, VO>>.ProcessContext c) throws Exception {
      K key = c.element().getKey();
      AbstractWindowSet<K, VI, VO, W> windowSet = windowSetFactory.create(
          key, windowFn.windowCoder(), c.keyedState(), c.windowingInternals());

      BatchTimerManager timerManager = new BatchTimerManager(Instant.now());
      TriggerExecutor<K, VI, VO, W> triggerExecutor = new TriggerExecutor<>(
          windowFn, timerManager, trigger,
          c.keyedState(), c.windowingInternals(), windowSet);

      for (WindowedValue<VI> e : c.element().getValue()) {
        // First, handle anything that needs to happen for this element
        triggerExecutor.onElement(e);

        // Then, since elements are sorted by their timestamp, advance the watermark and fire any
        // timers that need to be fired.
        timerManager.advanceWatermark(triggerExecutor, e.getTimestamp());

        // Also, fire any processing timers that need to fire
        timerManager.advanceProcessingTime(triggerExecutor, Instant.now());
      }

      // Finish any pending windows by advancing the watermark to infinity.
      timerManager.advanceWatermark(triggerExecutor, new Instant(Long.MAX_VALUE));

      // Finally, advance the processing time
      timerManager.advanceProcessingTime(triggerExecutor, Instant.now());

      windowSet.persist();
    }
  }
}
