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
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

/**
 * DoFn that merges windows and groups elements in those windows, optionally
 * combining values.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public abstract class GroupAlsoByWindowsDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>>
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
    if (windowingStrategy.getWindowFn().isNonMerging()
        && windowingStrategy.getTrigger().getSpec() instanceof DefaultTrigger
        && windowingStrategy.getMode() == AccumulationMode.DISCARDING_FIRED_PANES) {
      return new GroupAlsoByWindowsViaIteratorsDoFn<K, V, W>();
    }

    return new GABWViaWindowSetDoFn<>(
        windowingStrategy, AbstractWindowSet.<K, V, W>factoryFor(windowingStrategy, inputCoder));
  }

  /**
   * Construct a {@link GroupAlsoByWindowsDoFn} using the {@code combineFn} if available.
   */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      GroupAlsoByWindowsDoFn<K, InputT, OutputT, W>
  create(
      final WindowingStrategy<?, W> windowingStrategy,
      final KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn,
      final Coder<K> keyCoder,
      final Coder<InputT> inputCoder) {
    Preconditions.checkNotNull(combineFn);
    return new GABWViaWindowSetDoFn<>(
        windowingStrategy, CombiningWindowSet.<K, InputT, AccumT, OutputT, W>factory(
            combineFn, keyCoder, inputCoder));
  }

  private static class GABWViaWindowSetDoFn<K, InputT, OutputT, W extends BoundedWindow>
     extends GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {

    private AbstractWindowSet.Factory<K, InputT, OutputT, W> windowSetFactory;
    private WindowingStrategy<Object, W> strategy;

    public GABWViaWindowSetDoFn(WindowingStrategy<?, W> windowingStrategy,
        AbstractWindowSet.Factory<K, InputT, OutputT, W> factory) {
      @SuppressWarnings("unchecked")
      WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
      this.strategy = noWildcard;
      this.windowSetFactory = factory;
    }

    @Override
    public void processElement(
        DoFn<KV<K, Iterable<WindowedValue<InputT>>>,
        KV<K, OutputT>>.ProcessContext c)
        throws Exception {
      K key = c.element().getKey();
      BatchTimerManager timerManager = new BatchTimerManager(Instant.now());
      TriggerExecutor<K, InputT, OutputT, W> triggerExecutor = TriggerExecutor.create(
          key, strategy, timerManager, windowSetFactory, c.keyedState(), c.windowingInternals());

      for (WindowedValue<InputT> e : c.element().getValue()) {
        // First, handle anything that needs to happen for this element
        triggerExecutor.onElement(e);

        // Then, since elements are sorted by their timestamp, advance the watermark and fire any
        // timers that need to be fired.
        timerManager.advanceWatermark(triggerExecutor, e.getTimestamp());

        // Also, fire any processing timers that need to fire
        timerManager.advanceProcessingTime(triggerExecutor, Instant.now());
      }

      // Merge the active windows for the current key, to fire any data-based triggers.
      triggerExecutor.merge();

      // Finish any pending windows by advancing the watermark to infinity.
      timerManager.advanceWatermark(triggerExecutor, new Instant(Long.MAX_VALUE));

      // Finally, advance the processing time to infinity to fire any timers.
      timerManager.advanceProcessingTime(triggerExecutor, new Instant(Long.MAX_VALUE));

      triggerExecutor.persistWindowSet();
    }
  }
}
