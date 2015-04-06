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
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.PartitioningWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.AbstractWindowSet.Factory;
import com.google.cloud.dataflow.sdk.util.TriggerExecutor.TimerManager;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

/**
 * DoFn that merges windows and groups elements in those windows.
 *
 * @param <K> key type
 * @param <VI> input value element type
 * @param <VO> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public abstract class StreamingGroupAlsoByWindowsDoFn<K, VI, VO, W extends BoundedWindow>
    extends DoFn<TimerOrElement<KV<K, VI>>, KV<K, VO>> implements DoFn.RequiresKeyedState {

  public static <K, VI, VA, VO, W extends BoundedWindow>
      StreamingGroupAlsoByWindowsDoFn<K, VI, VO, W> create(
          final WindowingStrategy<?, W> windowingStrategy,
          final KeyedCombineFn<K, VI, VA, VO> combineFn,
          final Coder<K> keyCoder,
          final Coder<VI> inputValueCoder) {
    Preconditions.checkNotNull(combineFn);
    return new StreamingGABWViaWindowSetDoFn<>(windowingStrategy,
        CombiningWindowSet.<K, VI, VA, VO, W>factory(combineFn, keyCoder, inputValueCoder));
  }

  public static <K, VI, W extends BoundedWindow>
  StreamingGroupAlsoByWindowsDoFn<K, VI, Iterable<VI>, W> createForIterable(
      final WindowingStrategy<?, W> windowingStrategy,
      final Coder<VI> inputValueCoder) {
    if (windowingStrategy.getWindowFn() instanceof PartitioningWindowFn
        // TODO: Characterize the other kinds of triggers that work with the
        // PartitioningBufferingWindowSet
        && windowingStrategy.getTrigger() instanceof DefaultTrigger) {
      return new StreamingGABWViaWindowSetDoFn<>(windowingStrategy,
          PartitionBufferingWindowSet.<K, VI, W>factory(inputValueCoder));
    } else {
      return new StreamingGABWViaWindowSetDoFn<>(windowingStrategy,
          BufferingWindowSet.<K, VI, W>factory(inputValueCoder));
    }
  }

  private static class StreamingGABWViaWindowSetDoFn<K, VI, VO, W extends BoundedWindow>
  extends StreamingGroupAlsoByWindowsDoFn<K, VI, VO, W> {
    private final WindowFn<Object, W> windowFn;
    private Factory<K, VI, VO, W> windowSetFactory;
    private Trigger<W> trigger;

    public StreamingGABWViaWindowSetDoFn(WindowingStrategy<?, W> windowingStrategy,
        AbstractWindowSet.Factory<K, VI, VO, W> windowSetFactory) {
      this.windowSetFactory = windowSetFactory;
      @SuppressWarnings("unchecked")
      WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
      this.windowFn = noWildcard.getWindowFn();
      this.trigger = noWildcard.getTrigger();
    }

    @Override
    public void processElement(ProcessContext context) throws Exception {
      if (!context.element().isTimer()) {
        KV<K, VI> element = context.element().element();
        K key = element.getKey();
        VI value = element.getValue();
        AbstractWindowSet<K, VI, VO, W> windowSet = windowSetFactory.create(
                key, windowFn.windowCoder(), context.keyedState(), context.windowingInternals());
        TriggerExecutor<K, VI, VO, W> executor = new TriggerExecutor<>(
            windowFn,
            new StreamingTimerManager(context),
            trigger,
            context.keyedState(),
            context.windowingInternals(),
            windowSet);

        executor.onElement(WindowedValue.of(
            value, context.timestamp(), context.windowingInternals().windows()));
        windowSet.persist();
      } else {
        TimerOrElement<KV<K, VI>> timer = context.element();
        @SuppressWarnings("unchecked")
        K key = (K) timer.key();
        AbstractWindowSet<K, VI, VO, W> windowSet = windowSetFactory.create(
            key, windowFn.windowCoder(), context.keyedState(), context.windowingInternals());
        TriggerExecutor<K, VI, VO, W> executor = new TriggerExecutor<>(
            windowFn,
            new StreamingTimerManager(context),
            trigger,
            context.keyedState(),
            context.windowingInternals(),
            windowSet);

        executor.onTimer(timer.tag());
        windowSet.persist();
      }
    }
  }

  private static class StreamingTimerManager implements TimerManager {

    private DoFn<?, ?>.ProcessContext context;

    public StreamingTimerManager(DoFn<?, ?>.ProcessContext context) {
      this.context = context;
    }

    @Override
    public void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain) {
      // TODO: Hook up support for processing-time timers.
      Preconditions.checkArgument(Trigger.TimeDomain.EVENT_TIME.equals(domain),
          "Streaming currently only supports Watermark based timers.");
      context.windowingInternals().setTimer(timer, timestamp);
    }

    @Override
    public void deleteTimer(String timer, Trigger.TimeDomain domain) {
      // TODO: Hook up support for processing-time timers.
      Preconditions.checkArgument(Trigger.TimeDomain.EVENT_TIME.equals(domain),
          "Streaming currently only supports Watermark based timers.");
      context.windowingInternals().deleteTimer(timer);
    }

    @Override
    public Instant currentProcessingTime() {
      throw new UnsupportedOperationException("Streaming doesn't yet support processing time.");
    }
  }
}
