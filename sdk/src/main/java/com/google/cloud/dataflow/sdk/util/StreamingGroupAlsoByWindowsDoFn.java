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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.util.AbstractWindowSet.Factory;
import com.google.cloud.dataflow.sdk.util.TriggerExecutor.TimerManager;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

/**
 * DoFn that merges windows and groups elements in those windows.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public abstract class StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<TimerOrElement<KV<K, InputT>>, KV<K, OutputT>> implements DoFn.RequiresKeyedState {

  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W> create(
          final WindowingStrategy<?, W> windowingStrategy,
          final KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn,
          final Coder<K> keyCoder,
          final Coder<InputT> inputValueCoder) {
    Preconditions.checkNotNull(combineFn);
    return new StreamingGABWViaWindowSetDoFn<>(windowingStrategy,
        CombiningWindowSet.<K, InputT, AccumT, OutputT, W>factory(
            combineFn, keyCoder, inputValueCoder));
  }

  public static <K, V, W extends BoundedWindow>
  StreamingGroupAlsoByWindowsDoFn<K, V, Iterable<V>, W> createForIterable(
      final WindowingStrategy<?, W> windowingStrategy,
      final Coder<V> inputCoder) {
    return new StreamingGABWViaWindowSetDoFn<>(
        windowingStrategy, AbstractWindowSet.<K, V, W>factoryFor(windowingStrategy, inputCoder));
  }

  private static class StreamingGABWViaWindowSetDoFn<K, InputT, OutputT, W extends BoundedWindow>
  extends StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {
    private final Factory<K, InputT, OutputT, W> windowSetFactory;
    private final WindowingStrategy<Object, W> windowingStrategy;

    private TriggerExecutor<K, InputT, OutputT, W> executor;

    public StreamingGABWViaWindowSetDoFn(WindowingStrategy<?, W> windowingStrategy,
        AbstractWindowSet.Factory<K, InputT, OutputT, W> windowSetFactory) {
      this.windowSetFactory = windowSetFactory;
      @SuppressWarnings("unchecked")
      WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
      this.windowingStrategy = noWildcard;
    }

    private void initForKey(ProcessContext c, K key) throws Exception{
      if (executor == null) {
        TimerManager timerManager = new StreamingTimerManager(c);
        executor = TriggerExecutor.create(
          key, windowingStrategy, timerManager, windowSetFactory,
          c.keyedState(), c.windowingInternals());
      }
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      @SuppressWarnings("unchecked")
      K key = c.element().isTimer() ? (K) c.element().key() : c.element().element().getKey();
      initForKey(c, key);

      if (c.element().isTimer()) {
        executor.onTimer(c.element().tag());
      } else {
        InputT value = c.element().element().getValue();
        executor.onElement(
            WindowedValue.of(value, c.timestamp(), c.windowingInternals().windows()));
      }
    }

    @Override
    public void finishBundle(Context c) throws Exception {
      if (executor != null) {
        // Merge before finishing the bundle in case it causes triggers to fire.
        executor.merge();
        executor.persistWindowSet();
      }

      // Prepare this DoFn for reuse.
      executor = null;
    }
  }

  private static class StreamingTimerManager implements TimerManager {

    private DoFn<?, ?>.ProcessContext context;

    public StreamingTimerManager(DoFn<?, ?>.ProcessContext context) {
      this.context = context;
    }

    @Override
    public void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain) {
      context.windowingInternals().setTimer(timer, timestamp, domain);
    }

    @Override
    public void deleteTimer(String timer, Trigger.TimeDomain domain) {
      context.windowingInternals().deleteTimer(timer, domain);
    }

    @Override
    public Instant currentProcessingTime() {
      return Instant.now();
    }
  }
}
