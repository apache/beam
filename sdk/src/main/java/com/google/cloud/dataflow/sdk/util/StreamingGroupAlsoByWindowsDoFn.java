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
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

/**
 * DoFn that merges windows and groups elements in those windows.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
@SystemDoFnInternal
public abstract class StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<TimerOrElement<KV<K, InputT>>, KV<K, OutputT>> {

  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W> create(
          final WindowingStrategy<?, W> windowingStrategy,
          final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn,
          final Coder<K> keyCoder) {
    Preconditions.checkNotNull(combineFn);
    return new StreamingGABWViaWindowSetDoFn<>(windowingStrategy,
        SystemReduceFn.<K, InputT, AccumT, OutputT, W>combining(keyCoder, combineFn));
  }

  public static <K, V, W extends BoundedWindow>
  StreamingGroupAlsoByWindowsDoFn<K, V, Iterable<V>, W> createForIterable(
      final WindowingStrategy<?, W> windowingStrategy,
      final Coder<V> inputCoder) {
    return new StreamingGABWViaWindowSetDoFn<>(
        windowingStrategy, SystemReduceFn.<K, V, W>buffering(inputCoder));
  }

  private static class StreamingGABWViaWindowSetDoFn<K, InputT, OutputT, W extends BoundedWindow>
  extends StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {

    private final Aggregator<Long, Long> droppedDueToClosedWindow =
        createAggregator(ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER, new Sum.SumLongFn());
    private final Aggregator<Long, Long> droppedDueToLateness =
        createAggregator(ReduceFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER, new Sum.SumLongFn());

    private final WindowingStrategy<Object, W> windowingStrategy;
    private SystemReduceFn.Factory<K, InputT, OutputT, W> reduceFnFactory;

    private transient ReduceFnRunner<K, InputT, OutputT, W> runner;

    public StreamingGABWViaWindowSetDoFn(WindowingStrategy<?, W> windowingStrategy,
        SystemReduceFn.Factory<K, InputT, OutputT, W> reduceFnFactory) {
      @SuppressWarnings("unchecked")
      WindowingStrategy<Object, W> noWildcard = (WindowingStrategy<Object, W>) windowingStrategy;
      this.windowingStrategy = noWildcard;
      this.reduceFnFactory = reduceFnFactory;
    }

    private void initForKey(ProcessContext c, K key) throws Exception{
      if (runner == null) {
        TimerInternals timerInternals = c.windowingInternals().timerInternals();
        runner = new ReduceFnRunner<>(
            key, windowingStrategy, timerInternals, c.windowingInternals(),
            droppedDueToClosedWindow, droppedDueToLateness, reduceFnFactory.create(key));
      }
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      if (c.element().isTimer()) {
        processTimer(c);
      } else {
        processValue(c);
      }
    }


    private void processTimer(ProcessContext c) throws Exception {
      @SuppressWarnings("unchecked")
      K key = (K) c.element().key();
      initForKey(c, key);
      runner.onTimer(c.element().getTimer());
    }

    private void processValue(ProcessContext c) throws Exception {
      K key = c.element().element().getKey();
      initForKey(c, key);
      InputT value = c.element().element().getValue();
      runner.processElement(
          WindowedValue.of(
              value,
              c.timestamp(),
              c.windowingInternals().windows(),
              c.pane()));
    }

    @Override
    public void finishBundle(Context c) throws Exception {
      if (runner != null) {
        // Merge before finishing the bundle in case it causes triggers to fire.
        runner.merge();
        runner.persist();
      }

      // Prepare this DoFn for reuse.
      runner = null;
    }
  }
}
