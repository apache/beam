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

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Iterables;

import org.joda.time.Instant;

import java.util.List;

/**
 * The default batch {@link GroupAlsoByWindowsDoFn} implementation, if no specialized "fast path"
 * implementation is applicable.
 */
@SystemDoFnInternal
class GroupAlsoByWindowsViaOutputBufferDoFn<K, InputT, OutputT, W extends BoundedWindow>
   extends GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {

  private final Aggregator<Long, Long> droppedDueToClosedWindow =
      createAggregator(ReduceFnRunner.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER, new Sum.SumLongFn());
  private final Aggregator<Long, Long> droppedDueToLateness =
      createAggregator(ReduceFnRunner.DROPPED_DUE_TO_LATENESS_COUNTER, new Sum.SumLongFn());

  private final WindowingStrategy<?, W> strategy;
  private SystemReduceFn.Factory<K, InputT, OutputT, W> reduceFnFactory;

  public GroupAlsoByWindowsViaOutputBufferDoFn(
      WindowingStrategy<?, W> windowingStrategy,
      SystemReduceFn.Factory<K, InputT, OutputT, W> reduceFnFactory) {
    this.strategy = windowingStrategy;
    this.reduceFnFactory = reduceFnFactory;
  }

  @Override
  public void processElement(
      DoFn<KV<K, Iterable<WindowedValue<InputT>>>,
      KV<K, OutputT>>.ProcessContext c)
      throws Exception {
    K key = c.element().getKey();
    // Used with Batch, we know that all the data is available for this key. We can't use the
    // timer manager from the context because it doesn't exist. So we create one and emulate the
    // watermark, knowing that we have all data and it is in timestamp order.
    BatchTimerInternals timerInternals = new BatchTimerInternals(Instant.now());

    ReduceFnRunner<K, InputT, OutputT, W> runner = new ReduceFnRunner<>(
        key, strategy, timerInternals, c.windowingInternals(),
        droppedDueToClosedWindow, droppedDueToLateness, reduceFnFactory.create(key));

    Iterable<List<WindowedValue<InputT>>> chunks =
        Iterables.partition(c.element().getValue(), 1000);
    for (Iterable<WindowedValue<InputT>> chunk : chunks) {
      // Process the chunk of elements.
      runner.processElements(chunk);

      // Then, since elements are sorted by their timestamp, advance the input watermark
      // to the first element, and fire any timers that may have been scheduled.
      timerInternals.advanceInputWatermark(runner, chunk.iterator().next().getTimestamp());

      // Fire any processing timers that need to fire
      timerInternals.advanceProcessingTime(runner, Instant.now());

      // Leave the output watermark undefined. Since there's no late data in batch mode
      // there's really no need to track it as we do for streaming.
    }

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(runner, BoundedWindow.TIMESTAMP_MAX_VALUE);

    // Finally, advance the processing time to infinity to fire any timers.
    timerInternals.advanceProcessingTime(runner, BoundedWindow.TIMESTAMP_MAX_VALUE);

    runner.persist();
  }
}
