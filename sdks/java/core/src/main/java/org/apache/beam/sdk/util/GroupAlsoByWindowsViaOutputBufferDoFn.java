/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.KV;

import com.google.common.collect.Iterables;

import org.joda.time.Instant;

import java.util.List;

/**
 * The default batch {@link GroupAlsoByWindowsDoFn} implementation, if no specialized "fast path"
 * implementation is applicable.
 */
@SystemDoFnInternal
public class GroupAlsoByWindowsViaOutputBufferDoFn<K, InputT, OutputT, W extends BoundedWindow>
   extends GroupAlsoByWindowsDoFn<K, InputT, OutputT, W> {

  private final WindowingStrategy<?, W> strategy;
  private SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn;

  public GroupAlsoByWindowsViaOutputBufferDoFn(
      WindowingStrategy<?, W> windowingStrategy,
      SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn) {
    this.strategy = windowingStrategy;
    this.reduceFn = reduceFn;
  }

  @Override
  public void processElement(
      DoFn<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>>.ProcessContext c)
          throws Exception {
    K key = c.element().getKey();
    // Used with Batch, we know that all the data is available for this key. We can't use the
    // timer manager from the context because it doesn't exist. So we create one and emulate the
    // watermark, knowing that we have all data and it is in timestamp order.
    BatchTimerInternals timerInternals = new BatchTimerInternals(Instant.now());

    // It is the responsibility of the user of GroupAlsoByWindowsViaOutputBufferDoFn to only
    // provide a WindowingInternals instance with the appropriate key type for StateInternals.
    @SuppressWarnings("unchecked")
    StateInternals<K> stateInternals = (StateInternals<K>) c.windowingInternals().stateInternals();

    ReduceFnRunner<K, InputT, OutputT, W> reduceFnRunner =
        new ReduceFnRunner<K, InputT, OutputT, W>(
            key,
            strategy,
            stateInternals,
            timerInternals,
            c.windowingInternals(),
            droppedDueToClosedWindow,
            reduceFn,
            c.getPipelineOptions());

    Iterable<List<WindowedValue<InputT>>> chunks =
        Iterables.partition(c.element().getValue(), 1000);
    for (Iterable<WindowedValue<InputT>> chunk : chunks) {
      // Process the chunk of elements.
      reduceFnRunner.processElements(chunk);

      // Then, since elements are sorted by their timestamp, advance the input watermark
      // to the first element, and fire any timers that may have been scheduled.
      timerInternals.advanceInputWatermark(reduceFnRunner, chunk.iterator().next().getTimestamp());

      // Fire any processing timers that need to fire
      timerInternals.advanceProcessingTime(reduceFnRunner, Instant.now());

      // Leave the output watermark undefined. Since there's no late data in batch mode
      // there's really no need to track it as we do for streaming.
    }

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(reduceFnRunner, BoundedWindow.TIMESTAMP_MAX_VALUE);

    // Finally, advance the processing time to infinity to fire any timers.
    timerInternals.advanceProcessingTime(reduceFnRunner, BoundedWindow.TIMESTAMP_MAX_VALUE);

    reduceFnRunner.persist();
  }
}
