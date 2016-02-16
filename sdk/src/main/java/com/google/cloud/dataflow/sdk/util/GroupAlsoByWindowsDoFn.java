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

/**
 * DoFn that merges windows and groups elements in those windows, optionally
 * combining values.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
@SystemDoFnInternal
public abstract class GroupAlsoByWindowsDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> {
  public static final String DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER = "DroppedDueToClosedWindow";
  public static final String DROPPED_DUE_TO_LATENESS_COUNTER = "DroppedDueToLateness";

  protected final Aggregator<Long, Long> droppedDueToClosedWindow =
      createAggregator(DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER, new Sum.SumLongFn());
  protected final Aggregator<Long, Long> droppedDueToLateness =
      createAggregator(DROPPED_DUE_TO_LATENESS_COUNTER, new Sum.SumLongFn());

  /**
   * Create the default {@link GroupAlsoByWindowsDoFn}, which uses window sets to implement the
   * grouping.
   *
   * @param windowingStrategy The window function and trigger to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow> GroupAlsoByWindowsDoFn<K, V, Iterable<V>, W>
      createDefault(WindowingStrategy<?, W> windowingStrategy, Coder<V> inputCoder) {
    return new GroupAlsoByWindowsViaOutputBufferDoFn<>(
        windowingStrategy, SystemReduceFn.<K, V, W>buffering(inputCoder));
  }
}
