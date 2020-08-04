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
package org.apache.beam.runners.flink.translation.functions;

import java.util.Collections;
import java.util.Objects;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * A Flink combine runner takes elements pre-grouped by window and produces output after seeing all
 * input.
 */
public class SingleWindowFlinkCombineRunner<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends AbstractFlinkCombineRunner<K, InputT, AccumT, OutputT, W> {

  @Override
  public void combine(
      FlinkCombiner<K, InputT, AccumT, OutputT> flinkCombiner,
      WindowingStrategy<Object, W> windowingStrategy,
      SideInputReader sideInputReader,
      PipelineOptions options,
      Iterable<WindowedValue<KV<K, InputT>>> elements,
      Collector<WindowedValue<KV<K, OutputT>>> out) {
    final TimestampCombiner timestampCombiner = windowingStrategy.getTimestampCombiner();
    final WindowFn<Object, W> windowFn = windowingStrategy.getWindowFn();
    final PeekingIterator<WindowedValue<KV<K, InputT>>> iterator =
        Iterators.peekingIterator(elements.iterator());

    @SuppressWarnings("unchecked")
    final W currentWindow = (W) Iterables.getOnlyElement(iterator.peek().getWindows());
    final K key = iterator.peek().getValue().getKey();

    Tuple2<AccumT, Instant> combinedState = null;
    while (iterator.hasNext()) {
      final WindowedValue<KV<K, InputT>> currentValue = iterator.next();
      Preconditions.checkState(
          currentWindow.equals(Iterables.getOnlyElement(currentValue.getWindows())),
          "Incompatible windows.");
      if (combinedState == null) {
        AccumT accumT =
            flinkCombiner.firstInput(
                key,
                currentValue.getValue().getValue(),
                options,
                sideInputReader,
                Collections.singleton(currentWindow));
        Instant windowTimestamp =
            timestampCombiner.assign(
                currentWindow, windowFn.getOutputTime(currentValue.getTimestamp(), currentWindow));
        combinedState = new Tuple2<>(accumT, windowTimestamp);
      } else {
        combinedState.f0 =
            flinkCombiner.addInput(
                key,
                combinedState.f0,
                currentValue.getValue().getValue(),
                options,
                sideInputReader,
                Collections.singleton(currentWindow));
        combinedState.f1 =
            timestampCombiner.combine(
                combinedState.f1,
                timestampCombiner.assign(
                    currentWindow,
                    windowingStrategy
                        .getWindowFn()
                        .getOutputTime(currentValue.getTimestamp(), currentWindow)));
      }
    }

    // Output the final value of combiners.
    Objects.requireNonNull(combinedState);
    final AccumT accumulator = combinedState.f0;
    final Instant windowTimestamp = combinedState.f1;
    out.collect(
        WindowedValue.of(
            KV.of(
                key,
                flinkCombiner.extractOutput(
                    key,
                    accumulator,
                    options,
                    sideInputReader,
                    Collections.singleton(currentWindow))),
            windowTimestamp,
            currentWindow,
            PaneInfo.NO_FIRING));
  }
}
