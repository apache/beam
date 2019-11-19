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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.joda.time.Instant;
import scala.Tuple2;

/** An {@link Aggregator} for the Spark Batch Runner.
 * The accumulator is a {@code Iterable<WindowedValue<AccumT>> because an {@code InputT} can be in multiple windows. So, when accumulating {@code InputT} values, we create one accumulator per input window.
 * */
class AggregatorCombiner<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends Aggregator<
        WindowedValue<KV<K, InputT>>,
        Iterable<WindowedValue<AccumT>>,
        Iterable<WindowedValue<OutputT>>> {

  private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
  private WindowingStrategy<InputT, W> windowingStrategy;
  private TimestampCombiner timestampCombiner;
  private IterableCoder<WindowedValue<AccumT>> accumulatorCoder;
  private IterableCoder<WindowedValue<OutputT>> outputCoder;

  public AggregatorCombiner(
      Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Coder<AccumT> accumulatorCoder,
      Coder<OutputT> outputCoder) {
    this.combineFn = combineFn;
    this.windowingStrategy = (WindowingStrategy<InputT, W>) windowingStrategy;
    this.timestampCombiner = windowingStrategy.getTimestampCombiner();
    this.accumulatorCoder =
        IterableCoder.of(
            WindowedValue.FullWindowedValueCoder.of(
                accumulatorCoder, windowingStrategy.getWindowFn().windowCoder()));
    this.outputCoder =
        IterableCoder.of(
            WindowedValue.FullWindowedValueCoder.of(
                outputCoder, windowingStrategy.getWindowFn().windowCoder()));
  }

  @Override
  public Iterable<WindowedValue<AccumT>> zero() {
    return new ArrayList<>();
  }

  private Iterable<WindowedValue<AccumT>> createAccumulator(WindowedValue<KV<K, InputT>> inputWv) {
    // need to create an accumulator because combineFn can modify its input accumulator.
    AccumT accumulator = combineFn.createAccumulator();
    AccumT accumT = combineFn.addInput(accumulator, inputWv.getValue().getValue());
    return Lists.newArrayList(
        WindowedValue.of(accumT, inputWv.getTimestamp(), inputWv.getWindows(), inputWv.getPane()));
  }

  @Override
  public Iterable<WindowedValue<AccumT>> reduce(
      Iterable<WindowedValue<AccumT>> accumulators, WindowedValue<KV<K, InputT>> inputWv) {
    return merge(accumulators, createAccumulator(inputWv));
  }

  @Override
  public Iterable<WindowedValue<AccumT>> merge(
      Iterable<WindowedValue<AccumT>> accumulators1,
      Iterable<WindowedValue<AccumT>> accumulators2) {

    // merge the windows of all the accumulators
    Iterable<WindowedValue<AccumT>> accumulators = Iterables.concat(accumulators1, accumulators2);
    Set<W> accumulatorsWindows = collectAccumulatorsWindows(accumulators);
    Map<W, W> windowToMergeResult;
    try {
      windowToMergeResult = mergeWindows(windowingStrategy, accumulatorsWindows);
    } catch (Exception e) {
      throw new RuntimeException("Unable to merge accumulators windows", e);
    }

    // group accumulators by their merged window
    Map<W, List<Tuple2<AccumT, Instant>>> mergedWindowToAccumulators = new HashMap<>();
    for (WindowedValue<AccumT> accumulatorWv : accumulators) {
      for (BoundedWindow accumulatorWindow : accumulatorWv.getWindows()) {
        W mergedWindowForAccumulator = windowToMergeResult.get(accumulatorWindow);
        mergedWindowForAccumulator =
            (mergedWindowForAccumulator == null)
                ? (W) accumulatorWindow
                : mergedWindowForAccumulator;

        // we need only the timestamp and the AccumT, we create a tuple
        Tuple2<AccumT, Instant> accumAndInstant =
            new Tuple2<>(
                accumulatorWv.getValue(),
                timestampCombiner.assign(
                    mergedWindowForAccumulator,
                    windowingStrategy
                        .getWindowFn()
                        .getOutputTime(accumulatorWv.getTimestamp(), mergedWindowForAccumulator)));
        if (mergedWindowToAccumulators.get(mergedWindowForAccumulator) == null) {
          mergedWindowToAccumulators.put(
              mergedWindowForAccumulator, Lists.newArrayList(accumAndInstant));
        } else {
          mergedWindowToAccumulators.get(mergedWindowForAccumulator).add(accumAndInstant);
        }
      }
    }
    // merge the accumulators for each mergedWindow
    List<WindowedValue<AccumT>> result = new ArrayList<>();
    for (Map.Entry<W, List<Tuple2<AccumT, Instant>>> entry :
        mergedWindowToAccumulators.entrySet()) {
      W mergedWindow = entry.getKey();
      List<Tuple2<AccumT, Instant>> accumsAndInstantsForMergedWindow = entry.getValue();

      // we need to create the first accumulator because combineFn.mergerAccumulators can modify the
      // first accumulator
      AccumT first = combineFn.createAccumulator();
      Iterable<AccumT> accumulatorsToMerge =
          Iterables.concat(
              Collections.singleton(first),
              accumsAndInstantsForMergedWindow.stream()
                  .map(x -> x._1())
                  .collect(Collectors.toList()));
      result.add(
          WindowedValue.of(
              combineFn.mergeAccumulators(accumulatorsToMerge),
              timestampCombiner.combine(
                  accumsAndInstantsForMergedWindow.stream()
                      .map(x -> x._2())
                      .collect(Collectors.toList())),
              mergedWindow,
              PaneInfo.NO_FIRING));
    }
    return result;
  }

  @Override
  public Iterable<WindowedValue<OutputT>> finish(Iterable<WindowedValue<AccumT>> reduction) {
    List<WindowedValue<OutputT>> result = new ArrayList<>();
    for (WindowedValue<AccumT> windowedValue : reduction) {
      result.add(windowedValue.withValue(combineFn.extractOutput(windowedValue.getValue())));
    }
    return result;
  }

  @Override
  public Encoder<Iterable<WindowedValue<AccumT>>> bufferEncoder() {
    return EncoderHelpers.fromBeamCoder(accumulatorCoder);
  }

  @Override
  public Encoder<Iterable<WindowedValue<OutputT>>> outputEncoder() {
    return EncoderHelpers.fromBeamCoder(outputCoder);
  }

  private Set<W> collectAccumulatorsWindows(Iterable<WindowedValue<AccumT>> accumulators) {
    Set<W> windows = new HashSet<>();
    for (WindowedValue<?> accumulator : accumulators) {
      for (BoundedWindow untypedWindow : accumulator.getWindows()) {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;
        windows.add(window);
      }
    }
    return windows;
  }

  private Map<W, W> mergeWindows(WindowingStrategy<InputT, W> windowingStrategy, Set<W> windows)
      throws Exception {
    WindowFn<InputT, W> windowFn = windowingStrategy.getWindowFn();

    if (windowingStrategy.getWindowFn().isNonMerging()) {
      // Return an empty map, indicating that every window is not merged.
      return Collections.emptyMap();
    }

    Map<W, W> windowToMergeResult = new HashMap<>();
    windowFn.mergeWindows(new MergeContextImpl(windowFn, windows, windowToMergeResult));
    return windowToMergeResult;
  }

  private class MergeContextImpl extends WindowFn<InputT, W>.MergeContext {

    private Set<W> windows;
    private Map<W, W> windowToMergeResult;

    MergeContextImpl(WindowFn<InputT, W> windowFn, Set<W> windows, Map<W, W> windowToMergeResult) {
      windowFn.super();
      this.windows = windows;
      this.windowToMergeResult = windowToMergeResult;
    }

    @Override
    public Collection<W> windows() {
      return windows;
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      for (W w : toBeMerged) {
        windowToMergeResult.put(w, mergeResult);
      }
    }
  }
}
