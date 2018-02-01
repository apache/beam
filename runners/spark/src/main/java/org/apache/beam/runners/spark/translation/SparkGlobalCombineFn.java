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

package org.apache.beam.runners.spark.translation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A {@link org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn}
 * with a {@link CombineWithContext.Context} for the SparkRunner.
 */
public class SparkGlobalCombineFn<InputT, AccumT, OutputT> extends SparkAbstractCombineFn {
  private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;

  public SparkGlobalCombineFn(
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, sideInputs, windowingStrategy);
    this.combineFn = combineFn;
  }

  /**
   * Implements Spark's zeroValue function in:
   * <p>
   * {@link org.apache.spark.api.java.JavaRDD#aggregate}.
   * </p>
   */
  Iterable<WindowedValue<AccumT>> zeroValue() {
    return Lists.newArrayList();
  }

  private Iterable<WindowedValue<AccumT>> createAccumulator(WindowedValue<InputT> input) {

    // sort exploded inputs.
    Iterable<WindowedValue<InputT>> sortedInputs = sortByWindows(input.explodeWindows());

    TimestampCombiner timestampCombiner = windowingStrategy.getTimestampCombiner();
    WindowFn<?, BoundedWindow> windowFn = windowingStrategy.getWindowFn();

    //--- inputs iterator, by window order.
    final Iterator<WindowedValue<InputT>> iterator = sortedInputs.iterator();
    WindowedValue<InputT> currentInput = iterator.next();
    BoundedWindow currentWindow = Iterables.getFirst(currentInput.getWindows(), null);

    // first create the accumulator and accumulate first input.
    AccumT accumulator = combineFn.createAccumulator(ctxtForInput(currentInput));
    accumulator = combineFn.addInput(accumulator, currentInput.getValue(),
        ctxtForInput(currentInput));

    // keep track of the timestamps assigned by the TimestampCombiner.
    Instant windowTimestamp =
        timestampCombiner.assign(
            currentWindow,
            windowingStrategy
                .getWindowFn()
                .getOutputTime(currentInput.getTimestamp(), currentWindow));

    // accumulate the next windows, or output.
    List<WindowedValue<AccumT>> output = Lists.newArrayList();

    // if merging, merge overlapping windows, e.g. Sessions.
    final boolean merging = !windowingStrategy.getWindowFn().isNonMerging();

    while (iterator.hasNext()) {
      WindowedValue<InputT> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      boolean mergingAndIntersecting = merging
          && isIntersecting((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);

      if (mergingAndIntersecting || nextWindow.equals(currentWindow)) {
        if (mergingAndIntersecting) {
          // merge intersecting windows.
          currentWindow = merge((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);
        }
        // keep accumulating and carry on ;-)
        accumulator = combineFn.addInput(accumulator, nextValue.getValue(),
            ctxtForInput(nextValue));
        windowTimestamp =
            timestampCombiner.merge(
                currentWindow,
                windowTimestamp,
                windowingStrategy
                    .getWindowFn()
                    .getOutputTime(nextValue.getTimestamp(), currentWindow));
      } else {
        // moving to the next window, first add the current accumulation to output
        // and initialize the accumulator.
        output.add(WindowedValue.of(accumulator, windowTimestamp, currentWindow,
            PaneInfo.NO_FIRING));
        // re-init accumulator, window and timestamp.
        accumulator = combineFn.createAccumulator(ctxtForInput(nextValue));
        accumulator = combineFn.addInput(accumulator, nextValue.getValue(),
            ctxtForInput(nextValue));
        currentWindow = nextWindow;
        windowTimestamp = timestampCombiner.assign(currentWindow,
            windowFn.getOutputTime(nextValue.getTimestamp(), currentWindow));
      }
    }

    // add last accumulator to the output.
    output.add(WindowedValue.of(accumulator, windowTimestamp, currentWindow, PaneInfo.NO_FIRING));

    return output;
  }

  /**
   * Implement Spark's seqOp function in:
   * <p>
   * {@link org.apache.spark.api.java.JavaRDD#aggregate}.
   * </p>
   */
  Iterable<WindowedValue<AccumT>> seqOp(Iterable<WindowedValue<AccumT>> accum,
                                        WindowedValue<InputT> input) {
    return combOp(accum, createAccumulator(input));
  }

  /**
   * Implement Spark's combOp function in:
   * <p>
   * {@link org.apache.spark.api.java.JavaRDD#aggregate}.
   * </p>
   */
  Iterable<WindowedValue<AccumT>> combOp(Iterable<WindowedValue<AccumT>> a1,
                                         Iterable<WindowedValue<AccumT>> a2) {

    // concatenate accumulators.
    Iterable<WindowedValue<AccumT>> accumulators = Iterables.concat(a1, a2);
    // if empty, return an empty accumulators iterable.
    if (!accumulators.iterator().hasNext()) {
      return Lists.newArrayList();
    }

    // sort accumulators, no need to explode since inputs were exploded.
    Iterable<WindowedValue<AccumT>> sortedAccumulators = sortByWindows(accumulators);

    @SuppressWarnings("unchecked")
    TimestampCombiner timestampCombiner = windowingStrategy.getTimestampCombiner();

    //--- accumulators iterator, by window order.
    final Iterator<WindowedValue<AccumT>> iterator = sortedAccumulators.iterator();

    // get the first accumulator and assign it to the current window's accumulators.
    WindowedValue<AccumT> currentValue = iterator.next();
    BoundedWindow currentWindow = Iterables.getFirst(currentValue.getWindows(), null);
    List<AccumT> currentWindowAccumulators = Lists.newArrayList();
    currentWindowAccumulators.add(currentValue.getValue());

    // keep track of the timestamps assigned by the TimestampCombiner,
    // in createCombiner we already merge the timestamps assigned
    // to individual elements, here we will just merge them.
    List<Instant> windowTimestamps = Lists.newArrayList();
    windowTimestamps.add(currentValue.getTimestamp());

    // accumulate the next windows, or output.
    List<WindowedValue<AccumT>> output = Lists.newArrayList();

    // if merging, merge overlapping windows, e.g. Sessions.
    final boolean merging = !windowingStrategy.getWindowFn().isNonMerging();

    while (iterator.hasNext()) {
      WindowedValue<AccumT> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      boolean mergingAndIntersecting = merging
          && isIntersecting((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);

      if (mergingAndIntersecting || nextWindow.equals(currentWindow)) {
        if (mergingAndIntersecting) {
          // merge intersecting windows.
          currentWindow = merge((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);
        }
        // add to window accumulators.
        currentWindowAccumulators.add(nextValue.getValue());
        windowTimestamps.add(nextValue.getTimestamp());
      } else {
        // before moving to the next window,
        // add the current accumulation to the output and initialize the accumulation.

        // merge the timestamps of all accumulators to merge.
        Instant mergedTimestamp = timestampCombiner.merge(currentWindow, windowTimestamps);

        // merge accumulators.
        // transforming a KV<K, Iterable<AccumT>> into a KV<K, Iterable<AccumT>>.
        // for the (possibly merged) window.
        Iterable<AccumT> accumsToMerge = Iterables.unmodifiableIterable(currentWindowAccumulators);
        WindowedValue<Iterable<AccumT>> preMergeWindowedValue = WindowedValue.of(
            accumsToMerge, mergedTimestamp, currentWindow, PaneInfo.NO_FIRING);
        // applying the actual combiner onto the accumulators.
        AccumT accumulated = combineFn.mergeAccumulators(accumsToMerge,
            ctxtForInput(preMergeWindowedValue));
        WindowedValue<AccumT> postMergeWindowedValue = preMergeWindowedValue.withValue(accumulated);
        // emit the accumulated output.
        output.add(postMergeWindowedValue);

        // re-init accumulator, window and timestamps.
        currentWindowAccumulators.clear();
        currentWindowAccumulators.add(nextValue.getValue());
        currentWindow = nextWindow;
        windowTimestamps.clear();
        windowTimestamps.add(nextValue.getTimestamp());
      }
    }

    // merge the last chunk of accumulators.
    Instant mergedTimestamp = timestampCombiner.merge(currentWindow, windowTimestamps);
    Iterable<AccumT> accumsToMerge = Iterables.unmodifiableIterable(currentWindowAccumulators);
    WindowedValue<Iterable<AccumT>> preMergeWindowedValue = WindowedValue.of(
        accumsToMerge, mergedTimestamp, currentWindow, PaneInfo.NO_FIRING);
    AccumT accumulated = combineFn.mergeAccumulators(accumsToMerge,
        ctxtForInput(preMergeWindowedValue));
    WindowedValue<AccumT> postMergeWindowedValue = preMergeWindowedValue.withValue(accumulated);
    output.add(postMergeWindowedValue);

    return output;
  }

  Iterable<WindowedValue<OutputT>> extractOutput(Iterable<WindowedValue<AccumT>> wvas) {
    return StreamSupport.stream(wvas.spliterator(), false)
        .map(
            wva -> {
              if (wva == null) {
                return null;
              }
              return wva.withValue(combineFn.extractOutput(wva.getValue(), ctxtForInput(wva)));
            })
        .collect(Collectors.toList());
  }
}
