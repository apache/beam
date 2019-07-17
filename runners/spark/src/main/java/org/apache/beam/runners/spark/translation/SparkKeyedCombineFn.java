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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

/**
 * A {@link org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn} with a {@link
 * org.apache.beam.sdk.transforms.CombineWithContext.Context} for the SparkRunner.
 */
public class SparkKeyedCombineFn<K, InputT, AccumT, OutputT> extends SparkAbstractCombineFn {
  private final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn;

  public SparkKeyedCombineFn(
      CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
      SerializablePipelineOptions options,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, sideInputs, windowingStrategy);
    this.combineFn = combineFn;
  }

  /** Applying the combine function directly on a key's grouped values - post grouping. */
  public OutputT apply(WindowedValue<KV<K, Iterable<InputT>>> windowedKv) {
    // apply combine function on grouped values.
    return combineFn.apply(windowedKv.getValue().getValue(), ctxtForInput(windowedKv));
  }

  /**
   * Implements Spark's createCombiner function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  Iterable<WindowedValue<KV<K, AccumT>>> createCombiner(WindowedValue<KV<K, InputT>> wkvi) {
    // sort exploded inputs.
    Iterable<WindowedValue<KV<K, InputT>>> sortedInputs = sortByWindows(wkvi.explodeWindows());

    TimestampCombiner timestampCombiner = windowingStrategy.getTimestampCombiner();
    WindowFn<?, BoundedWindow> windowFn = windowingStrategy.getWindowFn();

    // --- inputs iterator, by window order.
    final Iterator<WindowedValue<KV<K, InputT>>> iterator = sortedInputs.iterator();
    WindowedValue<KV<K, InputT>> currentInput = iterator.next();
    BoundedWindow currentWindow = Iterables.getFirst(currentInput.getWindows(), null);

    // first create the accumulator and accumulate first input.
    K key = currentInput.getValue().getKey();
    AccumT accumulator = combineFn.createAccumulator(ctxtForInput(currentInput));
    accumulator =
        combineFn.addInput(
            accumulator, currentInput.getValue().getValue(), ctxtForInput(currentInput));

    // keep track of the timestamps assigned by the TimestampCombiner.
    Instant windowTimestamp =
        timestampCombiner.assign(
            currentWindow,
            windowingStrategy
                .getWindowFn()
                .getOutputTime(currentInput.getTimestamp(), currentWindow));

    // accumulate the next windows, or output.
    List<WindowedValue<KV<K, AccumT>>> output = Lists.newArrayList();

    // if merging, merge overlapping windows, e.g. Sessions.
    final boolean merging = !windowingStrategy.getWindowFn().isNonMerging();

    while (iterator.hasNext()) {
      WindowedValue<KV<K, InputT>> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      boolean mergingAndIntersecting =
          merging && isIntersecting((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);

      if (mergingAndIntersecting || nextWindow.equals(currentWindow)) {
        if (mergingAndIntersecting) {
          // merge intersecting windows.
          currentWindow = merge((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);
        }
        // keep accumulating and carry on ;-)
        accumulator =
            combineFn.addInput(
                accumulator, nextValue.getValue().getValue(), ctxtForInput(nextValue));
        windowTimestamp =
            timestampCombiner.combine(
                windowTimestamp,
                timestampCombiner.assign(
                    currentWindow,
                    windowFn.getOutputTime(nextValue.getTimestamp(), currentWindow)));
      } else {
        // moving to the next window, first add the current accumulation to output
        // and initialize the accumulator.
        output.add(
            WindowedValue.of(
                KV.of(key, accumulator), windowTimestamp, currentWindow, PaneInfo.NO_FIRING));
        // re-init accumulator, window and timestamp.
        accumulator = combineFn.createAccumulator(ctxtForInput(nextValue));
        accumulator =
            combineFn.addInput(
                accumulator, nextValue.getValue().getValue(), ctxtForInput(nextValue));
        currentWindow = nextWindow;
        windowTimestamp =
            timestampCombiner.assign(
                currentWindow, windowFn.getOutputTime(nextValue.getTimestamp(), currentWindow));
      }
    }

    // add last accumulator to the output.
    output.add(
        WindowedValue.of(
            KV.of(key, accumulator), windowTimestamp, currentWindow, PaneInfo.NO_FIRING));

    return output;
  }

  /**
   * Implements Spark's mergeValue function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  Iterable<WindowedValue<KV<K, AccumT>>> mergeValue(
      WindowedValue<KV<K, InputT>> wkvi, Iterable<WindowedValue<KV<K, AccumT>>> wkvas) {
    // by calling createCombiner on the inputs and afterwards merging the accumulators,we avoid
    // an explode&accumulate for the input that will result in poor O(n^2) performance:
    // first sort the exploded input - O(nlogn).
    // follow with an accumulators sort = O(mlogm).
    // now for each (exploded) input, find a matching accumulator (if exists) to merge into, or
    // create a new one - O(n*m).
    // this results in - O(nlogn) + O(mlogm) + O(n*m) ~> O(n^2)
    // instead, calling createCombiner will create accumulators from the input - O(nlogn) + O(n).
    // now, calling mergeCombiners will finally result in - O((n+m)log(n+m)) + O(n+m) ~> O(nlogn).
    return mergeCombiners(createCombiner(wkvi), wkvas);
  }

  /**
   * Implements Spark's mergeCombiners function in:
   *
   * <p>{@link org.apache.spark.rdd.PairRDDFunctions#combineByKey}.
   */
  Iterable<WindowedValue<KV<K, AccumT>>> mergeCombiners(
      Iterable<WindowedValue<KV<K, AccumT>>> a1, Iterable<WindowedValue<KV<K, AccumT>>> a2) {
    // concatenate accumulators.
    Iterable<WindowedValue<KV<K, AccumT>>> accumulators = Iterables.concat(a1, a2);

    // sort accumulators, no need to explode since inputs were exploded.
    Iterable<WindowedValue<KV<K, AccumT>>> sortedAccumulators = sortByWindows(accumulators);

    TimestampCombiner timestampCombiner = windowingStrategy.getTimestampCombiner();

    // --- accumulators iterator, by window order.
    final Iterator<WindowedValue<KV<K, AccumT>>> iterator = sortedAccumulators.iterator();

    // get the first accumulator and assign it to the current window's accumulators.
    WindowedValue<KV<K, AccumT>> currentValue = iterator.next();
    K key = currentValue.getValue().getKey();
    BoundedWindow currentWindow = Iterables.getFirst(currentValue.getWindows(), null);
    List<AccumT> currentWindowAccumulators = Lists.newArrayList();
    currentWindowAccumulators.add(currentValue.getValue().getValue());

    // keep track of the timestamps assigned by the TimestampCombiner,
    // in createCombiner we already merge the timestamps assigned
    // to individual elements, here we will just merge them.
    List<Instant> windowTimestamps = Lists.newArrayList();
    windowTimestamps.add(currentValue.getTimestamp());

    // accumulate the next windows, or output.
    List<WindowedValue<KV<K, AccumT>>> output = Lists.newArrayList();

    // if merging, merge overlapping windows, e.g. Sessions.
    final boolean merging = !windowingStrategy.getWindowFn().isNonMerging();

    while (iterator.hasNext()) {
      WindowedValue<KV<K, AccumT>> nextValue = iterator.next();
      BoundedWindow nextWindow = Iterables.getOnlyElement(nextValue.getWindows());

      boolean mergingAndIntersecting =
          merging && isIntersecting((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);

      if (mergingAndIntersecting || nextWindow.equals(currentWindow)) {
        if (mergingAndIntersecting) {
          // merge intersecting windows.
          currentWindow = merge((IntervalWindow) currentWindow, (IntervalWindow) nextWindow);
        }
        // add to window accumulators.
        currentWindowAccumulators.add(nextValue.getValue().getValue());
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
        WindowedValue<KV<K, Iterable<AccumT>>> preMergeWindowedValue =
            WindowedValue.of(
                KV.of(key, accumsToMerge), mergedTimestamp, currentWindow, PaneInfo.NO_FIRING);
        // applying the actual combiner onto the accumulators.
        AccumT accumulated =
            combineFn.mergeAccumulators(accumsToMerge, ctxtForInput(preMergeWindowedValue));
        WindowedValue<KV<K, AccumT>> postMergeWindowedValue =
            preMergeWindowedValue.withValue(KV.of(key, accumulated));
        // emit the accumulated output.
        output.add(postMergeWindowedValue);

        // re-init accumulator, window and timestamps.
        currentWindowAccumulators.clear();
        currentWindowAccumulators.add(nextValue.getValue().getValue());
        currentWindow = nextWindow;
        windowTimestamps.clear();
        windowTimestamps.add(nextValue.getTimestamp());
      }
    }

    // merge the last chunk of accumulators.
    Instant mergedTimestamp = timestampCombiner.merge(currentWindow, windowTimestamps);
    Iterable<AccumT> accumsToMerge = Iterables.unmodifiableIterable(currentWindowAccumulators);
    WindowedValue<KV<K, Iterable<AccumT>>> preMergeWindowedValue =
        WindowedValue.of(
            KV.of(key, accumsToMerge), mergedTimestamp, currentWindow, PaneInfo.NO_FIRING);
    AccumT accumulated =
        combineFn.mergeAccumulators(accumsToMerge, ctxtForInput(preMergeWindowedValue));
    WindowedValue<KV<K, AccumT>> postMergeWindowedValue =
        preMergeWindowedValue.withValue(KV.of(key, accumulated));
    output.add(postMergeWindowedValue);

    return output;
  }

  Iterable<WindowedValue<OutputT>> extractOutput(Iterable<WindowedValue<KV<K, AccumT>>> wkvas) {
    return StreamSupport.stream(wkvas.spliterator(), false)
        .map(
            wkva -> {
              if (wkva == null) {
                return null;
              }
              AccumT accumulator = wkva.getValue().getValue();
              return wkva.withValue(combineFn.extractOutput(accumulator, ctxtForInput(wkva)));
            })
        .collect(Collectors.toList());
  }
}
