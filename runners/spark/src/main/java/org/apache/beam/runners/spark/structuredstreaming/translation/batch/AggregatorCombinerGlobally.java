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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.joda.time.Instant;
import scala.Tuple2;

/** An {@link Aggregator} for the Spark Batch Runner. It does not use ReduceFnRunner
 * for windowMerging, because reduceFnRunner is based on state which requires a keyed collection.
 * The accumulator is a {@code Iterable<WindowedValue<AccumT>> because an {@code InputT} can be in multiple windows. So, when accumulating {@code InputT} values, we create one accumulator per input window.
 * */

class AggregatorCombinerGlobally<InputT, AccumT, OutputT, W extends BoundedWindow>
    extends Aggregator<Tuple2<Integer, WindowedValue<InputT>>, Iterable<WindowedValue<AccumT>>, Iterable<WindowedValue<OutputT>>> {

  private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
  private WindowingStrategy<InputT, W> windowingStrategy;
  private TimestampCombiner timestampCombiner;

  public AggregatorCombinerGlobally(Combine.CombineFn<InputT, AccumT, OutputT> combineFn, WindowingStrategy<?, ?> windowingStrategy) {
    this.combineFn = combineFn;
    this.windowingStrategy = (WindowingStrategy<InputT, W>) windowingStrategy;
    this.timestampCombiner = windowingStrategy.getTimestampCombiner();
  }

  @Override public Iterable<WindowedValue<AccumT>> zero() {
    return new ArrayList<>();
  }

  @Override public Iterable<WindowedValue<AccumT>> reduce(Iterable<WindowedValue<AccumT>> accumulators,
      Tuple2<Integer, WindowedValue<InputT>> inputTupe) {

    WindowedValue<InputT> input = inputTupe._2;
    //concatenate accumulators windows and input windows and merge the windows
    Collection<W> inputWindows = (Collection<W>)input.getWindows();
    Set<W> windows = collectAccumulatorsWindows(accumulators);
    windows.addAll(inputWindows);
    Map<W, W> windowToMergeResult;
    try {
      windowToMergeResult = mergeWindows(windowingStrategy, windows);
    } catch (Exception e) {
      throw new RuntimeException("Unable to merge accumulators windows and input windows", e);
    }

    // iterate through the input windows and for each, create an accumulator with the merged window
    // associated to it and call addInput with the accumulator.
    // Maintain a map of the accumulators for use as output
    Map<W, Tuple2<AccumT, Instant>> mapState = new HashMap<>();
    for (W inputWindow:inputWindows) {
      W mergedWindow = windowToMergeResult.get(inputWindow);
      mergedWindow = mergedWindow == null ? inputWindow : mergedWindow;
      Tuple2<AccumT, Instant> accumAndInstant = mapState.get(mergedWindow);
      // if there is no accumulator associated with this window yet, create one
      if (accumAndInstant == null) {
        AccumT accum = combineFn.addInput(combineFn.createAccumulator(), input.getValue());
        Instant windowTimestamp =
            timestampCombiner.assign(
                mergedWindow, windowingStrategy.getWindowFn().getOutputTime(input.getTimestamp(), mergedWindow));
        accumAndInstant = new Tuple2<>(accum, windowTimestamp);
        mapState.put(mergedWindow, accumAndInstant);
      } else {
        AccumT updatedAccum =
            combineFn.addInput(accumAndInstant._1, input.getValue());
        Instant updatedTimestamp = timestampCombiner.combine(accumAndInstant._2, timestampCombiner
            .assign(mergedWindow,
                windowingStrategy.getWindowFn().getOutputTime(input.getTimestamp(), mergedWindow)));
        accumAndInstant = new Tuple2<>(updatedAccum, updatedTimestamp);
      }
    }
    // output the accumulators map
    List<WindowedValue<AccumT>> result = new ArrayList<>();
    for (Map.Entry<W, Tuple2<AccumT, Instant>> entry : mapState.entrySet()) {
      AccumT accumulator = entry.getValue()._1;
      Instant windowTimestamp = entry.getValue()._2;
      W window = entry.getKey();
      result.add(WindowedValue.of(accumulator, windowTimestamp, window, PaneInfo.NO_FIRING));
    }
    return result;
  }

  @Override public Iterable<WindowedValue<AccumT>> merge(
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
    Map<W, List<WindowedValue<AccumT>>> mergedWindowToAccumulators = new HashMap<>();
    for (WindowedValue<AccumT> accumulator : accumulators) {
      //each accumulator has only one window
      BoundedWindow accumulatorWindow = accumulator.getWindows().iterator().next();
      W mergedWindowForAccumulator = windowToMergeResult.get(accumulatorWindow);
      if (mergedWindowToAccumulators.get(mergedWindowForAccumulator) == null){
        mergedWindowToAccumulators.put(mergedWindowForAccumulator, Collections.singletonList(accumulator));
      }
      else {
        mergedWindowToAccumulators.get(mergedWindowForAccumulator).add(accumulator);
      }
    }
    // merge the accumulators for each mergedWindow
    List<WindowedValue<AccumT>> result = new ArrayList<>();
    for (Map.Entry<W, List<WindowedValue<AccumT>>> entry : mergedWindowToAccumulators.entrySet()){
      W mergedWindow = entry.getKey();
      List<WindowedValue<AccumT>> accumulatorsForMergedWindow = entry.getValue();
      result.add(WindowedValue
          .of(combineFn.mergeAccumulators(accumulatorsForMergedWindow.stream().map(x -> x.getValue()).collect(
              Collectors.toList())), timestampCombiner.combine(accumulatorsForMergedWindow.stream().map(x -> x.getTimestamp()).collect(
              Collectors.toList())),
              mergedWindow, PaneInfo.NO_FIRING));
    }
    return result;
  }

  @Override public Iterable<WindowedValue<OutputT>> finish(Iterable<WindowedValue<AccumT>> reduction) {
    List<WindowedValue<OutputT>> result = new ArrayList<>();
    for (WindowedValue<AccumT> windowedValue: reduction) {
      result.add(windowedValue.withValue(combineFn.extractOutput(windowedValue.getValue())));
    }
    return result;
  }

  @Override public Encoder<Iterable<WindowedValue<AccumT>>> bufferEncoder() {
    // TODO replace with accumulatorCoder if possible
    return EncoderHelpers.genericEncoder();
  }

  @Override public Encoder<Iterable<WindowedValue<OutputT>>> outputEncoder() {
    // TODO replace with outputCoder if possible
    return EncoderHelpers.genericEncoder();
  }

  private Set<W> collectAccumulatorsWindows(Iterable<WindowedValue<AccumT>> accumulators) {
    Set<W> windows = new HashSet<>();
    for (WindowedValue<?> accumulator : accumulators) {
      // an accumulator has only one window associated to it.
      W accumulatorWindow = (W) accumulator.getWindows().iterator().next();
      windows.add(accumulatorWindow);
    } return windows;
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
