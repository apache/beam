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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * {@link OldDoFn.ProcessContext} for our Flink Wrappers.
 */
class FlinkProcessContext<InputT, OutputT>
    extends OldDoFn<InputT, OutputT>.ProcessContext {

  private final PipelineOptions pipelineOptions;
  private final RuntimeContext runtimeContext;
  private Collector<WindowedValue<OutputT>> collector;
  private final boolean requiresWindowAccess;

  protected WindowedValue<InputT> windowedValue;

  protected WindowingStrategy<?, ?> windowingStrategy;

  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  FlinkProcessContext(
      PipelineOptions pipelineOptions,
      RuntimeContext runtimeContext,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Collector<WindowedValue<OutputT>> collector,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs) {
    doFn.super();
    checkNotNull(pipelineOptions);
    checkNotNull(runtimeContext);
    checkNotNull(doFn);
    checkNotNull(collector);

    this.pipelineOptions = pipelineOptions;
    this.runtimeContext = runtimeContext;
    this.collector = collector;
    this.requiresWindowAccess = doFn instanceof OldDoFn.RequiresWindowAccess;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;

    super.setupDelegateAggregators();
  }

  FlinkProcessContext(
      PipelineOptions pipelineOptions,
      RuntimeContext runtimeContext,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs) {
    doFn.super();
    checkNotNull(pipelineOptions);
    checkNotNull(runtimeContext);
    checkNotNull(doFn);

    this.pipelineOptions = pipelineOptions;
    this.runtimeContext = runtimeContext;
    this.collector = null;
    this.requiresWindowAccess = doFn instanceof OldDoFn.RequiresWindowAccess;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;

    super.setupDelegateAggregators();
  }

  public FlinkProcessContext<InputT, OutputT> forOutput(
      Collector<WindowedValue<OutputT>> collector) {
    this.collector = collector;

    // for now, returns ourselves, to be easy on the GC
    return this;
  }



  public FlinkProcessContext<InputT, OutputT> forWindowedValue(
      WindowedValue<InputT> windowedValue) {
    this.windowedValue = windowedValue;

    // for now, returns ourselves, to be easy on the GC
    return this;
  }

  @Override
  public InputT element() {
    return this.windowedValue.getValue();
  }


  @Override
  public Instant timestamp() {
    return windowedValue.getTimestamp();
  }

  @Override
  public BoundedWindow window() {
    if (!requiresWindowAccess) {
      throw new UnsupportedOperationException(
          "window() is only available in the context of a OldDoFn marked as RequiresWindowAccess.");
    }
    return Iterables.getOnlyElement(windowedValue.getWindows());
  }

  @Override
  public PaneInfo pane() {
    return windowedValue.getPane();
  }

  @Override
  public WindowingInternals<InputT, OutputT> windowingInternals() {

    return new WindowingInternals<InputT, OutputT>() {

      @Override
      public StateInternals stateInternals() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void outputWindowedValue(
          OutputT value,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        collector.collect(WindowedValue.of(value, timestamp, windows, pane));
        outputWithTimestampAndWindow(value, timestamp, windows, pane);
      }

      @Override
      public TimerInternals timerInternals() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Collection<? extends BoundedWindow> windows() {
        return windowedValue.getWindows();
      }

      @Override
      public PaneInfo pane() {
        return windowedValue.getPane();
      }

      @Override
      public <T> void writePCollectionViewData(TupleTag<?> tag,
          Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public <ViewT> ViewT sideInput(
          PCollectionView<ViewT> view,
          BoundedWindow mainInputWindow) {

        checkNotNull(view, "View passed to sideInput cannot be null");
        checkNotNull(
            sideInputs.get(view),
            "Side input for " + view + " not available.");

        // get the side input strategy for mapping the window
        WindowingStrategy<?, ?> windowingStrategy = sideInputs.get(view);

        BoundedWindow sideInputWindow =
            windowingStrategy.getWindowFn().getSideInputWindow(mainInputWindow);

        Map<BoundedWindow, ViewT> sideInputs =
            runtimeContext.getBroadcastVariableWithInitializer(
                view.getTagInternal().getId(), new SideInputInitializer<>(view));
        return sideInputs.get(sideInputWindow);
      }
    };
  }

  @Override
  public PipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  @Override
  public <ViewT> ViewT sideInput(PCollectionView<ViewT> view) {
    checkNotNull(view, "View passed to sideInput cannot be null");
    checkNotNull(sideInputs.get(view), "Side input for " + view + " not available.");
    Iterator<? extends BoundedWindow> windowIter = windowedValue.getWindows().iterator();
    BoundedWindow window;
    if (!windowIter.hasNext()) {
      throw new IllegalStateException(
          "sideInput called when main input element is not in any windows");
    } else {
      window = windowIter.next();
      if (windowIter.hasNext()) {
        throw new IllegalStateException(
            "sideInput called when main input element is in multiple windows");
      }
    }

    // get the side input strategy for mapping the window
    WindowingStrategy<?, ?> windowingStrategy = sideInputs.get(view);

    BoundedWindow sideInputWindow =
        windowingStrategy.getWindowFn().getSideInputWindow(window);

    Map<BoundedWindow, ViewT> sideInputs =
        runtimeContext.getBroadcastVariableWithInitializer(
            view.getTagInternal().getId(), new SideInputInitializer<>(view));
    ViewT result = sideInputs.get(sideInputWindow);
    if (result == null) {
      result = view.getViewFn().apply(Collections.<WindowedValue<?>>emptyList());
    }
    return result;
  }

  @Override
  public void output(OutputT value) {
    if (windowedValue != null) {
      outputWithTimestamp(value, windowedValue.getTimestamp());
    } else {
      outputWithTimestamp(value, null);
    }
  }

  @Override
  public void outputWithTimestamp(OutputT value, Instant timestamp) {
    if (windowedValue == null) {
      // we are in startBundle() or finishBundle()

      try {
        Collection windows = windowingStrategy.getWindowFn().assignWindows(
            new FlinkNoElementAssignContext(
                windowingStrategy.getWindowFn(),
                value,
                timestamp));

        collector.collect(
            WindowedValue.of(
                value,
                timestamp != null ? timestamp : new Instant(Long.MIN_VALUE),
                windows,
                PaneInfo.NO_FIRING));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      collector.collect(
          WindowedValue.of(
              value,
              timestamp,
              windowedValue.getWindows(),
              windowedValue.getPane()));
    }
  }

  protected void outputWithTimestampAndWindow(
      OutputT value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane) {
    collector.collect(
        WindowedValue.of(
            value, timestamp, windows, pane));
  }

  @Override
  public <T> void sideOutput(TupleTag<T> tag, T output) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
    sideOutput(tag, output);
  }

  @Override
  protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT>
  createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
    SerializableFnAggregatorWrapper<AggInputT, AggOutputT> wrapper =
        new SerializableFnAggregatorWrapper<>(combiner);
    Accumulator<?, ?> existingAccum =
        (Accumulator<AggInputT, Serializable>) runtimeContext.getAccumulator(name);
    if (existingAccum != null) {
      return wrapper;
    } else {
      runtimeContext.addAccumulator(name, wrapper);
    }
    return wrapper;
  }
}
