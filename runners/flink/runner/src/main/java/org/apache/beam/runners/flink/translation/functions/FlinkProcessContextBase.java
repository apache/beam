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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.core.OldDoFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.WindowingInternals;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.joda.time.Instant;

/**
 * {@link OldDoFn.ProcessContext} for our Flink Wrappers.
 */
abstract class FlinkProcessContextBase<InputT, OutputT>
    extends OldDoFn<InputT, OutputT>.ProcessContext {

  private final PipelineOptions pipelineOptions;
  private final RuntimeContext runtimeContext;
  private final boolean requiresWindowAccess;
  protected final WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  protected WindowedValue<InputT> windowedValue;

  FlinkProcessContextBase(
      PipelineOptions pipelineOptions,
      RuntimeContext runtimeContext,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ? extends BoundedWindow>> sideInputs) {
    doFn.super();
    checkNotNull(pipelineOptions);
    checkNotNull(runtimeContext);
    checkNotNull(doFn);

    this.pipelineOptions = pipelineOptions;
    this.runtimeContext = runtimeContext;
    this.requiresWindowAccess = doFn instanceof OldDoFn.RequiresWindowAccess;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;

    super.setupDelegateAggregators();
  }

  public void setWindowedValue(WindowedValue<InputT> windowedValue) {
    this.windowedValue = windowedValue;
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
        outputWithTimestampAndWindow(value, timestamp, windows, pane);
      }

      @Override
      public <SideOutputT> void sideOutputWindowedValue(
          TupleTag<SideOutputT> tag,
          SideOutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        throw new UnsupportedOperationException();
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
      public <ViewT> ViewT sideInput(
          PCollectionView<ViewT> view,
          BoundedWindow sideInputWindow) {

        checkNotNull(view, "View passed to sideInput cannot be null");
        checkNotNull(
            sideInputs.get(view),
            "Side input for " + view + " not available.");

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
  public final void outputWithTimestamp(OutputT value, Instant timestamp) {
    if (windowedValue == null) {
      // we are in startBundle() or finishBundle()

      try {
        Collection windows = windowingStrategy.getWindowFn().assignWindows(
            new FlinkNoElementAssignContext(
                windowingStrategy.getWindowFn(),
                value,
                timestamp));

        outputWithTimestampAndWindow(
            value,
            timestamp != null ? timestamp : new Instant(Long.MIN_VALUE),
            windows,
            PaneInfo.NO_FIRING);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      outputWithTimestampAndWindow(
          value, timestamp, windowedValue.getWindows(), windowedValue.getPane());
    }
  }

  protected abstract void outputWithTimestampAndWindow(
      OutputT value,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane);

  @Override
  public abstract <T> void sideOutput(TupleTag<T> tag, T output);

  @Override
  public abstract <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp);

  @Override
  public <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT>
  createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
    @SuppressWarnings("unchecked")
    SerializableFnAggregatorWrapper<AggInputT, AggOutputT> result =
        (SerializableFnAggregatorWrapper<AggInputT, AggOutputT>)
            runtimeContext.getAccumulator(name);

    if (result == null) {
      result = new SerializableFnAggregatorWrapper<>(combiner);
      runtimeContext.addAccumulator(name, result);
    }
    return result;  }
}
