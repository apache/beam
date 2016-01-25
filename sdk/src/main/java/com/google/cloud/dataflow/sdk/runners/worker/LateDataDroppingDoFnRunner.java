/*
 * Copyright (C) 2016 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunnerBase;
import com.google.cloud.dataflow.sdk.util.DoFnRunners.OutputManager;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.WindowTracing;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

import org.joda.time.Instant;

import java.util.List;

/**
 * A customized {@link DoFnRunner} that handles late data dropping.
 *
 * <p>It expands windows before checking data lateness.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output value element type
 * @param <W> window type
 */
public class LateDataDroppingDoFnRunner<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFnRunnerBase<KeyedWorkItem<K, InputT>, KV<K, OutputT>> {
  private final WindowingStrategy<?, W> windowingStrategy;
  private final TimerInternals timerInternals;
  private final Aggregator<Long, Long> droppedDueToLateness;

  public LateDataDroppingDoFnRunner(
      PipelineOptions options,
      StreamingGroupAlsoByWindowsDoFn<K, InputT, OutputT, ?> doFn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<KV<K, OutputT>> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowingStrategy<?, W> windowingStrategy) {
    super(options, doFn, sideInputReader, outputManager,
        mainOutputTag, sideOutputTags, stepContext,
        addCounterMutator, windowingStrategy);
    this.windowingStrategy = windowingStrategy;
    this.timerInternals = stepContext.timerInternals();

    droppedDueToLateness = doFn.droppedDueToLateness;
  }

  @Override
  public void invokeProcessElement(WindowedValue<KeyedWorkItem<K, InputT>> elem) {
    final K key = elem.getValue().key();
    Iterable<Iterable<WindowedValue<InputT>>> elements = Iterables.transform(
        elem.getValue().elementsIterable(),
        new Function<WindowedValue<InputT>, Iterable<WindowedValue<InputT>>>() {
          @Override
          public Iterable<WindowedValue<InputT>> apply(final WindowedValue<InputT> input) {
            return Iterables.transform(
                input.getWindows(),
                new Function<BoundedWindow, WindowedValue<InputT>>() {
                  @Override
                  public WindowedValue<InputT> apply(BoundedWindow window) {
                    return WindowedValue.of(
                        input.getValue(), input.getTimestamp(), window, input.getPane());
                  }
                });
          }});

    Iterable<WindowedValue<InputT>> nonLateElements = Iterables.filter(
        Iterables.concat(elements),
        new Predicate<WindowedValue<InputT>>() {
          @Override
          public boolean apply(WindowedValue<InputT> input) {
            BoundedWindow window = Iterables.getOnlyElement(input.getWindows());
            if (canDropDueToExpiredWindow(window)) {
              // The element is too late for this window.
              droppedDueToLateness.addValue(1L);
              WindowTracing.debug(
                  "ReduceFnRunner.processElement: Dropping element at {} for key:{}; window:{} "
                  + "since too far behind inputWatermark:{}; outputWatermark:{}",
                  input.getTimestamp(), key, window, timerInternals.currentInputWatermarkTime(),
                  timerInternals.currentOutputWatermarkTime());
              return false;
            } else {
              return true;
            }
          }
        });
    KeyedWorkItem<K, InputT> keyedWorkItem = KeyedWorkItems.workItem(
        elem.getValue().key(), elem.getValue().timersIterable(), nonLateElements);
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(createProcessContext(elem.withValue(keyedWorkItem)));
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /** Is {@code window} expired w.r.t. the garbage collection watermark? */
  private boolean canDropDueToExpiredWindow(BoundedWindow window) {
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    return inputWM != null
        && window.maxTimestamp().plus(windowingStrategy.getAllowedLateness()).isBefore(inputWM);
  }
}
