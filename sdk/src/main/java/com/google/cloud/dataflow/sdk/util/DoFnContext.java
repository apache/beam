/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn.AssignContext;
import com.google.cloud.dataflow.sdk.util.DoFnRunner.OutputManager;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Instant;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A concrete implementation of {@link DoFn<I, O>.Context} used for running
 * a {@link DoFn}.
 *
 * @param <I> the type of the DoFn's (main) input elements
 * @param <O> the type of the DoFn's (main) output elements
 * @param <R> the type of object which receives outputs
 */
class DoFnContext<I, O, R> extends DoFn<I, O>.Context {
  private static final int MAX_SIDE_OUTPUTS = 1000;

  final PipelineOptions options;
  final DoFn<I, O> fn;
  final PTuple sideInputs;
  final OutputManager<R> outputManager;
  final Map<TupleTag<?>, R> outputMap;
  final TupleTag<O> mainOutputTag;
  final StepContext stepContext;
  final CounterSet.AddCounterMutator addCounterMutator;
  final WindowFn windowFn;

  public DoFnContext(PipelineOptions options,
                     DoFn<I, O> fn,
                     PTuple sideInputs,
                     OutputManager<R> outputManager,
                     TupleTag<O> mainOutputTag,
                     List<TupleTag<?>> sideOutputTags,
                     StepContext stepContext,
                     CounterSet.AddCounterMutator addCounterMutator,
                     WindowFn windowFn) {
    fn.super();
    this.options = options;
    this.fn = fn;
    this.sideInputs = sideInputs;
    this.outputManager = outputManager;
    this.mainOutputTag = mainOutputTag;
    this.outputMap = new HashMap<>();
    outputMap.put(mainOutputTag, outputManager.initialize(mainOutputTag));
    for (TupleTag<?> sideOutputTag : sideOutputTags) {
      outputMap.put(sideOutputTag, outputManager.initialize(sideOutputTag));
    }
    this.stepContext = stepContext;
    this.addCounterMutator = addCounterMutator;
    this.windowFn = windowFn;
  }

  public R getReceiver(TupleTag<?> tag) {
    R receiver = outputMap.get(tag);
    if (receiver == null) {
      throw new IllegalArgumentException(
          "calling getReceiver() with unknown tag " + tag);
    }
    return receiver;
  }

  //////////////////////////////////////////////////////////////////////////////

  @Override
  public PipelineOptions getPipelineOptions() {
    return options;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T sideInput(PCollectionView<T, ?> view) {
    TupleTag<?> tag = view.getTagInternal();
    if (!sideInputs.has(tag)) {
      throw new IllegalArgumentException(
          "calling sideInput() with unknown view; " +
          "did you forget to pass the view in " +
          "ParDo.withSideInputs()?");
    }
    return view.fromIterableInternal((Iterable<WindowedValue<?>>) sideInputs.get(tag));
  }

  <T> WindowedValue<T> makeWindowedValue(
      T output, Instant timestamp, Collection<? extends BoundedWindow> windows) {
    final Instant inputTimestamp = timestamp;

    if (timestamp == null) {
      timestamp = new Instant(Long.MIN_VALUE);
    }

    if (windows == null) {
      try {
        windows = windowFn.assignWindows(windowFn.new AssignContext() {
            @Override
            public Object element() {
              throw new UnsupportedOperationException(
                  "WindowFn attemped to access input element when none was available");
            }

            @Override
            public Instant timestamp() {
              if (inputTimestamp == null) {
                throw new UnsupportedOperationException(
                    "WindowFn attemped to access input timestamp when none was available");
              }
              return inputTimestamp;
            }

            @Override
            public Collection<? extends BoundedWindow> windows() {
              throw new UnsupportedOperationException(
                  "WindowFn attemped to access input windows when none were available");
            }
          });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return WindowedValue.of(output, timestamp, windows);
  }

  void outputWindowedValue(
      O output,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows) {
    WindowedValue<O> windowedElem = makeWindowedValue(output, timestamp, windows);
    outputManager.output(outputMap.get(mainOutputTag), windowedElem);
    if (stepContext != null) {
      stepContext.noteOutput(windowedElem);
    }
  }

  protected <T> void sideOutputWindowedValue(TupleTag<T> tag,
                                             T output,
                                             Instant timestamp,
                                             Collection<? extends BoundedWindow> windows) {
    R receiver = outputMap.get(tag);
    if (receiver == null) {
      // This tag wasn't declared nor was it seen before during this execution.
      // Thus, this must be a new, undeclared and unconsumed output.

      // To prevent likely user errors, enforce the limit on the number of side
      // outputs.
      if (outputMap.size() >= MAX_SIDE_OUTPUTS) {
        throw new IllegalArgumentException(
            "the number of side outputs has exceeded a limit of "
            + MAX_SIDE_OUTPUTS);
      }

      // Register the new TupleTag with outputManager and add an entry for it in
      // the outputMap.
      receiver = outputManager.initialize(tag);
      outputMap.put(tag, receiver);
    }

    WindowedValue<T> windowedElem = makeWindowedValue(output, timestamp, windows);
    outputManager.output(receiver, windowedElem);
    if (stepContext != null) {
      stepContext.noteSideOutput(tag, windowedElem);
    }
  }

  // Following implementations of output, outputWithTimestamp, and sideOutput
  // are only accessible in DoFn.startBundle and DoFn.finishBundle, and will be shadowed by
  // ProcessContext's versions in DoFn.processElement.
  @Override
  public void output(O output) {
    outputWindowedValue(output, null, null);
  }

  @Override
  public void outputWithTimestamp(O output, Instant timestamp) {
    outputWindowedValue(output, timestamp, null);
  }

  @Override
  public <T> void sideOutput(TupleTag<T> tag, T output) {
    sideOutputWindowedValue(tag, output, null, null);
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
    sideOutputWindowedValue(tag, output, timestamp, null);
  }

  private String generateInternalAggregatorName(String userName) {
    return "user-" + stepContext.getStepName() + "-" + userName;
  }

  @Override
  public <AI, AA, AO> Aggregator<AI> createAggregator(
      String name, Combine.CombineFn<? super AI, AA, AO> combiner) {
    return new AggregatorImpl<>(generateInternalAggregatorName(name), combiner, addCounterMutator);
  }

  @Override
  public <AI, AO> Aggregator<AI> createAggregator(
      String name, SerializableFunction<Iterable<AI>, AO> combiner) {
    return new AggregatorImpl<AI, Iterable<AI>, AO>(
        generateInternalAggregatorName(name), combiner, addCounterMutator);
  }
}
