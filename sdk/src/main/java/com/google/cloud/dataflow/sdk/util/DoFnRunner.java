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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs a DoFn by constructing the appropriate contexts and passing them in.
 *
 * @param <I> the type of the DoFn's (main) input elements
 * @param <O> the type of the DoFn's (main) output elements
 * @param <R> the type of object which receives outputs
 */
public class DoFnRunner<I, O, R> {

  /** Information about how to create output receivers and output to them. */
  public interface OutputManager<R> {

    /** Returns the receiver to use for a given tag. */
    public R initialize(TupleTag<?> tag);

    /** Outputs a single element to the provided receiver. */
    public void output(R receiver, WindowedValue<?> output);

  }

  /** The DoFn being run. */
  public final DoFn<I, O> fn;

  /** The context used for running the DoFn. */
  public final DoFnContext<I, O, R> context;

  private DoFnRunner(PipelineOptions options,
                     DoFn<I, O> fn,
                     PTuple sideInputs,
                     OutputManager<R> outputManager,
                     TupleTag<O> mainOutputTag,
                     List<TupleTag<?>> sideOutputTags,
                     StepContext stepContext,
                     CounterSet.AddCounterMutator addCounterMutator,
                     WindowFn windowFn) {
    this.fn = fn;
    this.context = new DoFnContext<>(options, fn, sideInputs, outputManager,
                                     mainOutputTag, sideOutputTags, stepContext,
                                     addCounterMutator, windowFn);
  }

  public static <I, O, R> DoFnRunner<I, O, R> create(
      PipelineOptions options,
      DoFn<I, O> fn,
      PTuple sideInputs,
      OutputManager<R> outputManager,
      TupleTag<O> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowFn windowFn) {
    return new DoFnRunner<>(
        options, fn, sideInputs, outputManager,
        mainOutputTag, sideOutputTags, stepContext, addCounterMutator, windowFn);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <I, O> DoFnRunner<I, O, List> createWithListOutputs(
      PipelineOptions options,
      DoFn<I, O> fn,
      PTuple sideInputs,
      TupleTag<O> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator,
      WindowFn windowFn) {
    return create(
        options, fn, sideInputs,
        new OutputManager<List>() {
          @Override
          public List initialize(TupleTag<?> tag) {
            return new ArrayList<>();
          }
          @Override
          public void output(List list, WindowedValue<?> output) {
            list.add(output);
          }
        },
        mainOutputTag, sideOutputTags, stepContext, addCounterMutator, windowFn);
  }

  /** Calls {@link DoFn#startBundle}. */
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.startBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /**
   * Calls {@link DoFn#processElement} with a ProcessContext containing
   * the current element.
   */
  public void processElement(WindowedValue<I> elem) {
    DoFnProcessContext<I, O> processContext = new DoFnProcessContext<I, O>(fn, context, elem);

    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(processContext);
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /** Calls {@link DoFn#finishBundle}. */
  public void finishBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.finishBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  /** Returns the receiver who gets outputs with the provided tag. */
  public R getReceiver(TupleTag<?> tag) {
    return context.getReceiver(tag);
  }
}
