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

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresKeyedState;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Instant;

import java.util.Collection;

/**
 * A concrete implementation of {@link DoFn<I, O>.ProcessContext} used for running
 * a {@link DoFn} over a single element.
 *
 * @param <I> the type of the DoFn's (main) input elements
 * @param <O> the type of the DoFn's (main) output elements
 */
class DoFnProcessContext<I, O> extends DoFn<I, O>.ProcessContext {

  final DoFn<I, O> fn;
  final DoFnContext<I, O, ?> context;
  final WindowedValue<I> windowedValue;

  public DoFnProcessContext(DoFn<I, O> fn,
                            DoFnContext<I, O, ?> context,
                            WindowedValue<I> windowedValue) {
    fn.super();
    this.fn = fn;
    this.context = context;
    this.windowedValue = windowedValue;
  }

  @Override
  public PipelineOptions getPipelineOptions() {
    return context.getPipelineOptions();
  }

  @Override
  public I element() {
    return windowedValue.getValue();
  }

  @Override
  public KeyedState keyedState() {
    if (!(fn instanceof RequiresKeyedState)
        || (element() != null && !(element() instanceof KV))) {
      throw new UnsupportedOperationException(
          "Keyed state is only available in the context of a keyed DoFn marked as requiring state");
    }

    return context.stepContext;
  }

  @Override
  public <T> T sideInput(PCollectionView<T, ?> view) {
    return context.sideInput(view);
  }

  @Override
  public void output(O output) {
    context.outputWindowedValue(output, windowedValue.getTimestamp(), windowedValue.getWindows());
  }

  @Override
  public void outputWithTimestamp(O output, Instant timestamp) {
    checkTimestamp(timestamp);
    context.outputWindowedValue(output, timestamp, windowedValue.getWindows());
  }

  void outputWindowedValue(
      O output,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows) {
    context.outputWindowedValue(output, timestamp, windows);
  }

  @Override
  public <T> void sideOutput(TupleTag<T> tag, T output) {
    context.sideOutputWindowedValue(tag,
                                    output,
                                    windowedValue.getTimestamp(),
                                    windowedValue.getWindows());
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
    checkTimestamp(timestamp);
    context.sideOutputWindowedValue(tag, output, timestamp, windowedValue.getWindows());
  }

  @Override
  public <AI, AA, AO> Aggregator<AI> createAggregator(
      String name, Combine.CombineFn<? super AI, AA, AO> combiner) {
    return context.createAggregator(name, combiner);
  }

  @Override
  public <AI, AO> Aggregator<AI> createAggregator(
      String name, SerializableFunction<Iterable<AI>, AO> combiner) {
    return context.createAggregator(name, combiner);
  }

  @Override
  public Instant timestamp() {
    return windowedValue.getTimestamp();
  }

  @Override
  public Collection<? extends BoundedWindow> windows() {
    return windowedValue.getWindows();
  }

  private void checkTimestamp(Instant timestamp) {
    Preconditions.checkArgument(
        !timestamp.isBefore(windowedValue.getTimestamp().minus(fn.getAllowedTimestampSkew())));
  }
}
