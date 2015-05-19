/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import java.util.Iterator;
import java.util.Map;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.joda.time.Instant;
import scala.Tuple2;

/**
 * DoFunctions ignore side outputs. MultiDoFunctions deal with side outputs by enriching the
 * underlying data with multiple TupleTags.
 *
 * @param <I> Input type for DoFunction.
 * @param <O> Output type for DoFunction.
 */
class MultiDoFnFunction<I, O> implements PairFlatMapFunction<Iterator<I>, TupleTag<?>, Object> {
  // TODO: I think implementing decoding logic will allow us to do away with having two types of
  // DoFunctions. Josh originally made these two classes in order to help ease the typing of
  // results. Correctly using coders should just fix this.

  private final DoFn<I, O> mFunction;
  private final SparkRuntimeContext mRuntimeContext;
  private final TupleTag<O> mMainOutputTag;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  MultiDoFnFunction(
      DoFn<I, O> fn,
      SparkRuntimeContext runtimeContext,
      TupleTag<O> mainOutputTag,
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this.mFunction = fn;
    this.mRuntimeContext = runtimeContext;
    this.mMainOutputTag = mainOutputTag;
    this.mSideInputs = sideInputs;
  }

  @Override
  public Iterable<Tuple2<TupleTag<?>, Object>> call(Iterator<I> iter) throws Exception {
    ProcCtxt ctxt = new ProcCtxt(mFunction);
    mFunction.startBundle(ctxt);
    while (iter.hasNext()) {
      ctxt.element = iter.next();
      mFunction.processElement(ctxt);
    }
    mFunction.finishBundle(ctxt);
    return Iterables.transform(ctxt.outputs.entries(),
        new Function<Map.Entry<TupleTag<?>, Object>, Tuple2<TupleTag<?>, Object>>() {
          @Override
          public Tuple2<TupleTag<?>, Object> apply(Map.Entry<TupleTag<?>, Object> input) {
            return new Tuple2<TupleTag<?>, Object>(input.getKey(), input.getValue());
          }
        });
  }

  private class ProcCtxt extends DoFn<I, O>.ProcessContext {

    private final Multimap<TupleTag<?>, Object> outputs = LinkedListMultimap.create();
    private I element;

    ProcCtxt(DoFn<I, O> fn) {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return mRuntimeContext.getPipelineOptions();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      BroadcastHelper<?> broadcastHelper = mSideInputs.get(view.getTagInternal());
      @SuppressWarnings("unchecked")
      Iterable<WindowedValue<?>> contents =
          (Iterable<WindowedValue<?>>) broadcastHelper.getValue();
      return view.fromIterableInternal(contents);
    }

    @Override
    public synchronized void output(O o) {
      outputs.put(mMainOutputTag, o);
    }

    @Override
    public synchronized <T> void sideOutput(TupleTag<T> tag, T t) {
      outputs.put(tag, t);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tupleTag, T t, Instant instant) {
      outputs.put(tupleTag, t);
    }

    @Override
    public <AI, AA, AO> Aggregator<AI> createAggregator(
        String named,
        Combine.CombineFn<? super AI, AA, AO> combineFn) {
      return mRuntimeContext.createAggregator(named, combineFn);
    }

    @Override
    public <AI, AO> Aggregator<AI> createAggregator(
        String named,
        SerializableFunction<Iterable<AI>, AO> sfunc) {
      return mRuntimeContext.createAggregator(named, sfunc);
    }

    @Override
    public I element() {
      return element;
    }

    @Override
    public DoFn.KeyedState keyedState() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {
      output(output);
    }

    @Override
    public Instant timestamp() {
      return Instant.now();
    }

    @Override
    public BoundedWindow window() {
      return null;
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
      return null;
    }
  }
}
