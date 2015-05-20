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
import java.util.LinkedList;
import java.util.List;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow's Do functions correspond to Spark's FlatMap functions.
 *
 * @param <I> Input element type.
 * @param <O> Output element type.
 */
class DoFnFunction<I, O> implements FlatMapFunction<Iterator<I>, O> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnFunction.class);

  private final DoFn<I, O> mFunction;
  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  /**
   * @param fn         DoFunction to be wrapped.
   * @param runtime    Runtime to apply function in.
   * @param sideInputs Side inputs used in DoFunction.
   */
  DoFnFunction(DoFn<I, O> fn,
               SparkRuntimeContext runtime,
               Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    this.mFunction = fn;
    this.mRuntimeContext = runtime;
    this.mSideInputs = sideInputs;
  }


  @Override
  public Iterable<O> call(Iterator<I> iter) throws Exception {
    ProcCtxt ctxt = new ProcCtxt(mFunction);
    //setup
    mFunction.startBundle(ctxt);
    //operation
    while (iter.hasNext()) {
      ctxt.element = iter.next();
      mFunction.processElement(ctxt);
    }
    //cleanup
    mFunction.finishBundle(ctxt);
    return ctxt.outputs;
  }

  private class ProcCtxt extends DoFn<I, O>.ProcessContext {

    private final List<O> outputs = new LinkedList<>();
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
      @SuppressWarnings("unchecked")
      BroadcastHelper<Iterable<WindowedValue<?>>> broadcastHelper =
          (BroadcastHelper<Iterable<WindowedValue<?>>>) mSideInputs.get(view.getTagInternal());
      Iterable<WindowedValue<?>> contents = broadcastHelper.getValue();
      return view.fromIterableInternal(contents);
    }

    @Override
    public synchronized void output(O o) {
      outputs.add(o);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tupleTag, T t) {
      String message = "sideOutput is an unsupported operation for doFunctions, use a " +
          "MultiDoFunction instead.";
      LOG.warn(message);
      throw new UnsupportedOperationException(message);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tupleTag, T t, Instant instant) {
      String message =
          "sideOutputWithTimestamp is an unsupported operation for doFunctions, use a " +
          "MultiDoFunction instead.";
      LOG.warn(message);
      throw new UnsupportedOperationException(message);
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
