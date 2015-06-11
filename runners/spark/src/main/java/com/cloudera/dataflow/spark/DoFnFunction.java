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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.TimerManager;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
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

  private static final Collection<? extends BoundedWindow> GLOBAL_WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

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
    ctxt.setup();
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

    void setup() {
      super.setupDelegateAggregators();
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
    public <AI, AO> Aggregator<AI, AO> createAggregatorInternal(
        String named,
        Combine.CombineFn<AI, ?, AO> combineFn) {
      return mRuntimeContext.createAggregator(named, combineFn);
    }

    @Override
    public I element() {
      return element;
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
      return GlobalWindow.INSTANCE;
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
      return new WindowingInternals<I, O>() {

        @Override
        public Collection<? extends BoundedWindow> windows() {
          return GLOBAL_WINDOWS;
        }

        @Override
        public void outputWindowedValue(O output, Instant timestamp, Collection<?
            extends BoundedWindow> windows) {
          output(output);
        }

        @Override
        public KeyedState keyedState() {
          throw new UnsupportedOperationException(
              "WindowingInternals#keyedState() is not yet supported.");

        }

        @Override
        public <T> void store(CodedTupleTag<T> tag, T value, Instant timestamp)
            throws IOException {
          throw new UnsupportedOperationException(
              "WindowingInternals#store() is not yet supported.");
        }

        @Override
        public <T> void writeToTagList(CodedTupleTag<T> tag, T value) throws IOException {
          throw new UnsupportedOperationException(
              "WindowingInternals#writeToTagList() is not yet supported.");
        }

        @Override
        public <T> void deleteTagList(CodedTupleTag<T> tag) {
          throw new UnsupportedOperationException(
              "WindowingInternals#deleteTagList() is not yet supported.");
        }

        @Override
        public <T> Iterable<T> readTagList(CodedTupleTag<T> tag) throws IOException {
          throw new UnsupportedOperationException(
              "WindowingInternals#readTagList() is not yet supported.");
        }

        @Override
        public <T> Map<CodedTupleTag<T>, Iterable<T>> readTagList(List<CodedTupleTag<T>> tags)
            throws IOException {
          throw new UnsupportedOperationException(
              "WindowingInternals#readTagList() is not yet supported.");
        }

        @Override
        public TimerManager getTimerManager() {
          throw new UnsupportedOperationException(
              "WindowingInternals#getTimerManager() is not yet supported.");
        }

        @Override
        public <T> void writePCollectionViewData(TupleTag<?> tag,
            Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
          throw new UnsupportedOperationException(
              "WindowingInternals#writePCollectionViewData() is not yet supported.");
        }
      };
    }

  }
}
