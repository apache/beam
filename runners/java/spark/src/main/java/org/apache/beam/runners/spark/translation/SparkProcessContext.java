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

package org.apache.beam.runners.spark.translation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.state.*;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkProcessContext<I, O, V> extends DoFn<I, O>.ProcessContext {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProcessContext.class);

  private final DoFn<I, O> fn;
  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  protected WindowedValue<I> windowedValue;

  SparkProcessContext(DoFn<I, O> fn,
      SparkRuntimeContext runtime,
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    fn.super();
    this.fn = fn;
    this.mRuntimeContext = runtime;
    this.mSideInputs = sideInputs;
  }

  void setup() {
    setupDelegateAggregators();
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
  public abstract void output(O output);

  public abstract void output(WindowedValue<O> output);

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
    return windowedValue.getValue();
  }

  @Override
  public void outputWithTimestamp(O output, Instant timestamp) {
    output(WindowedValue.of(output, timestamp,
        windowedValue.getWindows(), windowedValue.getPane()));
  }

  @Override
  public Instant timestamp() {
    return windowedValue.getTimestamp();
  }

  @Override
  public BoundedWindow window() {
    if (!(fn instanceof DoFn.RequiresWindowAccess)) {
      throw new UnsupportedOperationException(
          "window() is only available in the context of a DoFn marked as RequiresWindow.");
    }
    return Iterables.getOnlyElement(windowedValue.getWindows());
  }

  @Override
  public PaneInfo pane() {
    return windowedValue.getPane();
  }

  @Override
  public WindowingInternals<I, O> windowingInternals() {
    return new WindowingInternals<I, O>() {

      @Override
      public Collection<? extends BoundedWindow> windows() {
        return windowedValue.getWindows();
      }

      @Override
      public void outputWindowedValue(O output, Instant timestamp, Collection<?
          extends BoundedWindow> windows, PaneInfo paneInfo) {
        output(WindowedValue.of(output, timestamp, windows, paneInfo));
      }

      @Override
      public StateInternals stateInternals() {
        //TODO: implement state internals.
        // This is a temporary placeholder to get the TfIdfTest
        // working for the initial Beam code drop.
        return InMemoryStateInternals.forKey("DUMMY");
      }

      @Override
      public TimerInternals timerInternals() {
        throw new UnsupportedOperationException(
            "WindowingInternals#timerInternals() is not yet supported.");
      }

      @Override
      public PaneInfo pane() {
        return windowedValue.getPane();
      }

      @Override
      public <T> void writePCollectionViewData(TupleTag<?> tag,
          Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
        throw new UnsupportedOperationException(
            "WindowingInternals#writePCollectionViewData() is not yet supported.");
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
        throw new UnsupportedOperationException(
            "WindowingInternals#sideInput() is not yet supported.");
      }
    };
  }

  protected abstract void clearOutput();
  protected abstract Iterator<V> getOutputIterator();

  protected Iterable<V> getOutputIterable(final Iterator<WindowedValue<I>> iter,
      final DoFn<I, O> doFn) {
    return new Iterable<V>() {
      @Override
      public Iterator<V> iterator() {
        return new ProcCtxtIterator(iter, doFn);
      }
    };
  }

  private class ProcCtxtIterator extends AbstractIterator<V> {

    private final Iterator<WindowedValue<I>> inputIterator;
    private final DoFn<I, O> doFn;
    private Iterator<V> outputIterator;
    private boolean calledFinish;

    ProcCtxtIterator(Iterator<WindowedValue<I>> iterator, DoFn<I, O> doFn) {
      this.inputIterator = iterator;
      this.doFn = doFn;
      this.outputIterator = getOutputIterator();
    }

    @Override
    protected V computeNext() {
      // Process each element from the (input) iterator, which produces, zero, one or more
      // output elements (of type V) in the output iterator. Note that the output
      // collection (and iterator) is reset between each call to processElement, so the
      // collection only holds the output values for each call to processElement, rather
      // than for the whole partition (which would use too much memory).
      while (true) {
        if (outputIterator.hasNext()) {
          return outputIterator.next();
        } else if (inputIterator.hasNext()) {
          clearOutput();
          windowedValue = inputIterator.next();
          try {
            doFn.processElement(SparkProcessContext.this);
          } catch (Exception e) {
            throw new SparkProcessException(e);
          }
          outputIterator = getOutputIterator();
        } else {
          // no more input to consume, but finishBundle can produce more output
          if (!calledFinish) {
            clearOutput();
            try {
              calledFinish = true;
              doFn.finishBundle(SparkProcessContext.this);
            } catch (Exception e) {
              throw new SparkProcessException(e);
            }
            outputIterator = getOutputIterator();
            continue; // try to consume outputIterator from start of loop
          }
          return endOfData();
        }
      }
    }
  }

  public static class SparkProcessException extends RuntimeException {
    SparkProcessException(Throwable t) {
      super(t);
    }
  }

}
