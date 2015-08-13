/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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
import java.util.Map;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.AbstractIterator;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkProcessContext<I, O, V> extends DoFn<I, O>.ProcessContext {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProcessContext.class);

  private static final Collection<? extends BoundedWindow> GLOBAL_WINDOWS =
      Collections.singletonList(GlobalWindow.INSTANCE);

  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  protected I element;

  SparkProcessContext(DoFn<I, O> fn,
      SparkRuntimeContext runtime,
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs) {
    fn.super();
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
  public PaneInfo pane() {
    return PaneInfo.NO_FIRING;
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
          extends BoundedWindow> windows, PaneInfo paneInfo) {
        output(output);
      }

      @Override
      public StateInternals stateInternals() {
        throw new UnsupportedOperationException(
            "WindowingInternals#stateInternals() is not yet supported.");
      }

      @Override
      public TimerInternals timerInternals() {
        throw new UnsupportedOperationException(
            "WindowingInternals#timerInternals() is not yet supported.");
      }

      @Override
      public PaneInfo pane() {
        return PaneInfo.NO_FIRING;
      }

      @Override
      public <T> void writePCollectionViewData(TupleTag<?> tag,
          Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
        throw new UnsupportedOperationException(
            "WindowingInternals#writePCollectionViewData() is not yet supported.");
      }
    };
  }

  protected abstract void clearOutput();
  protected abstract Iterator<V> getOutputIterator();

  protected Iterable<V> getOutputIterable(final Iterator<I> iter, final DoFn<I, O> doFn) {
    return new Iterable<V>() {
      @Override
      public Iterator<V> iterator() {
        return new ProcCtxtIterator(iter, doFn);
      }
    };
  }

  private class ProcCtxtIterator extends AbstractIterator<V> {

    private final Iterator<I> inputIterator;
    private final DoFn<I, O> doFn;
    private Iterator<V> outputIterator;
    private boolean calledFinish = false;

    ProcCtxtIterator(Iterator<I> iterator, DoFn<I, O> doFn) {
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
          element = inputIterator.next();
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

  static class SparkProcessException extends RuntimeException {
    public SparkProcessException(Throwable t) {
      super(t);
    }
  }

}
