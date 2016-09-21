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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark runner process context.
 */
public abstract class SparkProcessContext<InputT, OutputT, ValueT>
    extends OldDoFn<InputT, OutputT>.ProcessContext {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProcessContext.class);

  private final OldDoFn<InputT, OutputT> fn;
  private final SparkRuntimeContext mRuntimeContext;
  private final Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs;

  protected WindowedValue<InputT> windowedValue;

  SparkProcessContext(OldDoFn<InputT, OutputT> fn,
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
    return view.getViewFn().apply(contents);
  }

  @Override
  public abstract void output(OutputT output);

  public abstract void output(WindowedValue<OutputT> output);

  @Override
  public <T> void sideOutput(TupleTag<T> tupleTag, T t) {
    String message = "sideOutput is an unsupported operation for doFunctions, use a "
        + "MultiDoFunction instead.";
    LOG.warn(message);
    throw new UnsupportedOperationException(message);
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tupleTag, T t, Instant instant) {
    String message =
        "sideOutputWithTimestamp is an unsupported operation for doFunctions, use a "
            + "MultiDoFunction instead.";
    LOG.warn(message);
    throw new UnsupportedOperationException(message);
  }

  @Override
  public <AggregatprInputT, AggregatorOutputT>
  Aggregator<AggregatprInputT, AggregatorOutputT> createAggregatorInternal(
          String named,
          Combine.CombineFn<AggregatprInputT, ?, AggregatorOutputT> combineFn) {
    return mRuntimeContext.createAggregator(getAccumulator(), named, combineFn);
  }

  public abstract Accumulator<NamedAggregators> getAccumulator();

  @Override
  public InputT element() {
    return windowedValue.getValue();
  }

  @Override
  public void outputWithTimestamp(OutputT output, Instant timestamp) {
    output(WindowedValue.of(output, timestamp,
        windowedValue.getWindows(), windowedValue.getPane()));
  }

  @Override
  public Instant timestamp() {
    return windowedValue.getTimestamp();
  }

  @Override
  public BoundedWindow window() {
    if (!(fn instanceof OldDoFn.RequiresWindowAccess)) {
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
      public Collection<? extends BoundedWindow> windows() {
        return windowedValue.getWindows();
      }

      @Override
      public void outputWindowedValue(OutputT output, Instant timestamp, Collection<?
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
  protected abstract Iterator<ValueT> getOutputIterator();

  protected Iterable<ValueT> getOutputIterable(final Iterator<WindowedValue<InputT>> iter,
                                               final OldDoFn<InputT, OutputT> doFn) {
    return new Iterable<ValueT>() {
      @Override
      public Iterator<ValueT> iterator() {
        return new ProcCtxtIterator(iter, doFn);
      }
    };
  }

  private class ProcCtxtIterator extends AbstractIterator<ValueT> {

    private final Iterator<WindowedValue<InputT>> inputIterator;
    private final OldDoFn<InputT, OutputT> doFn;
    private Iterator<ValueT> outputIterator;
    private boolean calledFinish;

    ProcCtxtIterator(Iterator<WindowedValue<InputT>> iterator, OldDoFn<InputT, OutputT> doFn) {
      this.inputIterator = iterator;
      this.doFn = doFn;
      this.outputIterator = getOutputIterator();
    }

    @Override
    protected ValueT computeNext() {
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
            handleProcessingException(e);
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
              handleProcessingException(e);
              throw new SparkProcessException(e);
            }
            outputIterator = getOutputIterator();
            continue; // try to consume outputIterator from start of loop
          }
          try {
            doFn.teardown();
          } catch (Exception e) {
            LOG.error(
                "Suppressing teardown exception that occurred after processing entire input", e);
          }
          return endOfData();
        }
      }
    }

    private void handleProcessingException(Exception e) {
      try {
        doFn.teardown();
      } catch (Exception e1) {
        LOG.error("Exception while cleaning up DoFn", e1);
        e.addSuppressed(e1);
      }
    }
  }

  /**
   * Spark process runtime exception.
   */
  public static class SparkProcessException extends RuntimeException {
    SparkProcessException(Throwable t) {
      super(t);
    }
  }

}
