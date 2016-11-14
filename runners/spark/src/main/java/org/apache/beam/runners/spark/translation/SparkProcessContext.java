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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.runners.spark.util.SparkSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.OldDoFn.RequiresWindowAccess;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.KV;
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
  private final SideInputReader sideInputReader;
  private final WindowFn<Object, ?> windowFn;

  WindowedValue<InputT> windowedValue;

  SparkProcessContext(OldDoFn<InputT, OutputT> fn,
                      SparkRuntimeContext runtime,
                      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs,
                      WindowFn<Object, ?> windowFn) {
    fn.super();
    this.fn = fn;
    this.mRuntimeContext = runtime;
    this.sideInputReader = new SparkSideInputReader(sideInputs);
    this.windowFn = windowFn;
  }

  void setup() {
    setupDelegateAggregators();
  }

  Iterable<ValueT> callWithCtxt(Iterator<WindowedValue<InputT>> iter) throws Exception{
    this.setup();
    // skip if bundle is empty.
    if (!iter.hasNext()) {
      return Lists.newArrayList();
    }
    try {
      fn.setup();
      fn.startBundle(this);
      return this.getOutputIterable(iter, fn);
    } catch (Exception e) {
      try {
        // this teardown handles exceptions encountered in setup() and startBundle(). teardown
        // after execution or due to exceptions in process element is called in the iterator
        // produced by ctxt.getOutputIterable returned from this method.
        fn.teardown();
      } catch (Exception teardownException) {
        LOG.error(
            "Suppressing exception while tearing down Function {}", fn, teardownException);
        e.addSuppressed(teardownException);
      }
      throw wrapUserCodeException(e);
    }
  }

  @Override
  public PipelineOptions getPipelineOptions() {
    return mRuntimeContext.getPipelineOptions();
  }

  @Override
  public <T> T sideInput(PCollectionView<T> view) {
    //validate element window.
    final Collection<? extends BoundedWindow> elementWindows = windowedValue.getWindows();
    checkState(elementWindows.size() == 1, "sideInput can only be called when the main "
        + "input element is in exactly one window");
    return sideInputReader.get(view, elementWindows.iterator().next());
  }

  @Override
  public <AggregatorInputT, AggregatorOutputT>
  Aggregator<AggregatorInputT, AggregatorOutputT> createAggregatorInternal(
      String named,
      Combine.CombineFn<AggregatorInputT, ?, AggregatorOutputT> combineFn) {
    return mRuntimeContext.createAggregator(getAccumulator(), named, combineFn);
  }

  public abstract Accumulator<NamedAggregators> getAccumulator();

  @Override
  public InputT element() {
    return windowedValue.getValue();
  }

  @Override
  public void output(OutputT output) {
    outputWithTimestamp(output, windowedValue != null ? windowedValue.getTimestamp() : null);
  }

  @Override
  public void outputWithTimestamp(OutputT output, Instant timestamp) {
    if (windowedValue == null) {
      // this is start/finishBundle.
      outputWindowedValue(noElementWindowedValue(output, timestamp, windowFn));
    } else {
      outputWindowedValue(WindowedValue.of(output, timestamp, windowedValue.getWindows(),
          windowedValue.getPane()));
    }
  }

  @Override
  public <T> void sideOutput(TupleTag<T> tag, T output) {
    sideOutputWithTimestamp(
        tag, output, windowedValue != null ? windowedValue.getTimestamp() : null);
  }

  @Override
  public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
    if (windowedValue == null) {
      // this is start/finishBundle.
      sideOutputWindowedValue(tag, noElementWindowedValue(output, timestamp, windowFn));
    } else {
      sideOutputWindowedValue(tag, WindowedValue.of(output, timestamp, windowedValue.getWindows(),
          windowedValue.getPane()));
    }
  }

  protected abstract void outputWindowedValue(WindowedValue<OutputT> output);

  protected abstract <T> void sideOutputWindowedValue(TupleTag<T> tag, WindowedValue<T> output);

  static <T, W extends BoundedWindow> WindowedValue<T> noElementWindowedValue(
      final T output, final Instant timestamp, WindowFn<Object, W> windowFn) {
    WindowFn<Object, W>.AssignContext assignContext =
        windowFn.new AssignContext() {

          @Override
          public Object element() {
            return output;
          }

          @Override
          public Instant timestamp() {
            if (timestamp != null) {
              return timestamp;
            }
            throw new UnsupportedOperationException(
                "outputWithTimestamp was called with " + "null timestamp.");
          }

          @Override
          public BoundedWindow window() {
            throw new UnsupportedOperationException(
                "Window not available for " + "start/finishBundle output.");
          }
        };
    try {
      @SuppressWarnings("unchecked")
      Collection<? extends BoundedWindow> windows = windowFn.assignWindows(assignContext);
      Instant outputTimestamp = timestamp != null ? timestamp : BoundedWindow.TIMESTAMP_MIN_VALUE;
      return WindowedValue.of(output, outputTimestamp, windows, PaneInfo.NO_FIRING);
    } catch (Exception e) {
      throw new RuntimeException("Failed to assign windows at start/finishBundle.", e);
    }
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
      public void outputWindowedValue(
          OutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo paneInfo) {
        SparkProcessContext.this.outputWindowedValue(
            WindowedValue.of(output, timestamp, windows, paneInfo));
      }

      @Override
      public <SideOutputT> void sideOutputWindowedValue(
          TupleTag<SideOutputT> tag,
          SideOutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo paneInfo) {
        SparkProcessContext.this.sideOutputWindowedValue(
            tag, WindowedValue.of(output, timestamp, windows, paneInfo));
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
      public <T> void writePCollectionViewData(
          TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
        throw new UnsupportedOperationException(
            "WindowingInternals#writePCollectionViewData() is not yet supported.");
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view, BoundedWindow sideInputWindow) {
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
          // grab the next element and process it.
          windowedValue = inputIterator.next();
          if (windowedValue.getWindows().size() <= 1
              || (!RequiresWindowAccess.class.isAssignableFrom(doFn.getClass())
                  && sideInputReader.isEmpty())) {
            // if there's no reason to explode, process compacted.
            invokeProcessElement();
          } else {
            // explode and process the element in each of it's assigned windows.
            for (WindowedValue<InputT> wv: windowedValue.explodeWindows()) {
              windowedValue = wv;
              invokeProcessElement();
            }
          }
          outputIterator = getOutputIterator();
        } else {
          // no more input to consume, but finishBundle can produce more output
          if (!calledFinish) {
            windowedValue = null; // clear the last element processed
            clearOutput();
            try {
              calledFinish = true;
              doFn.finishBundle(SparkProcessContext.this);
            } catch (Exception e) {
              handleProcessingException(e);
              throw wrapUserCodeException(e);
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

    private void invokeProcessElement() {
      try {
        doFn.processElement(SparkProcessContext.this);
      } catch (Exception e) {
        handleProcessingException(e);
        throw wrapUserCodeException(e);
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


  private RuntimeException wrapUserCodeException(Throwable t) {
    throw UserCodeException.wrapIf(!isSystemDoFn(), t);
  }

  private boolean isSystemDoFn() {
    return fn.getClass().isAnnotationPresent(SystemDoFnInternal.class);
  }

}
