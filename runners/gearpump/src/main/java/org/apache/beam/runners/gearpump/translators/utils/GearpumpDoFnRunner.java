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

package org.apache.beam.runners.gearpump.translators.utils;

import static org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.joda.time.Instant;


/**
 * a serializable {@link SimpleDoFnRunner}.
 */
public class GearpumpDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT>,
    Serializable {

  private final OldDoFn<InputT, OutputT> fn;
  private final transient PipelineOptions options;
  private final SideInputReader sideInputReader;
  private final DoFnRunners.OutputManager outputManager;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> sideOutputTags;
  private final ExecutionContext.StepContext stepContext;
  private final WindowFn<?, ?> windowFn;
  private DoFnContext<InputT, OutputT> context;

  public GearpumpDoFnRunner(
      GearpumpPipelineOptions pipelineOptions,
      OldDoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      DoFnRunners.OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      ExecutionContext.StepContext stepContext,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.fn = doFn;
    this.options = pipelineOptions;
    this.sideInputReader = sideInputReader;
    this.outputManager = outputManager;
    this.mainOutputTag = mainOutputTag;
    this.sideOutputTags = sideOutputTags;
    this.stepContext = stepContext;
    this.windowFn = windowingStrategy == null ? null : windowingStrategy.getWindowFn();
  }

  @Override
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      if (null == context) {
        this.context = new DoFnContext<>(
            options,
            fn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            stepContext,
            windowFn
        );
      }
      fn.startBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    if (elem.getWindows().size() <= 1
        || (!OldDoFn.RequiresWindowAccess.class.isAssignableFrom(fn.getClass())
        && context.sideInputReader.isEmpty())) {
      invokeProcessElement(elem);
    } else {
      // We could modify the windowed value (and the processContext) to
      // avoid repeated allocations, but this is more straightforward.
      for (BoundedWindow window : elem.getWindows()) {
        invokeProcessElement(WindowedValue.of(
            elem.getValue(), elem.getTimestamp(), window, elem.getPane()));
      }
    }
  }

  @Override
  public void finishBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.finishBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  private void invokeProcessElement(WindowedValue<InputT> elem) {
    final OldDoFn<InputT, OutputT>.ProcessContext processContext =
        new DoFnProcessContext<>(fn, context, elem);
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(processContext);
    } catch (Exception ex) {
      throw wrapUserCodeException(ex);
    }
  }

  private RuntimeException wrapUserCodeException(Throwable t) {
    throw UserCodeException.wrapIf(!isSystemDoFn(), t);
  }

  private boolean isSystemDoFn() {
    return fn.getClass().isAnnotationPresent(SystemDoFnInternal.class);
  }

  /**
   * A concrete implementation of {@code DoFn.Context} used for running a {@link DoFn}.
   *
   * @param <InputT>  the type of the DoFn's (main) input elements
   * @param <OutputT> the type of the DoFn's (main) output elements
   */
  private static class DoFnContext<InputT, OutputT>
      extends OldDoFn<InputT, OutputT>.Context {
    private static final int MAX_SIDE_OUTPUTS = 1000;

    final transient PipelineOptions options;
    final OldDoFn<InputT, OutputT> fn;
    final SideInputReader sideInputReader;
    final DoFnRunners.OutputManager outputManager;
    final TupleTag<OutputT> mainOutputTag;
    final ExecutionContext.StepContext stepContext;
    final WindowFn<?, ?> windowFn;

    /**
     * The set of known output tags, some of which may be undeclared, so we can throw an
     * exception when it exceeds {@link #MAX_SIDE_OUTPUTS}.
     */
    private final Set<TupleTag<?>> outputTags;

    public DoFnContext(PipelineOptions options,
        OldDoFn<InputT, OutputT> fn,
        SideInputReader sideInputReader,
        DoFnRunners.OutputManager outputManager,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> sideOutputTags,
        ExecutionContext.StepContext stepContext,
        WindowFn<?, ?> windowFn) {
      fn.super();
      this.options = options;
      this.fn = fn;
      this.sideInputReader = sideInputReader;
      this.outputManager = outputManager;
      this.mainOutputTag = mainOutputTag;
      this.outputTags = Sets.newHashSet();

      outputTags.add(mainOutputTag);
      for (TupleTag<?> sideOutputTag : sideOutputTags) {
        outputTags.add(sideOutputTag);
      }

      this.stepContext = stepContext;
      this.windowFn = windowFn;
      super.setupDelegateAggregators();
    }

    //////////////////////////////////////////////////////////////////////////////

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    <T, W extends BoundedWindow> WindowedValue<T> makeWindowedValue(
        T output, Instant timestamp, Collection<W> windows, PaneInfo pane) {
      final Instant inputTimestamp = timestamp;

      if (timestamp == null) {
        timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      if (windows == null) {
        try {
          // The windowFn can never succeed at accessing the element, so its type does not
          // matter here
          @SuppressWarnings("unchecked")
          WindowFn<Object, W> objectWindowFn = (WindowFn<Object, W>) windowFn;
          windows = objectWindowFn.assignWindows(objectWindowFn.new AssignContext() {
            @Override
            public Object element() {
              throw new UnsupportedOperationException(
                  "WindowFn attempted to access input element when none was available");
            }

            @Override
            public Instant timestamp() {
              if (inputTimestamp == null) {
                throw new UnsupportedOperationException(
                    "WindowFn attempted to access input timestamp when none was available");
              }
              return inputTimestamp;
            }

            @Override
            public BoundedWindow window() {
              throw new UnsupportedOperationException(
                  "WindowFn attempted to access input windows when none were available");
            }
          });
        } catch (Exception e) {
          throw UserCodeException.wrap(e);
        }
      }

      return WindowedValue.of(output, timestamp, windows, pane);
    }

    public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
      if (!sideInputReader.contains(view)) {
        throw new IllegalArgumentException("calling sideInput() with unknown view");
      }
      BoundedWindow sideInputWindow =
          view.getWindowingStrategyInternal().getWindowFn().getSideInputWindow(mainInputWindow);
      return sideInputReader.get(view, sideInputWindow);
    }

    void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      outputWindowedValue(makeWindowedValue(output, timestamp, windows, pane));
    }

    void outputWindowedValue(WindowedValue<OutputT> windowedElem) {
      outputManager.output(mainOutputTag, windowedElem);
      if (stepContext != null) {
        stepContext.noteOutput(windowedElem);
      }
    }

    protected <T> void sideOutputWindowedValue(TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      sideOutputWindowedValue(tag, makeWindowedValue(output, timestamp, windows, pane));
    }

    protected <T> void sideOutputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedElem) {
      if (!outputTags.contains(tag)) {
        // This tag wasn't declared nor was it seen before during this execution.
        // Thus, this must be a new, undeclared and unconsumed output.
        // To prevent likely user errors, enforce the limit on the number of side
        // outputs.
        if (outputTags.size() >= MAX_SIDE_OUTPUTS) {
          throw new IllegalArgumentException(
              "the number of side outputs has exceeded a limit of " + MAX_SIDE_OUTPUTS);
        }
        outputTags.add(tag);
      }

      outputManager.output(tag, windowedElem);
      if (stepContext != null) {
        stepContext.noteSideOutput(tag, windowedElem);
      }
    }

    // Following implementations of output, outputWithTimestamp, and sideOutput
    // are only accessible in DoFn.startBundle and DoFn.finishBundle, and will be shadowed by
    // ProcessContext's versions in DoFn.processElement.
    @Override
    public void output(OutputT output) {
      outputWindowedValue(output, null, null, PaneInfo.NO_FIRING);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputWindowedValue(output, timestamp, null, PaneInfo.NO_FIRING);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      checkNotNull(tag, "TupleTag passed to sideOutput cannot be null");
      sideOutputWindowedValue(tag, output, null, null, PaneInfo.NO_FIRING);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "TupleTag passed to sideOutputWithTimestamp cannot be null");
      sideOutputWindowedValue(tag, output, timestamp, null, PaneInfo.NO_FIRING);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
        String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      checkNotNull(combiner,
          "Combiner passed to createAggregator cannot be null");
      throw new UnsupportedOperationException("aggregator not supported in Gearpump runner");
    }
  }


  /**
   * A concrete implementation of {@code DoFn.ProcessContext} used for
   * running a {@link DoFn} over a single element.
   *
   * @param <InputT>  the type of the DoFn's (main) input elements
   * @param <OutputT> the type of the DoFn's (main) output elements
   */
  private static class DoFnProcessContext<InputT, OutputT>
      extends OldDoFn<InputT, OutputT>.ProcessContext {


    final OldDoFn<InputT, OutputT> fn;
    final DoFnContext<InputT, OutputT> context;
    final WindowedValue<InputT> windowedValue;

    public DoFnProcessContext(OldDoFn<InputT, OutputT> fn,
        DoFnContext<InputT, OutputT> context,
        WindowedValue<InputT> windowedValue) {
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
    public InputT element() {
      return windowedValue.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      checkNotNull(view, "View passed to sideInput cannot be null");
      Iterator<? extends BoundedWindow> windowIter = windows().iterator();
      BoundedWindow window;
      if (!windowIter.hasNext()) {
        if (context.windowFn instanceof GlobalWindows) {
          // TODO: Remove this once GroupByKeyOnly no longer outputs elements
          // without windows
          window = GlobalWindow.INSTANCE;
        } else {
          throw new IllegalStateException(
              "sideInput called when main input element is not in any windows");
        }
      } else {
        window = windowIter.next();
        if (windowIter.hasNext()) {
          throw new IllegalStateException(
              "sideInput called when main input element is in multiple windows");
        }
      }
      return context.sideInput(view, window);
    }

    @Override
    public BoundedWindow window() {
      if (!(fn instanceof OldDoFn.RequiresWindowAccess)) {
        throw new UnsupportedOperationException(
            "window() is only available in the context of a DoFn marked as RequiresWindow.");
      }
      return Iterables.getOnlyElement(windows());
    }

    @Override
    public PaneInfo pane() {
      return windowedValue.getPane();
    }

    @Override
    public void output(OutputT output) {
      context.outputWindowedValue(windowedValue.withValue(output));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      context.outputWindowedValue(output, timestamp,
          windowedValue.getWindows(), windowedValue.getPane());
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to sideOutput cannot be null");
      context.sideOutputWindowedValue(tag, windowedValue.withValue(output));
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to sideOutputWithTimestamp cannot be null");
      context.sideOutputWindowedValue(
          tag, output, timestamp, windowedValue.getWindows(), windowedValue.getPane());
    }

    @Override
    public Instant timestamp() {
      return windowedValue.getTimestamp();
    }

    public Collection<? extends BoundedWindow> windows() {
      return windowedValue.getWindows();
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      return new WindowingInternals<InputT, OutputT>() {
        @Override
        public void outputWindowedValue(OutputT output, Instant timestamp,
            Collection<? extends BoundedWindow> windows, PaneInfo pane) {
          context.outputWindowedValue(output, timestamp, windows, pane);
        }

        @Override
        public Collection<? extends BoundedWindow> windows() {
          return windowedValue.getWindows();
        }

        @Override
        public PaneInfo pane() {
          return windowedValue.getPane();
        }

        @Override
        public TimerInternals timerInternals() {
          return context.stepContext.timerInternals();
        }

        @Override
        public <T> void writePCollectionViewData(
            TupleTag<?> tag,
            Iterable<WindowedValue<T>> data,
            Coder<T> elemCoder) throws IOException {
          @SuppressWarnings("unchecked")
          Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>) context.windowFn.windowCoder();

          context.stepContext.writePCollectionViewData(
              tag, data, IterableCoder.of(WindowedValue.getFullCoder(elemCoder, windowCoder)),
              window(), windowCoder);
        }

        @Override
        public StateInternals<?> stateInternals() {
          return context.stepContext.stateInternals();
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
          return context.sideInput(view, mainInputWindow);
        }
      };
    }

    @Override
    protected <AggregatorInputT, AggregatorOutputT> Aggregator<AggregatorInputT, AggregatorOutputT>
    createAggregatorInternal(
        String name, Combine.CombineFn<AggregatorInputT, ?, AggregatorOutputT> combiner) {
      return context.createAggregatorInternal(name, combiner);
    }
  }
}
