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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.ExecutionContext.StepContext;
import org.apache.beam.runners.core.OldDoFn.RequiresWindowAccess;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;

/**
 * Runs a {@link OldDoFn} by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the {@link OldDoFn} (main) input elements
 * @param <OutputT> the type of the {@link OldDoFn} (main) output elements
 */
class SimpleOldDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  /** The {@link OldDoFn} being run. */
  private final OldDoFn<InputT, OutputT> fn;
  /** The context used for running the {@link OldDoFn}. */
  private final DoFnContext<InputT, OutputT> context;

  public SimpleOldDoFnRunner(
      PipelineOptions options,
      OldDoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      StepContext stepContext,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.fn = fn;
    this.context = new DoFnContext<>(
        options,
        fn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        windowingStrategy == null ? null : windowingStrategy.getWindowFn());
  }

  @Override
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.startBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    if (elem.getWindows().size() <= 1
        || (!RequiresWindowAccess.class.isAssignableFrom(fn.getClass())
            && context.sideInputReader.isEmpty())) {
      invokeProcessElement(elem);
    } else {
      // We could modify the windowed value (and the processContext) to
      // avoid repeated allocations, but this is more straightforward.
      for (WindowedValue<InputT> windowedValue : elem.explodeWindows()) {
        invokeProcessElement(windowedValue);
      }
    }
  }

  @Override
  public void onTimer(String timerId, BoundedWindow window, Instant timestamp,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException(
        String.format("Timers are not supported by %s", OldDoFn.class.getSimpleName()));
  }

  private void invokeProcessElement(WindowedValue<InputT> elem) {
    final OldDoFn<InputT, OutputT>.ProcessContext processContext = createProcessContext(elem);
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      fn.processElement(processContext);
    } catch (Exception ex) {
      throw wrapUserCodeException(ex);
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

  /**
   * Returns a new {@link OldDoFn.ProcessContext} for the given element.
   */
  private OldDoFn<InputT, OutputT>.ProcessContext createProcessContext(
      WindowedValue<InputT> elem) {
    return new DoFnProcessContext<InputT, OutputT>(fn, context, elem);
  }

  private RuntimeException wrapUserCodeException(Throwable t) {
    throw UserCodeException.wrapIf(!isSystemDoFn(), t);
  }

  private boolean isSystemDoFn() {
    return fn.getClass().isAnnotationPresent(SystemDoFnInternal.class);
  }

  /**
   * A concrete implementation of {@code OldDoFn.Context} used for running a {@link OldDoFn}.
   *
   * @param <InputT> the type of the {@link OldDoFn} (main) input elements
   * @param <OutputT> the type of the {@link OldDoFn} (main) output elements
   */
  private static class DoFnContext<InputT, OutputT> extends OldDoFn<InputT, OutputT>.Context {
    private static final int MAX_SIDE_OUTPUTS = 1000;

    final PipelineOptions options;
    final OldDoFn<InputT, OutputT> fn;
    final SideInputReader sideInputReader;
    final OutputManager outputManager;
    final TupleTag<OutputT> mainOutputTag;
    final StepContext stepContext;
    final WindowFn<?, ?> windowFn;

    /**
     * The set of known output tags, some of which may be undeclared, so we can throw an
     * exception when it exceeds {@link #MAX_SIDE_OUTPUTS}.
     */
    private Set<TupleTag<?>> outputTags;

    public DoFnContext(PipelineOptions options,
                       OldDoFn<InputT, OutputT> fn,
                       SideInputReader sideInputReader,
                       OutputManager outputManager,
                       TupleTag<OutputT> mainOutputTag,
                       List<TupleTag<?>> additionalOutputTags,
                       StepContext stepContext,
                       WindowFn<?, ?> windowFn) {
      fn.super();
      this.options = options;
      this.fn = fn;
      this.sideInputReader = sideInputReader;
      this.outputManager = outputManager;
      this.mainOutputTag = mainOutputTag;
      this.outputTags = Sets.newHashSet();

      outputTags.add(mainOutputTag);
      for (TupleTag<?> additionalOutputTag : additionalOutputTags) {
        outputTags.add(additionalOutputTag);
      }

      this.stepContext = stepContext;
      this.windowFn = windowFn;
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
            public W window() {
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

    public <T> T sideInput(PCollectionView<T> view, BoundedWindow sideInputWindow) {
      if (!sideInputReader.contains(view)) {
        throw new IllegalArgumentException("calling sideInput() with unknown view");
      }
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

    private <T> void outputWindowedValue(TupleTag<T> tag,
                                               T output,
                                               Instant timestamp,
                                               Collection<? extends BoundedWindow> windows,
                                               PaneInfo pane) {
      outputWindowedValue(tag, makeWindowedValue(output, timestamp, windows, pane));
    }

    private <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedElem) {
      if (!outputTags.contains(tag)) {
        // This tag wasn't declared nor was it seen before during this execution.
        // Thus, this must be a new, undeclared and unconsumed output.
        // To prevent likely user errors, enforce the limit on the number of side
        // outputs.
        if (outputTags.size() >= MAX_SIDE_OUTPUTS) {
          throw new IllegalArgumentException(
              "the number of outputs has exceeded a limit of " + MAX_SIDE_OUTPUTS);
        }
        outputTags.add(tag);
      }

      outputManager.output(tag, windowedElem);
      if (stepContext != null) {
        stepContext.noteOutput(tag, windowedElem);
      }
    }

    // Following implementations of output, outputWithTimestamp, and output
    // are only accessible in OldDoFn.startBundle and OldDoFn.finishBundle, and will be shadowed by
    // ProcessContext's versions in OldDoFn.processElement.
    @Override
    public void output(OutputT output) {
      outputWindowedValue(output, null, null, PaneInfo.NO_FIRING);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputWindowedValue(output, timestamp, null, PaneInfo.NO_FIRING);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkNotNull(tag, "TupleTag passed to output cannot be null");
      outputWindowedValue(tag, output, null, null, PaneInfo.NO_FIRING);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "TupleTag passed to outputWithTimestamp cannot be null");
      outputWindowedValue(tag, output, timestamp, null, PaneInfo.NO_FIRING);
    }
  }

  /**
   * A concrete implementation of {@link OldDoFn.ProcessContext} used for running a {@link OldDoFn}
   * over a single element.
   *
   * @param <InputT> the type of the {@link OldDoFn} (main) input elements
   * @param <OutputT> the type of the {@link OldDoFn} (main) output elements
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
      return context.sideInput(
          view, view.getWindowMappingFn().getSideInputWindow(window));
    }

    @Override
    public BoundedWindow window() {
      if (!(fn instanceof OldDoFn.RequiresWindowAccess)) {
        throw new UnsupportedOperationException(
            "window() is only available in the context of a OldDoFn marked as"
                + "RequiresWindowAccess.");
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
      checkTimestamp(timestamp);
      context.outputWindowedValue(output, timestamp,
          windowedValue.getWindows(), windowedValue.getPane());
    }

    void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      context.outputWindowedValue(output, timestamp, windows, pane);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to output cannot be null");
      context.outputWindowedValue(tag, windowedValue.withValue(output));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to outputWithTimestamp cannot be null");
      checkTimestamp(timestamp);
      context.outputWindowedValue(
          tag, output, timestamp, windowedValue.getWindows(), windowedValue.getPane());
    }

    @Override
    public Instant timestamp() {
      return windowedValue.getTimestamp();
    }

    public Collection<? extends BoundedWindow> windows() {
      return windowedValue.getWindows();
    }

    private void checkTimestamp(Instant timestamp) {
      if (timestamp.isBefore(windowedValue.getTimestamp().minus(fn.getAllowedTimestampSkew()))) {
        throw new IllegalArgumentException(String.format(
            "Cannot output with timestamp %s. Output timestamps must be no earlier than the "
            + "timestamp of the current input (%s) minus the allowed skew (%s). See the "
            + "OldDoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed skew.",
            timestamp, windowedValue.getTimestamp(),
            PeriodFormat.getDefault().print(fn.getAllowedTimestampSkew().toPeriod())));
      }
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
        public <AdditionalOutputT> void outputWindowedValue(
            TupleTag<AdditionalOutputT> tag,
            AdditionalOutputT output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          context.outputWindowedValue(tag, output, timestamp, windows, pane);
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
        public StateInternals stateInternals() {
          return context.stepContext.stateInternals();
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view, BoundedWindow sideInputWindow) {
          return context.sideInput(view, sideInputWindow);
        }
      };
    }
  }
}
