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
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Context;
import org.apache.beam.sdk.transforms.DoFn.InputProvider;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.ExecutionContext.StepContext;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;

/**
 * Runs a {@link DoFn} by constructing the appropriate contexts and passing them in.
 *
 * <p>Also, if the {@link DoFn} observes the window of the element, then {@link SimpleDoFnRunner}
 * explodes windows of the input {@link WindowedValue} and calls {@link DoFn.ProcessElement} for
 * each window individually.
 *
 * @param <InputT> the type of the {@link DoFn} (main) input elements
 * @param <OutputT> the type of the {@link DoFn} (main) output elements
 */
public class SimpleDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  /** The {@link DoFn} being run. */
  private final DoFn<InputT, OutputT> fn;

  /** The {@link DoFnInvoker} being run. */
  private final DoFnInvoker<InputT, OutputT> invoker;

  /** The context used for running the {@link DoFn}. */
  private final DoFnContext<InputT, OutputT> context;

  private final OutputManager outputManager;

  private final TupleTag<OutputT> mainOutputTag;

  private final boolean observesWindow;

  public SimpleDoFnRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      AggregatorFactory aggregatorFactory,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.fn = fn;
    this.observesWindow =
        DoFnSignatures.getSignature(fn.getClass()).processElement().observesWindow();
    this.invoker = DoFnInvokers.invokerFor(fn);
    this.outputManager = outputManager;
    this.mainOutputTag = mainOutputTag;
    this.context =
        new DoFnContext<>(
            options,
            fn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            stepContext,
            aggregatorFactory,
            windowingStrategy == null ? null : windowingStrategy.getWindowFn());
  }

  @Override
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeStartBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  @Override
  public void processElement(WindowedValue<InputT> compressedElem) {
    if (observesWindow) {
      for (WindowedValue<InputT> elem : compressedElem.explodeWindows()) {
        invokeProcessElement(elem);
      }
    } else {
      invokeProcessElement(compressedElem);
    }
  }

  private void invokeProcessElement(WindowedValue<InputT> elem) {
    final DoFnProcessContext<InputT, OutputT> processContext = createProcessContext(elem);

    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeProcessElement(processContext);
    } catch (Exception ex) {
      throw wrapUserCodeException(ex);
    }
  }

  @Override
  public void finishBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeFinishBundle(context);
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  /** Returns a new {@link DoFn.ProcessContext} for the given element. */
  private DoFnProcessContext<InputT, OutputT> createProcessContext(WindowedValue<InputT> elem) {
    return new DoFnProcessContext<InputT, OutputT>(fn, context, elem);
  }

  private RuntimeException wrapUserCodeException(Throwable t) {
    throw UserCodeException.wrapIf(!isSystemDoFn(), t);
  }

  private boolean isSystemDoFn() {
    return invoker.getClass().isAnnotationPresent(SystemDoFnInternal.class);
  }

  /**
   * A concrete implementation of {@code DoFn.Context} used for running a {@link DoFn}.
   *
   * @param <InputT> the type of the {@link DoFn} (main) input elements
   * @param <OutputT> the type of the {@link DoFn} (main) output elements
   */
  private static class DoFnContext<InputT, OutputT> extends DoFn<InputT, OutputT>.Context
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private static final int MAX_SIDE_OUTPUTS = 1000;

    final PipelineOptions options;
    final DoFn<InputT, OutputT> fn;
    final SideInputReader sideInputReader;
    final OutputManager outputManager;
    final TupleTag<OutputT> mainOutputTag;
    final StepContext stepContext;
    final AggregatorFactory aggregatorFactory;
    final WindowFn<?, ?> windowFn;

    /**
     * The set of known output tags, some of which may be undeclared, so we can throw an exception
     * when it exceeds {@link #MAX_SIDE_OUTPUTS}.
     */
    private Set<TupleTag<?>> outputTags;

    public DoFnContext(
        PipelineOptions options,
        DoFn<InputT, OutputT> fn,
        SideInputReader sideInputReader,
        OutputManager outputManager,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> sideOutputTags,
        StepContext stepContext,
        AggregatorFactory aggregatorFactory,
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
      this.aggregatorFactory = aggregatorFactory;
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
          windows =
              objectWindowFn.assignWindows(
                  objectWindowFn.new AssignContext() {
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

    private <T> void sideOutputWindowedValue(
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      sideOutputWindowedValue(tag, makeWindowedValue(output, timestamp, windows, pane));
    }

    private <T> void sideOutputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedElem) {
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
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      checkNotNull(combiner, "Combiner passed to createAggregator cannot be null");
      return aggregatorFactory.createAggregatorForDoFn(fn.getClass(), stepContext, name, combiner);
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public Context context(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @Processelement method.");
    }

    @Override
    public InputProvider<InputT> inputProvider() {
      throw new UnsupportedOperationException("InputProvider is for testing only.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver() {
      throw new UnsupportedOperationException("OutputReceiver is for testing only.");
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      throw new UnsupportedOperationException("WindowingInternals are unsupported.");
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
      throw new UnsupportedOperationException(
          "Cannot access RestrictionTracker outside of @ProcessElement method.");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException(
          "Cannot access state outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          "Cannot access timers outside of @ProcessElement and @OnTimer methods.");
    }
  }

  /**
   * A concrete implementation of {@link DoFn.ProcessContext} used for running a {@link DoFn} over a
   * single element.
   *
   * @param <InputT> the type of the {@link DoFn} (main) input elements
   * @param <OutputT> the type of the {@link DoFn} (main) output elements
   */
  private class DoFnProcessContext<InputT, OutputT> extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    final DoFn<InputT, OutputT> fn;
    final DoFnContext<InputT, OutputT> context;
    final WindowedValue<InputT> windowedValue;

    private DoFnProcessContext(
        DoFn<InputT, OutputT> fn,
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
          view, view.getWindowingStrategyInternal().getWindowFn().getSideInputWindow(window));
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
      context.outputWindowedValue(
          output, timestamp, windowedValue.getWindows(), windowedValue.getPane());
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to sideOutput cannot be null");
      context.sideOutputWindowedValue(tag, windowedValue.withValue(output));
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to sideOutputWithTimestamp cannot be null");
      checkTimestamp(timestamp);
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

    private void checkTimestamp(Instant timestamp) {
      if (timestamp.isBefore(windowedValue.getTimestamp().minus(fn.getAllowedTimestampSkew()))) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot output with timestamp %s. Output timestamps must be no earlier than the "
                    + "timestamp of the current input (%s) minus the allowed skew (%s). See the "
                    + "DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed "
                    + "skew.",
                timestamp,
                windowedValue.getTimestamp(),
                PeriodFormat.getDefault().print(fn.getAllowedTimestampSkew().toPeriod())));
      }
    }

    @Override
    protected <AggregatorInputT, AggregatorOutputT>
        Aggregator<AggregatorInputT, AggregatorOutputT> createAggregator(
            String name, CombineFn<AggregatorInputT, ?, AggregatorOutputT> combiner) {
      return context.createAggregator(name, combiner);
    }

    @Override
    public BoundedWindow window() {
      return Iterables.getOnlyElement(windowedValue.getWindows());
    }

    @Override
    public DoFn<InputT, OutputT>.Context context(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public InputProvider<InputT> inputProvider() {
      throw new UnsupportedOperationException("InputProvider parameters are not supported.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver() {
      throw new UnsupportedOperationException("OutputReceiver parameters are not supported.");
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
      throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
    }

    @Override
    public State state(String timerId) {
      throw new UnsupportedOperationException("State parameters are not supported.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timer parameters are not supported.");
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      return new WindowingInternals<InputT, OutputT>() {
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
            TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder)
            throws IOException {
          @SuppressWarnings("unchecked")
          Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>) context.windowFn.windowCoder();

          context.stepContext.writePCollectionViewData(
              tag,
              data,
              IterableCoder.of(WindowedValue.getFullCoder(elemCoder, windowCoder)),
              window(),
              windowCoder);
        }

        @Override
        public StateInternals<?> stateInternals() {
          return context.stepContext.stateInternals();
        }

        @Override
        public void outputWindowedValue(
            OutputT output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          throw new UnsupportedOperationException("A DoFn cannot output to a different window");
        }

        @Override
        public <SideOutputT> void sideOutputWindowedValue(
            TupleTag<SideOutputT> tag,
            SideOutputT output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          throw new UnsupportedOperationException(
              "A DoFn cannot side output to a different window");
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view, BoundedWindow sideInputWindow) {
          return context.sideInput(view, sideInputWindow);
        }
      };
    }
  }
}
