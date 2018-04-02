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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers.WindowedContextOutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers.WindowedContextMultiOutputReceiver;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.FakeArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
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

  private final PipelineOptions options;
  /** The {@link DoFn} being run. */
  private final DoFn<InputT, OutputT> fn;

  /** The {@link DoFnInvoker} being run. */
  private final DoFnInvoker<InputT, OutputT> invoker;

  private final SideInputReader sideInputReader;
  private final OutputManager outputManager;

  private final TupleTag<OutputT> mainOutputTag;
  /** The set of known output tags. */
  private final Set<TupleTag<?>> outputTags;

  private final boolean observesWindow;

  private final DoFnSignature signature;

  private final Coder<BoundedWindow> windowCoder;

  private final Duration allowedLateness;

  // Because of setKey(Object), we really must refresh stateInternals() at each access
  private final StepContext stepContext;

  public SimpleDoFnRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      StepContext stepContext,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.options = options;
    this.fn = fn;
    this.signature = DoFnSignatures.getSignature(fn.getClass());
    this.observesWindow = signature.processElement().observesWindow() || !sideInputReader.isEmpty();
    this.invoker = DoFnInvokers.invokerFor(fn);
    this.sideInputReader = sideInputReader;
    this.outputManager = outputManager;
    this.mainOutputTag = mainOutputTag;
    this.outputTags =
        Sets.newHashSet(FluentIterable.<TupleTag<?>>of(mainOutputTag).append(additionalOutputTags));
    this.stepContext = stepContext;

    // This is a cast of an _invariant_ coder. But we are assured by pipeline validation
    // that it really is the coder for whatever BoundedWindow subclass is provided
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> untypedCoder =
        (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    this.windowCoder = untypedCoder;
    this.allowedLateness = windowingStrategy.getAllowedLateness();
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return fn;
  }

  @Override
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeStartBundle(new DoFnStartBundleContext());
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

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {

    // The effective timestamp is when derived elements will have their timestamp set, if not
    // otherwise specified. If this is an event time timer, then they have the timestamp of the
    // timer itself. Otherwise, they are set to the input timestamp, which is by definition
    // non-late.
    Instant effectiveTimestamp;
    switch (timeDomain) {
      case EVENT_TIME:
        effectiveTimestamp = timestamp;
        break;

      case PROCESSING_TIME:
      case SYNCHRONIZED_PROCESSING_TIME:
        effectiveTimestamp = stepContext.timerInternals().currentInputWatermarkTime();
        break;

      default:
        throw new IllegalArgumentException(
            String.format("Unknown time domain: %s", timeDomain));
    }

    OnTimerArgumentProvider argumentProvider =
        new OnTimerArgumentProvider(window, effectiveTimestamp, timeDomain);
    invoker.invokeOnTimer(timerId, argumentProvider);
  }

  private void invokeProcessElement(WindowedValue<InputT> elem) {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeProcessElement(new DoFnProcessContext(elem));
    } catch (Exception ex) {
      throw wrapUserCodeException(ex);
    }
  }

  @Override
  public void finishBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeFinishBundle(new DoFnFinishBundleContext());
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  private RuntimeException wrapUserCodeException(Throwable t) {
    throw UserCodeException.wrapIf(!isSystemDoFn(), t);
  }

  private boolean isSystemDoFn() {
    return invoker.getClass().isAnnotationPresent(SystemDoFnInternal.class);
  }

  private <T> T sideInput(PCollectionView<T> view, BoundedWindow sideInputWindow) {
    if (!sideInputReader.contains(view)) {
      throw new IllegalArgumentException("calling sideInput() with unknown view");
    }
    return sideInputReader.get(view, sideInputWindow);
  }

  private <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedElem) {
    checkArgument(outputTags.contains(tag), "Unknown output tag %s", tag);
    outputManager.output(tag, windowedElem);
  }

  /**
   * A concrete implementation of {@link DoFn.StartBundleContext}.
   */
  private class DoFnStartBundleContext
      extends DoFn<InputT, OutputT>.StartBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private DoFnStartBundleContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access paneInfo outside of @ProcessElement methods.");
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return getPipelineOptions();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @ProcessElement method.");
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Element parameters are not supported outside of @ProcessElement method.");
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access timestamp outside of @ProcessElement method.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access output receiver outside of @ProcessElement method.");
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access output receiver outside of @ProcessElement method.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
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
   * B
   * A concrete implementation of {@link DoFn.FinishBundleContext}.
   */
  private class DoFnFinishBundleContext
      extends DoFn<InputT, OutputT>.FinishBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private DoFnFinishBundleContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access paneInfo outside of @ProcessElement methods.");
    }


    @Override
    public PipelineOptions pipelineOptions() {
      return getPipelineOptions();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @ProcessElement method.");
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access element outside of @ProcessElement method.");
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access timestamp outside of @ProcessElement method.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access outputReceiver in @FinishBundle method.");
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access outputReceiver in @FinishBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
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

    @Override
    public void output(OutputT output, Instant timestamp, BoundedWindow window) {
      output(mainOutputTag, output, timestamp, window);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }
  }

  /**
   * A concrete implementation of {@link DoFn.ProcessContext} used for running a {@link DoFn} over a
   * single element.
   */
  private class DoFnProcessContext extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    final WindowedValue<InputT> elem;

    /** Lazily initialized; should only be accessed via {@link #getNamespace()}. */
    @Nullable private StateNamespace namespace;

    /**
     * The state namespace for this context.
     *
     * <p>Any call to this method when more than one window is present will crash; this
     * represents a bug in the runner or the {@link DoFnSignature}, since values must be in exactly
     * one window when state or timers are relevant.
     */
    private StateNamespace getNamespace() {
      if (namespace == null) {
        namespace = StateNamespaces.window(windowCoder, window());
      }
      return namespace;
    }

    private DoFnProcessContext(
        WindowedValue<InputT> elem) {
      fn.super();
      this.elem = elem;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public InputT element() {
      return elem.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      checkNotNull(view, "View passed to sideInput cannot be null");
      BoundedWindow window = Iterables.getOnlyElement(windows());
      return SimpleDoFnRunner.this.sideInput(
          view, view.getWindowMappingFn().getSideInputWindow(window));
    }

    @Override
    public PaneInfo pane() {
      return elem.getPane();
    }

    @Override
    public void updateWatermark(Instant watermark) {
      throw new UnsupportedOperationException("Only splittable DoFn's can use updateWatermark()");
    }

    @Override
    public void output(OutputT output) {
      output(mainOutputTag, output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      checkTimestamp(timestamp);
      outputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to output cannot be null");
      outputWindowedValue(tag, elem.withValue(output));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to outputWithTimestamp cannot be null");
      checkTimestamp(timestamp);
      outputWindowedValue(
          tag, WindowedValue.of(output, timestamp, elem.getWindows(), elem.getPane()));
    }

    @Override
    public Instant timestamp() {
      return elem.getTimestamp();
    }

    public Collection<? extends BoundedWindow> windows() {
      return elem.getWindows();
    }


    @SuppressWarnings("deprecation") // Allowed Skew is deprecated for users, but must be respected
    private void checkTimestamp(Instant timestamp) {
      // The documentation of getAllowedTimestampSkew explicitly permits Long.MAX_VALUE to be used
      // for infinite skew. Defend against underflow in that case for timestamps before the epoch
      if (fn.getAllowedTimestampSkew().getMillis() != Long.MAX_VALUE
          && timestamp.isBefore(elem.getTimestamp().minus(fn.getAllowedTimestampSkew()))) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot output with timestamp %s. Output timestamps must be no earlier than the "
                    + "timestamp of the current input (%s) minus the allowed skew (%s). See the "
                    + "DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed "
                    + "skew.",
                timestamp,
                elem.getTimestamp(),
                PeriodFormat.getDefault().print(fn.getAllowedTimestampSkew().toPeriod())));
      }
    }

    @Override
    public BoundedWindow window() {
      return Iterables.getOnlyElement(elem.getWindows());
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return pane();
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return getPipelineOptions();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("StartBundleContext parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("FinishBundleContext parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return element();
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return  timestamp();
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return new WindowedContextOutputReceiver<>(this);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return new WindowedContextMultiOutputReceiver(this);
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
    }

    @Override
    public State state(String stateId) {
      try {
        StateSpec<?> spec =
            (StateSpec<?>) signature.stateDeclarations().get(stateId).field().get(fn);
        return stepContext
            .stateInternals()
            .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec) spec));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Timer timer(String timerId) {
      try {
        TimerSpec spec =
            (TimerSpec) signature.timerDeclarations().get(timerId).field().get(fn);
        return new TimerInternalsTimer(
            window(), getNamespace(), timerId, spec, stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * A concrete implementation of {@link DoFnInvoker.ArgumentProvider} used for running a {@link
   * DoFn} on a timer.
   */
  private class OnTimerArgumentProvider
      extends DoFn<InputT, OutputT>.OnTimerContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private final BoundedWindow window;
    private final Instant timestamp;
    private final TimeDomain timeDomain;

    /** Lazily initialized; should only be accessed via {@link #getNamespace()}. */
    private @Nullable StateNamespace namespace;

    /**
     * The state namespace for this context.
     *
     * <p>Any call to this method when more than one window is present will crash; this
     * represents a bug in the runner or the {@link DoFnSignature}, since values must be in exactly
     * one window when state or timers are relevant.
     */
    private StateNamespace getNamespace() {
      if (namespace == null) {
        namespace = StateNamespaces.window(windowCoder, window);
      }
      return namespace;
    }

    private OnTimerArgumentProvider(
        BoundedWindow window,
        Instant timestamp,
        TimeDomain timeDomain) {
      fn.super();
      this.window = window;
      this.timestamp = timestamp;
      this.timeDomain = timeDomain;
    }

    @Override
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public BoundedWindow window() {
      return window;
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access paneInfo outside of @ProcessElement methods.");
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return getPipelineOptions();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("StartBundleContext parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("FinishBundleContext parameters are not supported.");
    }

    @Override
    public TimeDomain timeDomain() {
      return timeDomain;
    }


    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("ProcessContext parameters are not supported.");
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      return timeDomain();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return new WindowedContextOutputReceiver<>(this);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return new WindowedContextMultiOutputReceiver(this);
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
    }

    @Override
    public State state(String stateId) {
      try {
        StateSpec<?> spec =
            (StateSpec<?>) signature.stateDeclarations().get(stateId).field().get(fn);
        return stepContext
            .stateInternals()
            .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec) spec));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Timer timer(String timerId) {
      try {
        TimerSpec spec =
            (TimerSpec) signature.timerDeclarations().get(timerId).field().get(fn);
        return new TimerInternalsTimer(
            window, getNamespace(), timerId, spec, stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(OutputT output) {
      output(mainOutputTag, output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window(), PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window(), PaneInfo.NO_FIRING));
    }
  }

  private class TimerInternalsTimer implements Timer {
    private final TimerInternals timerInternals;

    // The window and the namespace represent the same thing, but the namespace is a cached
    // and specially encoded form. Since the namespace can be cached across timers, it is
    // passed in whole rather than being computed here.
    private final BoundedWindow window;
    private final StateNamespace namespace;
    private final String timerId;
    private final TimerSpec spec;
    private Duration period = Duration.ZERO;
    private Duration offset = Duration.ZERO;

    public TimerInternalsTimer(
        BoundedWindow window,
        StateNamespace namespace,
        String timerId,
        TimerSpec spec,
        TimerInternals timerInternals) {
      this.window = window;
      this.namespace = namespace;
      this.timerId = timerId;
      this.spec = spec;
      this.timerInternals = timerInternals;
    }

    @Override
    public void set(Instant target) {
      verifyAbsoluteTimeDomain();
      verifyTargetTime(target);
      setUnderlyingTimer(target);
    }

    @Override
    public void setRelative() {
      Instant target;
      Instant now = getCurrentTime();
      if (period.equals(Duration.ZERO)) {
        target = now.plus(offset);
      } else {
        long millisSinceStart = now.plus(offset).getMillis() % period.getMillis();
        target = millisSinceStart == 0 ? now : now.plus(period).minus(millisSinceStart);
      }
      target = minTargetAndGcTime(target);
      setUnderlyingTimer(target);
    }

    @Override
    public Timer offset(Duration offset) {
      this.offset = offset;
      return this;
    }

    @Override
    public Timer align(Duration period) {
      this.period = period;
      return this;
    }

    /**
     * For event time timers the target time should be prior to window GC time. So it return
     * min(time to set, GC Time of window).
     */
    private Instant minTargetAndGcTime(Instant target) {
      if (TimeDomain.EVENT_TIME.equals(spec.getTimeDomain())) {
        Instant windowExpiry = LateDataUtils.garbageCollectionTime(window, allowedLateness);
        if (target.isAfter(windowExpiry)) {
          return windowExpiry;
        }
      }
      return target;
    }

    /**
     * Ensures that the target time is reasonable. For event time timers this means that the
     * time should be prior to window GC time.
     */
    private void verifyTargetTime(Instant target) {
      if (TimeDomain.EVENT_TIME.equals(spec.getTimeDomain())) {
        Instant windowExpiry = window.maxTimestamp().plus(allowedLateness);
        checkArgument(!target.isAfter(windowExpiry),
            "Attempted to set event time timer for %s but that is after"
            + " the expiration of window %s", target, windowExpiry);
      }
    }

    /** Verifies that the time domain of this timer is acceptable for absolute timers. */
    private void verifyAbsoluteTimeDomain() {
      if (!TimeDomain.EVENT_TIME.equals(spec.getTimeDomain())) {
        throw new IllegalStateException(
            "Cannot only set relative timers in processing time domain."
                + " Use #setRelative()");
      }
    }

    /**
     * Sets the timer for the target time without checking anything about whether it is
     * a reasonable thing to do. For example, absolute processing time timers are not
     * really sensible since the user has no way to compute a good choice of time.
     */
    private void setUnderlyingTimer(Instant target) {
      timerInternals.setTimer(namespace, timerId, target, spec.getTimeDomain());
    }

    private Instant getCurrentTime() {
      switch(spec.getTimeDomain()) {
        case EVENT_TIME:
          return timerInternals.currentInputWatermarkTime();
        case PROCESSING_TIME:
          return timerInternals.currentProcessingTime();
        case SYNCHRONIZED_PROCESSING_TIME:
          return timerInternals.currentSynchronizedProcessingTime();
        default:
          throw new IllegalStateException(
              String.format("Timer created for unknown time domain %s", spec.getTimeDomain()));
      }
    }

  }
}
