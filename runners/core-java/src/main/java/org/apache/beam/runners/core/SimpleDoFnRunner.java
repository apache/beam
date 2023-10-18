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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
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
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness",
  "keyfor"
}) // TODO(https://github.com/apache/beam/issues/20497)
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

  final @Nullable SchemaCoder<OutputT> mainOutputSchemaCoder;

  private @Nullable Map<TupleTag<?>, Coder<?>> outputCoders;

  private final @Nullable DoFnSchemaInformation doFnSchemaInformation;

  private final Map<String, PCollectionView<?>> sideInputMapping;

  /** Constructor. */
  public SimpleDoFnRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      StepContext stepContext,
      @Nullable Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.options = options;
    this.fn = fn;
    this.signature = DoFnSignatures.getSignature(fn.getClass());
    this.observesWindow = signature.processElement().observesWindow() || !sideInputReader.isEmpty();
    this.invoker = DoFnInvokers.invokerFor(fn);
    this.sideInputReader = sideInputReader;
    this.outputCoders = outputCoders;
    if (outputCoders != null && !outputCoders.isEmpty()) {
      Coder<OutputT> outputCoder = (Coder<OutputT>) outputCoders.get(mainOutputTag);
      mainOutputSchemaCoder =
          (outputCoder instanceof SchemaCoder) ? (SchemaCoder<OutputT>) outputCoder : null;
    } else {
      mainOutputSchemaCoder = null;
    }
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
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return fn;
  }

  @Override
  public void startBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeStartBundle(new DoFnStartBundleArgumentProvider());
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
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    Preconditions.checkNotNull(outputTimestamp, "outputTimestamp");

    OnTimerArgumentProvider<KeyT> argumentProvider =
        new OnTimerArgumentProvider<>(timerId, key, window, timestamp, outputTimestamp, timeDomain);
    invoker.invokeOnTimer(timerId, timerFamilyId, argumentProvider);
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
      invoker.invokeFinishBundle(new DoFnFinishBundleArgumentProvider());
    } catch (Throwable t) {
      // Exception in user code.
      throw wrapUserCodeException(t);
    }
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    invoker.invokeOnWindowExpiration(
        new OnWindowExpirationArgumentProvider<>(window, timestamp, key));
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

  @SuppressWarnings("deprecation") // Allowed Skew is deprecated for users, but must be respected
  private void checkTimestamp(Instant elemTimestamp, Instant timestamp) {
    Instant lowerBound;
    try {
      lowerBound = elemTimestamp.minus(fn.getAllowedTimestampSkew());
    } catch (ArithmeticException e) {
      lowerBound = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    if (timestamp.isBefore(lowerBound) || timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot output with timestamp %s. Output timestamps must be no earlier than the "
                  + "timestamp of the current input or timer (%s) minus the allowed skew (%s) and no "
                  + "later than %s. See the DoFn#getAllowedTimestampSkew() Javadoc for details "
                  + "on changing the allowed skew.",
              timestamp,
              elemTimestamp,
              fn.getAllowedTimestampSkew().getMillis() >= Integer.MAX_VALUE
                  ? fn.getAllowedTimestampSkew()
                  : PeriodFormat.getDefault().print(fn.getAllowedTimestampSkew().toPeriod()),
              BoundedWindow.TIMESTAMP_MAX_VALUE));
    }
  }

  private <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedElem) {
    checkArgument(outputTags.contains(tag), "Unknown output tag %s", tag);
    outputManager.output(tag, windowedElem);
  }

  /** An {@link DoFnInvoker.ArgumentProvider} for {@link DoFn.StartBundle @StartBundle}. */
  private class DoFnStartBundleArgumentProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {
    /** A concrete implementation of {@link DoFn.StartBundleContext}. */
    private class Context extends DoFn<InputT, OutputT>.StartBundleContext {
      private Context() {
        fn.super();
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return options;
      }
    }

    private final DoFnStartBundleArgumentProvider.Context context =
        new DoFnStartBundleArgumentProvider.Context();

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return context;
    }

    @Override
    public String getErrorContext() {
      return "SimpleDoFnRunner/StartBundle";
    }
  }

  /** An {@link DoFnInvoker.ArgumentProvider} for {@link DoFn.StartBundle @StartBundle}. */
  private class DoFnFinishBundleArgumentProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {
    /** A concrete implementation of {@link DoFn.FinishBundleContext}. */
    private class Context extends DoFn<InputT, OutputT>.FinishBundleContext {
      private Context() {
        fn.super();
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return options;
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

    private final DoFnFinishBundleArgumentProvider.Context context =
        new DoFnFinishBundleArgumentProvider.Context();

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return context;
    }

    @Override
    public String getErrorContext() {
      return "SimpleDoFnRunner/FinishBundle";
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
    private @Nullable StateNamespace namespace;

    /**
     * The state namespace for this context.
     *
     * <p>Any call to this method when more than one window is present will crash; this represents a
     * bug in the runner or the {@link DoFnSignature}, since values must be in exactly one window
     * when state or timers are relevant.
     */
    private StateNamespace getNamespace() {
      if (namespace == null) {
        namespace = StateNamespaces.window(windowCoder, window());
      }
      return namespace;
    }

    private DoFnProcessContext(WindowedValue<InputT> elem) {
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
    public void output(OutputT output) {
      output(mainOutputTag, output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      checkTimestamp(elem.getTimestamp(), timestamp);
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
      checkTimestamp(elem.getTimestamp(), timestamp);
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
    public Object key() {
      throw new UnsupportedOperationException(
          "Cannot access key as parameter outside of @OnTimer method.");
    }

    @Override
    public Object sideInput(String tagId) {
      return sideInput(sideInputMapping.get(tagId));
    }

    @Override
    public Object schemaElement(int index) {
      SerializableFunction converter = doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(element());
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access timerId as parameter outside of @OnTimer method.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, mainOutputTag);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, mainOutputTag, mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this, outputCoders);
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException(
          "@Restriction parameters are not supported. Only the RestrictionTracker is accessible.");
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
    public Object watermarkEstimatorState() {
      throw new UnsupportedOperationException(
          "@WatermarkEstimatorState parameters are not supported.");
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      throw new UnsupportedOperationException("WatermarkEstimator parameters are not supported.");
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      try {
        StateSpec<?> spec =
            (StateSpec<?>) signature.stateDeclarations().get(stateId).field().get(fn);
        State state =
            stepContext
                .stateInternals()
                .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec) spec));
        if (alwaysFetched) {
          return (State) ((ReadableState) state).readLater();
        } else {
          return state;
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Timer timer(String timerId) {
      try {
        TimerSpec spec = (TimerSpec) signature.timerDeclarations().get(timerId).field().get(fn);
        return new TimerInternalsTimer(
            window(), getNamespace(), timerId, spec, timestamp(), stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      try {
        TimerSpec spec =
            (TimerSpec) signature.timerFamilyDeclarations().get(timerFamilyId).field().get(fn);
        return new TimerInternalsTimerMap(
            timerFamilyId,
            window(),
            getNamespace(),
            spec,
            timestamp(),
            stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return stepContext.bundleFinalizer();
    }
  }

  /**
   * A concrete implementation of {@link DoFnInvoker.ArgumentProvider} used for running a {@link
   * DoFn} on a timer.
   */
  private class OnTimerArgumentProvider<KeyT> extends DoFn<InputT, OutputT>.OnTimerContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private final BoundedWindow window;
    private final Instant fireTimestamp;
    private final Instant timestamp;
    private final TimeDomain timeDomain;
    private final String timerId;
    private final KeyT key;

    /** Lazily initialized; should only be accessed via {@link #getNamespace()}. */
    private @Nullable StateNamespace namespace;

    /**
     * The state namespace for this context.
     *
     * <p>Any call to this method when more than one window is present will crash; this represents a
     * bug in the runner or the {@link DoFnSignature}, since values must be in exactly one window
     * when state or timers are relevant.
     */
    private StateNamespace getNamespace() {
      if (namespace == null) {
        namespace = StateNamespaces.window(windowCoder, window);
      }
      return namespace;
    }

    private OnTimerArgumentProvider(
        String timerId,
        KeyT key,
        BoundedWindow window,
        Instant fireTimestamp,
        Instant timestamp,
        TimeDomain timeDomain) {
      fn.super();
      this.timerId = timerId;
      this.window = window;
      this.fireTimestamp = fireTimestamp;
      this.timestamp = timestamp;
      this.timeDomain = timeDomain;
      this.key = key;
    }

    @Override
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public Instant fireTimestamp() {
      return fireTimestamp;
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
    public KeyT key() {
      return key;
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
    public Object sideInput(String tagId) {
      throw new UnsupportedOperationException("SideInput parameters are not supported.");
    }

    @Override
    public Object schemaElement(int index) {
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      return timerId;
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      return timeDomain();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, mainOutputTag);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, mainOutputTag, mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this, outputCoders);
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException("@Restriction parameters are not supported.");
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
    public Object watermarkEstimatorState() {
      throw new UnsupportedOperationException(
          "@WatermarkEstimatorState parameters are not supported.");
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      throw new UnsupportedOperationException("WatermarkEstimator parameters are not supported.");
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      try {
        StateSpec<?> spec =
            (StateSpec<?>) signature.stateDeclarations().get(stateId).field().get(fn);
        State state =
            stepContext
                .stateInternals()
                .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec) spec));
        if (alwaysFetched) {
          return (State) ((ReadableState) state).readLater();
        } else {
          return state;
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Timer timer(String timerId) {
      try {
        TimerSpec spec = (TimerSpec) signature.timerDeclarations().get(timerId).field().get(fn);
        return new TimerInternalsTimer(
            window, getNamespace(), timerId, spec, timestamp(), stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      try {
        TimerSpec spec =
            (TimerSpec) signature.timerFamilyDeclarations().get(timerFamilyId).field().get(fn);
        return new TimerInternalsTimerMap(
            timerFamilyId,
            window(),
            getNamespace(),
            spec,
            timestamp(),
            stepContext.timerInternals());
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
      checkTimestamp(timestamp(), timestamp);
      outputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkTimestamp(timestamp(), timestamp);
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window(), PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkTimestamp(timestamp(), timestamp);
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window(), PaneInfo.NO_FIRING));
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      throw new UnsupportedOperationException(
          "Bundle finalization is not supported in non-portable pipelines.");
    }
  }

  /**
   * A concrete implementation of {@link DoFnInvoker.ArgumentProvider} used for running a {@link
   * DoFn} on window expiration.
   */
  private class OnWindowExpirationArgumentProvider<KeyT>
      extends DoFn<InputT, OutputT>.OnWindowExpirationContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private final BoundedWindow window;
    private final Instant timestamp;
    private final KeyT key;
    /** Lazily initialized; should only be accessed via {@link #getNamespace()}. */
    private @Nullable StateNamespace namespace;

    /**
     * The state namespace for this context.
     *
     * <p>Any call to this method when more than one window is present will crash; this represents a
     * bug in the runner or the {@link DoFnSignature}, since values must be in exactly one window
     * when state or timers are relevant.
     */
    private StateNamespace getNamespace() {
      if (namespace == null) {
        namespace = StateNamespaces.window(windowCoder, window);
      }
      return namespace;
    }

    private OnWindowExpirationArgumentProvider(BoundedWindow window, Instant timestamp, KeyT key) {
      fn.super();
      this.window = window;
      this.timestamp = timestamp;
      this.key = key;
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
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("ProcessContext parameters are not supported.");
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public Object sideInput(String tagId) {
      throw new UnsupportedOperationException("SideInput parameters are not supported.");
    }

    @Override
    public Object schemaElement(int index) {
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp;
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Timer parameters are not supported.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public KeyT key() {
      return key;
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, mainOutputTag);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, mainOutputTag, mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this, outputCoders);
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException("@Restriction parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("OnTimerContext parameters are not supported.");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
    }

    @Override
    public Object watermarkEstimatorState() {
      throw new UnsupportedOperationException(
          "@WatermarkEstimatorState parameters are not supported.");
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      throw new UnsupportedOperationException("WatermarkEstimator parameters are not supported.");
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      try {
        StateSpec<?> spec =
            (StateSpec<?>) signature.stateDeclarations().get(stateId).field().get(fn);
        State state =
            stepContext
                .stateInternals()
                .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec) spec));
        if (alwaysFetched) {
          return (State) ((ReadableState) state).readLater();
        } else {
          return state;
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timer parameters are not supported.");
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      throw new UnsupportedOperationException("TimerFamily parameters are not supported.");
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
      checkTimestamp(this.timestamp, timestamp);
      outputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkTimestamp(this.timestamp, timestamp);
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window(), PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkTimestamp(this.timestamp, timestamp);
      outputWindowedValue(tag, WindowedValue.of(output, timestamp, window(), PaneInfo.NO_FIRING));
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      throw new UnsupportedOperationException(
          "Bundle finalization is not supported in non-portable pipelines.");
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
    private final String timerFamilyId;
    private final TimerSpec spec;
    private Instant target;
    private @Nullable Instant outputTimestamp;
    private boolean noOutputTimestamp;
    private final Instant elementInputTimestamp;
    private Duration period = Duration.ZERO;
    private Duration offset = Duration.ZERO;

    public TimerInternalsTimer(
        BoundedWindow window,
        StateNamespace namespace,
        String timerId,
        TimerSpec spec,
        Instant elementInputTimestamp,
        TimerInternals timerInternals) {
      this.window = window;
      this.namespace = namespace;
      this.timerId = timerId;
      this.timerFamilyId = "";
      this.spec = spec;
      this.noOutputTimestamp = false;
      this.elementInputTimestamp = elementInputTimestamp;
      this.timerInternals = timerInternals;
    }

    public TimerInternalsTimer(
        BoundedWindow window,
        StateNamespace namespace,
        String timerId,
        String timerFamilyId,
        TimerSpec spec,
        Instant elementInputTimestamp,
        TimerInternals timerInternals) {
      this.window = window;
      this.namespace = namespace;
      this.timerId = timerId;
      this.timerFamilyId = timerFamilyId;
      this.spec = spec;
      this.elementInputTimestamp = elementInputTimestamp;
      this.timerInternals = timerInternals;
    }

    @Override
    public void set(Instant target) {
      this.target = target;
      setAndVerifyOutputTimestamp();
      setUnderlyingTimer();
    }

    @Override
    public void setRelative() {
      Instant now = getCurrentRelativeTime();
      if (period.equals(Duration.ZERO)) {
        target = now.plus(offset);
      } else {
        long millisSinceStart = now.plus(offset).getMillis() % period.getMillis();
        target =
            millisSinceStart == 0 ? now : now.plus(period).minus(Duration.millis(millisSinceStart));
      }
      target = minTargetAndGcTime(target);

      setAndVerifyOutputTimestamp();
      setUnderlyingTimer();
    }

    @Override
    public void clear() {
      timerInternals.deleteTimer(namespace, timerId, timerFamilyId, spec.getTimeDomain());
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
     * For event time timers the target time should be prior to window GC time. So it returns
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

    @Override
    public Timer withOutputTimestamp(Instant outputTimestamp) {
      this.outputTimestamp = outputTimestamp;
      this.noOutputTimestamp = false;
      return this;
    }

    @Override
    public Timer withNoOutputTimestamp() {
      this.outputTimestamp = null;
      this.noOutputTimestamp = true;
      return this;
    }

    /**
     * Ensures that a timer's {@code outputTimestamp} is set at or after the current input timestamp
     * (minus allowed timestamp skew if set) and before the max timestamp of the window (plus
     * allowed lateness). <br>
     * If the outputTimestamp is not set, it is defaulted to either:
     * <li>The firing timestamp for timers in the {@link TimeDomain#EVENT_TIME}
     * <li>The current element timestamp for other time domains.
     */
    private void setAndVerifyOutputTimestamp() {
      if (outputTimestamp != null) {
        Instant lowerBound;
        try {
          lowerBound = elementInputTimestamp.minus(fn.getAllowedTimestampSkew());
        } catch (ArithmeticException e) {
          lowerBound = BoundedWindow.TIMESTAMP_MIN_VALUE;
        }
        if (outputTimestamp.isBefore(lowerBound)
            || outputTimestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot output timer with output timestamp %s. Output timestamps must be no "
                      + "earlier than the timestamp of the current input or timer (%s) minus the "
                      + "allowed skew (%s) and no later than %s. See the "
                      + "DoFn#getAllowedTimestampSkew() Javadoc for details on changing the "
                      + "allowed skew.",
                  outputTimestamp,
                  elementInputTimestamp,
                  fn.getAllowedTimestampSkew().getMillis() >= Integer.MAX_VALUE
                      ? fn.getAllowedTimestampSkew()
                      : PeriodFormat.getDefault().print(fn.getAllowedTimestampSkew().toPeriod()),
                  BoundedWindow.TIMESTAMP_MAX_VALUE));
        }
      } else if (!noOutputTimestamp && TimeDomain.EVENT_TIME.equals(spec.getTimeDomain())) {
        // The outputTimestamp was unset and this is a timer in the EVENT_TIME domain. The output
        // timestamp will be the firing timestamp.
        outputTimestamp = target;
      } else if (!noOutputTimestamp) {
        // The outputTimestamp was unset and this is a timer in the PROCESSING_TIME
        // (or SYNCHRONIZED_PROCESSING_TIME) domain. The output timestamp will be the timestamp of
        // the element (or timer) setting this timer.
        outputTimestamp = elementInputTimestamp;
      }
      if (outputTimestamp != null) {
        Instant windowExpiry = LateDataUtils.garbageCollectionTime(window, allowedLateness);
        if (TimeDomain.EVENT_TIME.equals(spec.getTimeDomain())) {
          checkArgument(
              !outputTimestamp.isAfter(windowExpiry),
              "Attempted to set an event-time timer with an output timestamp of %s that is"
                  + " after the expiration of window %s",
              outputTimestamp,
              windowExpiry);
          checkArgument(
              !target.isAfter(windowExpiry),
              "Attempted to set an event-time timer with a firing timestamp of %s that is"
                  + " after the expiration of window %s",
              target,
              windowExpiry);
        } else {
          checkArgument(
              !outputTimestamp.isAfter(windowExpiry),
              "Attempted to set a processing-time timer with an output timestamp of %s that is"
                  + " after the expiration of window %s",
              outputTimestamp,
              windowExpiry);
        }
      } else {
        outputTimestamp = BoundedWindow.TIMESTAMP_MAX_VALUE.plus(Duration.millis(1));
      }
    }

    /**
     * Sets the timer for the target time without checking anything about whether it is a reasonable
     * thing to do. For example, absolute processing time timers are not really sensible since the
     * user has no way to compute a good choice of time.
     */
    private void setUnderlyingTimer() {
      timerInternals.setTimer(
          namespace, timerId, timerFamilyId, target, outputTimestamp, spec.getTimeDomain());
    }

    @Override
    public Instant getCurrentRelativeTime() {
      return getCurrentTime(spec.getTimeDomain());
    }

    private Instant getCurrentTime(TimeDomain timeDomain) {
      switch (timeDomain) {
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

  private class TimerInternalsTimerMap implements TimerMap {

    Map<String, Timer> timers = new HashMap<>();
    private final TimerInternals timerInternals;
    private final BoundedWindow window;
    private final StateNamespace namespace;
    private final TimerSpec spec;
    private final Instant elementInputTimestamp;
    private final String timerFamilyId;

    public TimerInternalsTimerMap(
        String timerFamilyId,
        BoundedWindow window,
        StateNamespace namespace,
        TimerSpec spec,
        Instant elementInputTimestamp,
        TimerInternals timerInternals) {
      this.window = window;
      this.namespace = namespace;
      this.spec = spec;
      this.elementInputTimestamp = elementInputTimestamp;
      this.timerInternals = timerInternals;
      this.timerFamilyId = timerFamilyId;
    }

    @Override
    public void set(String timerId, Instant absoluteTime) {
      Timer timer =
          new TimerInternalsTimer(
              window,
              namespace,
              timerId,
              timerFamilyId,
              spec,
              elementInputTimestamp,
              timerInternals);
      timer.set(absoluteTime);
      timers.put(timerId, timer);
    }

    @Override
    public Timer get(String timerId) {
      if (timers.get(timerId) == null) {
        Timer timer =
            new TimerInternalsTimer(
                window,
                namespace,
                timerId,
                timerFamilyId,
                spec,
                elementInputTimestamp,
                timerInternals);
        timers.put(timerId, timer);
      }
      return timers.get(timerId);
    }
  }
}
