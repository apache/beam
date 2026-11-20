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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.beam.sdk.util.OutputBuilderSupplier;
import org.apache.beam.sdk.util.OutputBuilderSuppliers;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.util.WindowedValueReceiver;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
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
public class SimpleDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  private static final long MIN_TIMESTAMP_MILLIS = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
  private static final long MAX_TIMESTAMP_MILLIS = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
  private static final int MAX_INLINE_EXTRA_TAGS = 8;

  private final PipelineOptions options;

  /** The {@link DoFn} being run. */
  private final DoFn<InputT, OutputT> fn;

  /** The {@link DoFnInvoker} being run. */
  private final DoFnInvoker<InputT, OutputT> invoker;

  private final SideInputReader sideInputReader;
  final WindowedValueMultiReceiver outputManager;

  final TupleTag<OutputT> mainOutputTag;

  /** The set of known output tags. */
  final Set<TupleTag<?>> outputTags;

  /**
   * Deduplicated non-main output tags in declaration order when {@code 1 <= count <=
   * MAX_INLINE_EXTRA_TAGS}; {@code null} for single-output or large fan-out {@code DoFn}s.
   */
  final TupleTag<?> @Nullable [] extraOutputTags;

  private final boolean observesWindow;

  private final DoFnSignature signature;

  private final Coder<BoundedWindow> windowCoder;

  private final Duration allowedLateness;

  private final Duration allowedTimestampSkew;

  private final long allowedTimestampSkewMillis;

  // Because of setKey(Object), we really must refresh stateInternals() at each access
  private final StepContext stepContext;

  final @Nullable SchemaCoder<OutputT> mainOutputSchemaCoder;

  final @Nullable Map<TupleTag<?>, Coder<?>> outputCoders;

  private final @Nullable DoFnSchemaInformation doFnSchemaInformation;

  private final Map<String, PCollectionView<?>> sideInputMapping;

  private @Nullable DoFnProcessContext reusableProcessContext;

  /** Constructor. */
  @SuppressWarnings("deprecation") // Allowed Skew is deprecated for users, but must be respected
  public SimpleDoFnRunner(
      PipelineOptions options,
      DoFn<InputT, OutputT> fn,
      SideInputReader sideInputReader,
      WindowedValueMultiReceiver outputManager,
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
    if (additionalOutputTags.isEmpty()) {
      this.outputTags = Collections.singleton(mainOutputTag);
      this.extraOutputTags = null;
    } else {
      Set<TupleTag<?>> tags = Sets.newHashSetWithExpectedSize(1 + additionalOutputTags.size());
      tags.add(mainOutputTag);
      List<TupleTag<?>> orderedExtras = new ArrayList<>(additionalOutputTags.size());
      for (TupleTag<?> tag : additionalOutputTags) {
        if (tags.add(tag)) {
          orderedExtras.add(tag);
        }
      }
      this.outputTags = tags;
      if (orderedExtras.isEmpty() || orderedExtras.size() > MAX_INLINE_EXTRA_TAGS) {
        this.extraOutputTags = null;
      } else {
        this.extraOutputTags = orderedExtras.toArray(new TupleTag<?>[0]);
      }
    }
    this.stepContext = stepContext;

    // This is a cast of an _invariant_ coder. But we are assured by pipeline validation
    // that it really is the coder for whatever BoundedWindow subclass is provided
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> untypedCoder =
        (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    this.windowCoder = untypedCoder;
    this.allowedLateness = windowingStrategy.getAllowedLateness();
    Duration rawSkew = fn.getAllowedTimestampSkew();
    this.allowedTimestampSkew = rawSkew != null ? rawSkew : Duration.ZERO;
    this.allowedTimestampSkewMillis = this.allowedTimestampSkew.getMillis();
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
    } catch (Exception e) {
      throw wrapUserCodeException(e);
    }
  }

  @Override
  public void processElement(WindowedValue<InputT> compressedElem) {
    Context openTelemetryContext = compressedElem.getOpenTelemetryContext();
    if (openTelemetryContext == null || openTelemetryContext == Context.current()) {
      processElementInternal(compressedElem);
    } else {
      processElementWithContext(compressedElem, openTelemetryContext);
    }
  }

  private void processElementWithContext(
      WindowedValue<InputT> compressedElem, Context openTelemetryContext) {
    try (Scope ignore = openTelemetryContext.makeCurrent()) {
      processElementInternal(compressedElem);
    }
  }

  private void processElementInternal(WindowedValue<InputT> compressedElem) {
    if (!observesWindow || WindowedValues.singleWindowOrNull(compressedElem) != null) {
      invokeProcessElement(compressedElem);
    } else {
      explodeAndInvokeProcessElement(compressedElem);
    }
  }

  private void explodeAndInvokeProcessElement(WindowedValue<InputT> compressedElem) {
    for (WindowedValue<InputT> elem : compressedElem.explodeWindows()) {
      invokeProcessElement(elem);
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
      TimeDomain timeDomain,
      CausedByDrain causedByDrain) {
    Preconditions.checkNotNull(outputTimestamp, "outputTimestamp");

    OnTimerArgumentProvider<KeyT> argumentProvider =
        new OnTimerArgumentProvider<>(
            timerId, key, window, timestamp, outputTimestamp, timeDomain, causedByDrain);
    invoker.invokeOnTimer(timerId, timerFamilyId, argumentProvider);
  }

  private void invokeProcessElement(WindowedValue<InputT> elem) {
    // This can contain user code. Wrap it in case it throws an exception.
    DoFnProcessContext context = reusableProcessContext;
    if (context == null) {
      context = initReusableProcessContext();
    }
    if (context.elem == null) {
      context.setElement(elem);
      try {
        invoker.invokeProcessElement(context);
      } catch (Exception e) {
        throw wrapUserCodeException(e);
      } finally {
        context.clearElement();
      }
    } else {
      invokeProcessElementReentrant(elem);
    }
  }

  private DoFnProcessContext initReusableProcessContext() {
    DoFnProcessContext context = new DoFnProcessContext();
    reusableProcessContext = context;
    return context;
  }

  private void invokeProcessElementReentrant(WindowedValue<InputT> elem) {
    DoFnProcessContext reentrantContext = new DoFnProcessContext(elem);
    try {
      invoker.invokeProcessElement(reentrantContext);
    } catch (Exception e) {
      throw wrapUserCodeException(e);
    } finally {
      reentrantContext.clearElement();
    }
  }

  @Override
  public void finishBundle() {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      invoker.invokeFinishBundle(new DoFnFinishBundleArgumentProvider());
    } catch (Exception e) {
      throw wrapUserCodeException(e);
    }
  }

  @Override
  public <KeyT extends @Nullable Object> void finishKey(KeyT key) {}

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    invoker.invokeOnWindowExpiration(
        new OnWindowExpirationArgumentProvider<>(window, timestamp, key));
  }

  private RuntimeException wrapUserCodeException(Exception e) {
    throw UserCodeException.wrapIf(!isSystemDoFn(), e);
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

  void checkSameElementTimestamp(Instant elemTimestamp) {
    long skewMillis = allowedTimestampSkewMillis;
    if (skewMillis == 0L) {
      if (elemTimestamp.getMillis() <= MAX_TIMESTAMP_MILLIS) {
        return;
      }
    } else if (skewMillis > 0L) {
      long tsMillis = elemTimestamp.getMillis();
      if (tsMillis >= MIN_TIMESTAMP_MILLIS && tsMillis <= MAX_TIMESTAMP_MILLIS) {
        return;
      }
    }
    checkTimestampSlowPath(elemTimestamp, elemTimestamp);
  }

  void checkTimestamp(Instant elemTimestamp, Instant timestamp) {
    long tsMillis = timestamp.getMillis();
    long skewMillis = allowedTimestampSkewMillis;
    if (skewMillis >= 0L) {
      long elemMillis = elemTimestamp.getMillis();
      long lowerBoundMillis =
          elemMillis < Long.MIN_VALUE + skewMillis ? MIN_TIMESTAMP_MILLIS : elemMillis - skewMillis;
      if (tsMillis >= lowerBoundMillis && tsMillis <= MAX_TIMESTAMP_MILLIS) {
        return;
      }
    }
    checkTimestampSlowPath(elemTimestamp, timestamp);
  }

  private void checkTimestampSlowPath(Instant elemTimestamp, Instant timestamp) {
    Instant lowerBound;
    try {
      lowerBound = elemTimestamp.minus(allowedTimestampSkew);
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
              allowedTimestampSkewMillis >= Integer.MAX_VALUE
                  ? allowedTimestampSkew
                  : PeriodFormat.getDefault().print(allowedTimestampSkew.toPeriod()),
              BoundedWindow.TIMESTAMP_MAX_VALUE));
    }
  }

  @SuppressWarnings("ReferenceEquality")
  <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedElem) {
    if (tag != mainOutputTag) {
      validateSideOutputTag(tag);
    }
    outputManager.output(tag, windowedElem);
  }

  @SuppressWarnings("ReferenceEquality")
  private void validateSideOutputTag(TupleTag<?> tag) {
    TupleTag<?> @Nullable [] extraTags = this.extraOutputTags;
    if (extraTags != null) {
      for (int i = 0; i < extraTags.length; i++) {
        if (extraTags[i] == tag) {
          return;
        }
      }
    }
    checkArgument(outputTags.contains(tag), "Unknown output tag %s", tag);
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

    @Override
    public BundleFinalizer bundleFinalizer() {
      return stepContext.bundleFinalizer();
    }
  }

  /** An {@link DoFnInvoker.ArgumentProvider} for {@link DoFn.FinishBundle @FinishBundle}. */
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
        outputWindowedValue(tag, WindowedValues.of(output, timestamp, window, PaneInfo.NO_FIRING));
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

    @Override
    public BundleFinalizer bundleFinalizer() {
      return stepContext.bundleFinalizer();
    }
  }

  /**
   * A concrete implementation of {@link DoFn.ProcessContext} used for running a {@link DoFn} over a
   * single element. A single instance is reused across non-reentrant {@code @ProcessElement}
   * invocations via {@link #setElement(WindowedValue)} / {@link #clearElement()}; callers accessing
   * a retained context or receiver after {@code @ProcessElement} returns fail fast with {@link
   * IllegalStateException}.
   */
  private final class DoFnProcessContext extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT>, OutputBuilderSupplier {
    @Nullable WindowedValue<InputT> elem;

    /** Lazily initialized; should only be accessed via {@link #getNamespace()}. */
    private @Nullable StateNamespace namespace;

    private @Nullable DirectOutputReceiver<OutputT> cachedOutputReceiver;
    private @Nullable OutputReceiver<Row> cachedOutputRowReceiver;
    private @Nullable CachedMultiOutputReceiver cachedMultiOutputReceiver;

    DoFnProcessContext() {
      fn.super();
    }

    DoFnProcessContext(WindowedValue<InputT> elem) {
      fn.super();
      this.elem = elem;
    }

    void setElement(WindowedValue<InputT> elem) {
      this.elem = elem;
    }

    void clearElement() {
      this.elem = null;
      if (this.namespace != null) {
        this.namespace = null;
      }
    }

    WindowedValue<InputT> currentElem() {
      return checkStateNotNull(elem, "DoFnProcessContext accessed outside of @ProcessElement");
    }

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

    @Override
    public <T> WindowedValues.Builder<T> builder(T value) {
      return WindowedValues.builder(currentElem(), value);
    }

    @Override
    public <T> WindowedValue<T> build(T value) {
      return WindowedValues.ofWithCurrentContext(currentElem(), value);
    }

    @Override
    public <T> WindowedValue<T> buildWithTimestamp(T value, Instant timestamp) {
      return WindowedValues.ofWithCurrentContext(currentElem(), value, timestamp);
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public InputT element() {
      return currentElem().getValue();
    }

    @Override
    public CausedByDrain causedByDrain() {
      return currentElem().causedByDrain();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      checkNotNull(view, "View passed to sideInput cannot be null");
      BoundedWindow window = window();
      return SimpleDoFnRunner.this.sideInput(
          view, view.getWindowMappingFn().getSideInputWindow(window));
    }

    @Override
    public PaneInfo pane() {
      return currentElem().getPaneInfo();
    }

    @Override
    public void output(OutputT output) {
      checkNotNull(mainOutputTag, "Tag passed to output cannot be null");
      outputManager.output(mainOutputTag, currentElem().withValue(output));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputWithTimestamp(mainOutputTag, output, timestamp);
    }

    @Override
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      outputWindowedValue(mainOutputTag, output, timestamp, windows, paneInfo);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to output cannot be null");
      SimpleDoFnRunner.this.outputWindowedValue(tag, currentElem().withValue(output));
    }

    @Override
    public void outputWindowedValue(WindowedValue<OutputT> windowedValue) {
      outputWindowedValue(mainOutputTag, windowedValue);
    }

    @Override
    public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
      checkTimestamp(currentElem().getTimestamp(), windowedValue.getTimestamp());
      SimpleDoFnRunner.this.outputWindowedValue(tag, windowedValue);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to outputWithTimestamp cannot be null");
      WindowedValue<InputT> current = currentElem();
      checkTimestamp(current.getTimestamp(), timestamp);
      SimpleDoFnRunner.this.outputWindowedValue(
          tag, WindowedValues.ofWithCurrentContext(current, output, timestamp));
    }

    @Override
    public <T> void outputWindowedValue(
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      WindowedValue<InputT> current = currentElem();
      checkTimestamp(current.getTimestamp(), timestamp);
      SimpleDoFnRunner.this.outputWindowedValue(
          tag, WindowedValues.ofWithCurrentContext(current, output, timestamp, windows, paneInfo));
    }

    @Override
    public Instant timestamp() {
      return currentElem().getTimestamp();
    }

    @Override
    public @Nullable String currentRecordId() {
      return currentElem().getRecordId();
    }

    @Override
    public @Nullable Long currentRecordOffset() {
      return currentElem().getRecordOffset();
    }

    @Override
    public ValueKind valueKind() {
      return currentElem().getValueKind();
    }

    public Collection<? extends BoundedWindow> windows() {
      return currentElem().getWindows();
    }

    @Override
    public BoundedWindow window() {
      WindowedValue<InputT> current = currentElem();
      @Nullable BoundedWindow single = WindowedValues.singleWindowOrNull(current);
      if (single != null) {
        return single;
      }
      return Iterables.getOnlyElement(current.getWindows());
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
    public @Nullable Object sideInput(String tagId) {
      PCollectionView<?> view =
          checkStateNotNull(sideInputMapping.get(tagId), "Side input tag %s not found", tagId);
      return sideInput(view);
    }

    @Override
    public Object schemaElement(int index) {
      checkStateNotNull(
          doFnSchemaInformation,
          "attempt to access element via schema when no schema information provided");

      SerializableFunction<InputT, Object> converter =
          (SerializableFunction<InputT, Object>)
              doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(element());
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public @Nullable String currentRecordId(DoFn<InputT, OutputT> doFn) {
      return currentRecordId();
    }

    @Override
    public @Nullable Long currentRecordOffset(DoFn<InputT, OutputT> doFn) {
      return currentRecordOffset();
    }

    @Override
    public Instant fireTimestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access fire timestamp outside of @OnTimer method.");
    }

    @Override
    public CausedByDrain causedByDrain(DoFn<InputT, OutputT> doFn) {
      return currentElem().causedByDrain();
    }

    @Override
    public ValueKind valueKind(DoFn<InputT, OutputT> doFn) {
      return currentElem().getValueKind();
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
      return getOrCreateMainOutputReceiver();
    }

    private DirectOutputReceiver<OutputT> getOrCreateMainOutputReceiver() {
      DirectOutputReceiver<OutputT> receiver = cachedOutputReceiver;
      if (receiver == null) {
        receiver = new DirectOutputReceiver<>(mainOutputTag, outputManager);
        cachedOutputReceiver = receiver;
      }
      return receiver;
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      OutputReceiver<Row> receiver = cachedOutputRowReceiver;
      if (receiver == null) {
        checkStateNotNull(
            mainOutputSchemaCoder, "cannot provide row receiver without schema coder");
        receiver =
            DoFnOutputReceivers.rowReceiver(this, this, mainOutputTag, mainOutputSchemaCoder);
        cachedOutputRowReceiver = receiver;
      }
      return receiver;
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      CachedMultiOutputReceiver receiver = cachedMultiOutputReceiver;
      if (receiver == null) {
        receiver = new CachedMultiOutputReceiver(mainOutputTag, getOrCreateMainOutputReceiver());
        cachedMultiOutputReceiver = receiver;
      }
      return receiver;
    }

    private final class DirectOutputReceiver<T>
        implements OutputReceiver<T>, WindowedValueReceiver<T> {
      private final TupleTag<T> tag;
      private final WindowedValueMultiReceiver receiverOutputManager;

      DirectOutputReceiver(TupleTag<T> tag, WindowedValueMultiReceiver receiverOutputManager) {
        this.tag = tag;
        this.receiverOutputManager = receiverOutputManager;
      }

      @Override
      public void output(T output) {
        WindowedValue<InputT> current = currentElem();
        checkSameElementTimestamp(current.getTimestamp());
        receiverOutputManager.output(tag, WindowedValues.ofWithCurrentContext(current, output));
      }

      @Override
      public void outputWithTimestamp(T output, Instant timestamp) {
        WindowedValue<InputT> current = currentElem();
        checkTimestamp(current.getTimestamp(), timestamp);
        receiverOutputManager.output(
            tag, WindowedValues.ofWithCurrentContext(current, output, timestamp));
      }

      @Override
      public void outputWindowedValue(
          T value,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo paneInfo) {
        WindowedValue<InputT> current = currentElem();
        checkTimestamp(current.getTimestamp(), timestamp);
        receiverOutputManager.output(
            tag, WindowedValues.ofWithCurrentContext(current, value, timestamp, windows, paneInfo));
      }

      @Override
      public void outputWindowedValue(
          T value,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo paneInfo,
          ValueKind valueKind) {
        WindowedValue<InputT> current = currentElem();
        checkTimestamp(current.getTimestamp(), timestamp);
        receiverOutputManager.output(
            tag,
            WindowedValues.ofWithCurrentContext(
                current, value, timestamp, windows, paneInfo, valueKind));
      }

      @Override
      public OutputBuilder<T> builder(T value) {
        return WindowedValues.builder(currentElem(), value).setReceiver(this);
      }

      @Override
      public void output(WindowedValue<T> windowedValue) {
        checkTimestamp(currentElem().getTimestamp(), windowedValue.getTimestamp());
        receiverOutputManager.output(tag, windowedValue);
      }
    }

    private final class CachedMultiOutputReceiver implements MultiOutputReceiver {
      private final @Nullable TupleTag<OutputT> mainTag;
      private final DirectOutputReceiver<OutputT> mainReceiver;
      private @Nullable TupleTag<?> altMainTag;
      private final @Nullable TupleTag<?> singleExtraTag;
      private final @Nullable DirectOutputReceiver<?> singleExtraReceiver;
      private @Nullable TupleTag<?> altSingleExtraTag;
      private final TupleTag<?> @Nullable [] extraTags;
      private final DirectOutputReceiver<?> @Nullable [] extraReceivers;
      private TupleTag<?> @Nullable [] extraTagAliases;
      private final boolean hasLargeFanOut;
      private @Nullable Map<TupleTag<?>, DirectOutputReceiver<?>> lazyExtraReceivers;
      private @Nullable Map<TupleTag<?>, OutputReceiver<Row>> rowReceivers;

      @SuppressWarnings("unchecked")
      CachedMultiOutputReceiver(
          @Nullable TupleTag<OutputT> mainTag, DirectOutputReceiver<OutputT> mainReceiver) {
        this.mainTag = mainTag;
        this.mainReceiver = mainReceiver;
        TupleTag<?> @Nullable [] runnerExtraTags = extraOutputTags;
        if (runnerExtraTags == null) {
          this.singleExtraTag = null;
          this.singleExtraReceiver = null;
          this.extraTags = null;
          this.extraReceivers = null;
          this.hasLargeFanOut = outputTags.size() - 1 > MAX_INLINE_EXTRA_TAGS;
        } else if (runnerExtraTags.length == 1) {
          TupleTag<?> onlyExtra = runnerExtraTags[0];
          this.singleExtraTag = onlyExtra;
          this.singleExtraReceiver =
              new DirectOutputReceiver<>((TupleTag<Object>) onlyExtra, outputManager);
          this.extraTags = null;
          this.extraReceivers = null;
          this.hasLargeFanOut = false;
        } else {
          this.singleExtraTag = null;
          this.singleExtraReceiver = null;
          @SuppressWarnings("rawtypes")
          DirectOutputReceiver<?>[] receivers = new DirectOutputReceiver[runnerExtraTags.length];
          for (int i = 0; i < runnerExtraTags.length; i++) {
            receivers[i] =
                new DirectOutputReceiver<>((TupleTag<Object>) runnerExtraTags[i], outputManager);
          }
          this.extraTags = runnerExtraTags;
          this.extraReceivers = receivers;
          this.hasLargeFanOut = false;
        }
      }

      @Override
      @SuppressWarnings({"unchecked", "ReferenceEquality"})
      public <T> OutputReceiver<T> get(TupleTag<T> tag) {
        if (tag == null || tag == mainTag || tag == altMainTag) {
          return (OutputReceiver<T>) mainReceiver;
        }
        DirectOutputReceiver<?> onlyExtra = this.singleExtraReceiver;
        if (onlyExtra != null && (tag == singleExtraTag || tag == altSingleExtraTag)) {
          return (OutputReceiver<T>) onlyExtra;
        }
        TupleTag<?> @Nullable [] tags = this.extraTags;
        DirectOutputReceiver<?> @Nullable [] receivers = this.extraReceivers;
        if (tags != null && receivers != null) {
          for (int i = 0; i < tags.length; i++) {
            if (tags[i] == tag) {
              return (OutputReceiver<T>) receivers[i];
            }
          }
          TupleTag<?> @Nullable [] aliases = this.extraTagAliases;
          if (aliases != null) {
            for (int i = 0; i < aliases.length; i++) {
              if (aliases[i] == tag) {
                return (OutputReceiver<T>) receivers[i];
              }
            }
          }
        }
        return getSlowPath(tag);
      }

      @SuppressWarnings("unchecked")
      private <T> OutputReceiver<T> getSlowPath(TupleTag<T> tag) {
        if (hasLargeFanOut) {
          Map<TupleTag<?>, DirectOutputReceiver<?>> map = this.lazyExtraReceivers;
          if (map != null) {
            DirectOutputReceiver<?> cached = map.get(tag);
            if (cached != null) {
              return (OutputReceiver<T>) cached;
            }
          }
        }
        if (Objects.equals(mainTag, tag)) {
          altMainTag = tag;
          return (OutputReceiver<T>) mainReceiver;
        }
        DirectOutputReceiver<?> onlyExtra = this.singleExtraReceiver;
        if (onlyExtra != null && Objects.equals(singleExtraTag, tag)) {
          altSingleExtraTag = tag;
          return (OutputReceiver<T>) onlyExtra;
        }
        TupleTag<?> @Nullable [] tags = this.extraTags;
        DirectOutputReceiver<?> @Nullable [] receivers = this.extraReceivers;
        if (tags != null && receivers != null) {
          for (int i = 0; i < tags.length; i++) {
            if (Objects.equals(tags[i], tag)) {
              TupleTag<?> @Nullable [] aliases = this.extraTagAliases;
              if (aliases == null) {
                aliases = new TupleTag<?>[tags.length];
                this.extraTagAliases = aliases;
              }
              aliases[i] = tag;
              return (OutputReceiver<T>) receivers[i];
            }
          }
        } else if (hasLargeFanOut && outputTags.contains(tag)) {
          Map<TupleTag<?>, DirectOutputReceiver<?>> map = this.lazyExtraReceivers;
          if (map == null) {
            map = new HashMap<>();
            this.lazyExtraReceivers = map;
          }
          DirectOutputReceiver<T> created = new DirectOutputReceiver<>(tag, outputManager);
          map.put(tag, created);
          return created;
        }
        return DoFnOutputReceivers.windowedReceiver(
            DoFnProcessContext.this, DoFnProcessContext.this, tag);
      }

      @Override
      @SuppressWarnings({"unchecked", "ReferenceEquality"})
      public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
        Map<TupleTag<?>, Coder<?>> coders = checkNotNull(outputCoders);
        if (tag == mainTag && mainOutputSchemaCoder != null) {
          return outputRowReceiver(fn);
        }
        Map<TupleTag<?>, OutputReceiver<Row>> cache = this.rowReceivers;
        if (cache != null) {
          OutputReceiver<Row> cached = cache.get(tag);
          if (cached != null) {
            return cached;
          }
        }
        Coder<T> outputCoder = (Coder<T>) coders.get(tag);
        checkStateNotNull(outputCoder, "No output tag for %s ", tag);
        Preconditions.checkState(
            outputCoder instanceof SchemaCoder,
            "Output with tag %s must have a schema in order to call getRowReceiver",
            tag);
        OutputReceiver<Row> receiver =
            DoFnOutputReceivers.rowReceiver(
                DoFnProcessContext.this,
                DoFnProcessContext.this,
                tag,
                (SchemaCoder<T>) outputCoder);
        if (cache == null) {
          cache = new HashMap<>(4);
          this.rowReceivers = cache;
        }
        cache.put(tag, receiver);
        return receiver;
      }
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
    public DoFn<InputT, OutputT>.OnWindowExpirationContext onWindowExpirationContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnWindowExpirationContext outside of @OnWindowExpiration methods.");
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
        DoFnSignature.StateDeclaration stateDeclaration =
            checkStateNotNull(
                signature.stateDeclarations().get(stateId), "state not found: %s", stateId);

        StateSpec<?> spec =
            checkStateNotNull(
                (StateSpec<?>) stateDeclaration.field().get(fn),
                "Field %s corresponding to state id %s contained null",
                stateDeclaration.field(),
                stateId);

        @NonNull
        @Initialized // unclear why checkerframework needs this help
        State state =
            stepContext
                .stateInternals()
                .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec<?>) spec));
        if (alwaysFetched) {
          return (State) ((ReadableState<?>) state).readLater();
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
        DoFnSignature.TimerDeclaration timerDeclaration =
            checkStateNotNull(
                signature.timerDeclarations().get(timerId), "timer not found: %s", timerId);
        TimerSpec spec =
            (TimerSpec)
                checkStateNotNull(
                    timerDeclaration.field().get(fn),
                    "Field %s corresponding to timer id %s contained null",
                    timerDeclaration.field(),
                    timerId);
        return new TimerInternalsTimer(
            window(), getNamespace(), timerId, spec, timestamp(), stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      try {
        DoFnSignature.TimerFamilyDeclaration timerFamilyDeclaration =
            checkStateNotNull(
                signature.timerFamilyDeclarations().get(timerFamilyId),
                "timer family not found: %s",
                timerFamilyId);

        TimerSpec spec =
            (TimerSpec)
                checkStateNotNull(
                    timerFamilyDeclaration.field().get(fn),
                    "Field %s corresponding to timer family id %s contained null",
                    timerFamilyDeclaration.field(),
                    timerFamilyId);
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
    private final CausedByDrain causedByDrain;
    private final OutputBuilderSupplier builderSupplier;

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
        TimeDomain timeDomain,
        CausedByDrain causedByDrain) {
      fn.super();
      this.timerId = timerId;
      this.window = window;
      this.fireTimestamp = fireTimestamp;
      this.timestamp = timestamp;
      this.timeDomain = timeDomain;
      this.key = key;
      this.causedByDrain = causedByDrain;
      this.builderSupplier =
          OutputBuilderSuppliers.supplierForElement(
              WindowedValues.builder()
                  .setValue(null)
                  .setTimestamp(timestamp)
                  .setWindow(window)
                  .setPaneInfo(PaneInfo.NO_FIRING)
                  .setCausedByDrain(causedByDrain));
    }

    @Override
    public Instant timestamp() {
      return timestamp;
    }

    @Override
    public CausedByDrain causedByDrain() {
      return causedByDrain;
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
    public @Nullable Object sideInput(String tagId) {
      PCollectionView<?> view =
          checkStateNotNull(sideInputMapping.get(tagId), "Side input tag %s not found", tagId);
      return sideInput(view);
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      checkNotNull(view, "View passed to sideInput cannot be null");
      return SimpleDoFnRunner.this.sideInput(
          view, view.getWindowMappingFn().getSideInputWindow(window()));
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
    public @Nullable String currentRecordId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access record id outside of @ProcessElement method.");
    }

    @Override
    public @Nullable Long currentRecordOffset(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access record offset outside of @ProcessElement method.");
    }

    @Override
    public Instant fireTimestamp(DoFn<InputT, OutputT> doFn) {
      return fireTimestamp();
    }

    @Override
    public CausedByDrain causedByDrain(DoFn<InputT, OutputT> doFn) {
      return causedByDrain;
    }

    @Override
    public ValueKind valueKind(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("ValueKind parameters are not supported.");
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
      return DoFnOutputReceivers.windowedReceiver(this, builderSupplier, mainOutputTag);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      checkStateNotNull(mainOutputSchemaCoder, "cannot provide row receiver without schema coder");
      return DoFnOutputReceivers.rowReceiver(
          this, builderSupplier, mainOutputTag, mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      // ... what to doooo 0...
      return DoFnOutputReceivers.windowedMultiReceiver(this, builderSupplier, outputCoders);
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
    public DoFn<InputT, OutputT>.OnWindowExpirationContext onWindowExpirationContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnWindowExpirationContext outside of @OnWindowExpiration methods.");
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
        DoFnSignature.StateDeclaration stateDeclaration =
            checkStateNotNull(
                signature.stateDeclarations().get(stateId), "state not found: %s", stateId);

        StateSpec<?> spec =
            checkStateNotNull(
                (StateSpec<?>) stateDeclaration.field().get(fn),
                "Field %s corresponding to state id %s contained null",
                stateDeclaration.field(),
                stateId);

        @NonNull State state =
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
        DoFnSignature.TimerDeclaration timerDeclaration =
            checkStateNotNull(
                signature.timerDeclarations().get(timerId), "timer not found: %s", timerId);
        TimerSpec spec =
            (TimerSpec)
                checkStateNotNull(
                    timerDeclaration.field().get(fn),
                    "Field %s corresponding to timer id %s contained null",
                    timerDeclaration.field(),
                    timerId);
        return new TimerInternalsTimer(
            window, getNamespace(), timerId, spec, timestamp(), stepContext.timerInternals());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      try {
        DoFnSignature.TimerFamilyDeclaration timerFamilyDeclaration =
            checkStateNotNull(
                signature.timerFamilyDeclarations().get(timerFamilyId),
                "timer family not found: %s",
                timerFamilyId);
        TimerSpec spec =
            (TimerSpec)
                checkStateNotNull(
                    timerFamilyDeclaration.field().get(fn),
                    "Field %s corresponding to timer family id %s contained null",
                    timerFamilyDeclaration.field(),
                    timerFamilyId);
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
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      outputWindowedValue(mainOutputTag, output, timestamp, windows, paneInfo);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkTimestamp(timestamp(), timestamp);
      outputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkTimestamp(timestamp(), timestamp);
      outputWindowedValue(
          tag, output, timestamp, Collections.singleton(window()), PaneInfo.NO_FIRING);
    }

    @Override
    public <T> void outputWindowedValue(
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      checkTimestamp(timestamp(), timestamp);

      builderSupplier
          .builder(output)
          .setTimestamp(timestamp)
          .setWindows(windows)
          .setPaneInfo(paneInfo)
          .setReceiver(wv -> SimpleDoFnRunner.this.outputWindowedValue(tag, wv))
          .output();
    }

    @Override
    public void outputWindowedValue(WindowedValue<OutputT> windowedValue) {
      outputWindowedValue(mainOutputTag, windowedValue);
    }

    @Override
    public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
      checkTimestamp(timestamp(), windowedValue.getTimestamp());
      SimpleDoFnRunner.this.outputWindowedValue(tag, windowedValue);
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      throw new UnsupportedOperationException(
          "Bundle finalization is not supported in OnTimer calls.");
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
    private final OutputBuilderSupplier builderSupplier;

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
      this.builderSupplier =
          OutputBuilderSuppliers.supplierForElement(
              WindowedValues.<Void>builder()
                  .setValue(null)
                  .setWindow(window)
                  .setTimestamp(timestamp)
                  .setPaneInfo(PaneInfo.NO_FIRING));
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
    public @Nullable Object sideInput(String tagId) {
      PCollectionView<?> view =
          checkStateNotNull(sideInputMapping.get(tagId), "Side input tag %s not found", tagId);
      return sideInput(view);
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      checkNotNull(view, "View passed to sideInput cannot be null");
      return SimpleDoFnRunner.this.sideInput(
          view, view.getWindowMappingFn().getSideInputWindow(window()));
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
    public CausedByDrain causedByDrain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("CausedByDrain parameters are not supported.");
    }

    @Override
    public @Nullable String currentRecordId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access record id outside of @ProcessElement method.");
    }

    @Override
    public @Nullable Long currentRecordOffset(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access record offset outside of @ProcessElement method.");
    }

    @Override
    public Instant fireTimestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access fire timestamp outside of @OnTimer method.");
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
    public ValueKind valueKind(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("ValueKind parameters are not supported.");
    }

    @Override
    public KeyT key() {
      return key;
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, builderSupplier, mainOutputTag);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      checkStateNotNull(mainOutputSchemaCoder, "cannot provide row receiver without schema coder");
      return DoFnOutputReceivers.rowReceiver(
          this, builderSupplier, mainOutputTag, mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this, builderSupplier, outputCoders);
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
    public DoFn<InputT, OutputT>.OnWindowExpirationContext onWindowExpirationContext(
        DoFn<InputT, OutputT> doFn) {
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
        DoFnSignature.StateDeclaration stateDeclaration =
            checkStateNotNull(
                signature.stateDeclarations().get(stateId), "state not found: %s", stateId);
        StateSpec<?> spec =
            checkStateNotNull(
                (StateSpec<?>) stateDeclaration.field().get(fn),
                "Field %s corresponding to state id %s contained null",
                stateDeclaration.field(),
                stateId);
        @NonNull
        @Initialized // unclear why checkerframework needs this help
        State state =
            stepContext
                .stateInternals()
                .state(getNamespace(), StateTags.tagForSpec(stateId, (StateSpec<?>) spec));
        if (alwaysFetched) {
          return (State) ((ReadableState<?>) state).readLater();
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
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      outputWindowedValue(mainOutputTag, output, timestamp, windows, paneInfo);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkTimestamp(this.timestamp, timestamp);
      outputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkTimestamp(this.timestamp, timestamp);
      outputWindowedValue(
          tag, output, timestamp, Collections.singleton(window()), PaneInfo.NO_FIRING);
    }

    @Override
    public <T> void outputWindowedValue(
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      checkTimestamp(this.timestamp, timestamp);
      builderSupplier
          .builder(output)
          .setTimestamp(timestamp)
          .setWindows(windows)
          .setPaneInfo(paneInfo)
          .setReceiver(wv -> SimpleDoFnRunner.this.outputWindowedValue(tag, wv))
          .output();
    }

    @Override
    public void outputWindowedValue(WindowedValue<OutputT> windowedValue) {
      outputWindowedValue(mainOutputTag, windowedValue);
    }

    @Override
    public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
      checkTimestamp(this.timestamp, windowedValue.getTimestamp());
      SimpleDoFnRunner.this.outputWindowedValue(tag, windowedValue);
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      throw new UnsupportedOperationException(
          "Bundle finalization is not supported in OnWindowExpiration calls.");
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
    private @MonotonicNonNull Instant target;
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
      checkStateNotNull(target, "attempt to set outputTimestamp before setting target firing time");
      if (outputTimestamp != null) {
        // setting to local var so checkerframework knows that method calls will not mutate it
        Instant timestampToValidate = outputTimestamp;
        Instant lowerBound;
        try {
          lowerBound = elementInputTimestamp.minus(allowedTimestampSkew);
        } catch (ArithmeticException e) {
          lowerBound = BoundedWindow.TIMESTAMP_MIN_VALUE;
        }
        if (timestampToValidate.isBefore(lowerBound)
            || timestampToValidate.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot output timer with output timestamp %s. Output timestamps must be no "
                      + "earlier than the timestamp of the current input or timer (%s) minus the "
                      + "allowed skew (%s) and no later than %s. See the "
                      + "DoFn#getAllowedTimestampSkew() Javadoc for details on changing the "
                      + "allowed skew.",
                  timestampToValidate,
                  elementInputTimestamp,
                  allowedTimestampSkewMillis >= Integer.MAX_VALUE
                      ? allowedTimestampSkew
                      : PeriodFormat.getDefault().print(allowedTimestampSkew.toPeriod()),
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

      // Now it has been set for all cases other than this.noOutputTimestamp == true, and there are
      // further validations
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
      checkStateNotNull(
          outputTimestamp,
          "internal error: null outputTimestamp: must be populated by setAndVerifyOutputTimestamp()");
      checkStateNotNull(
          target,
          "internal error: attempt to set internal timer when target timestamp not yet set");
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
          return checkStateNotNull(
              timerInternals.currentSynchronizedProcessingTime(),
              "internal error: requested SYNCHRONIZED_PROCESSING_TIME but it was null");
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
      return timers.computeIfAbsent(
          timerId,
          id ->
              new TimerInternalsTimer(
                  window,
                  namespace,
                  id,
                  timerFamilyId,
                  spec,
                  elementInputTimestamp,
                  timerInternals));
    }
  }
}
