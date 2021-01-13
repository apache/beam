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
package org.apache.beam.sdk.transforms.reflect;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.DoFn.TruncateRestriction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * Interface for invoking the {@code DoFn} processing methods.
 *
 * <p>Instantiating a {@link DoFnInvoker} associates it with a specific {@link DoFn} instance,
 * referred to as the bound {@link DoFn}.
 */
@Internal
public interface DoFnInvoker<InputT, OutputT> {
  /** Invoke the {@link DoFn.Setup} method on the bound {@link DoFn}. */
  void invokeSetup();

  /** Invoke the {@link DoFn.StartBundle} method on the bound {@link DoFn}. */
  void invokeStartBundle(ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.FinishBundle} method on the bound {@link DoFn}. */
  void invokeFinishBundle(ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.Teardown} method on the bound {@link DoFn}. */
  void invokeTeardown();

  /** Invoke the {@link DoFn.OnWindowExpiration} method on the bound {@link DoFn}. */
  void invokeOnWindowExpiration(ArgumentProvider<InputT, OutputT> arguments);

  /**
   * Invoke the {@link DoFn.ProcessElement} method on the bound {@link DoFn}.
   *
   * @param extra Factory for producing extra parameter objects (such as window), if necessary.
   * @return The {@link DoFn.ProcessContinuation} returned by the underlying method, or {@link
   *     DoFn.ProcessContinuation#stop()} if it returns {@code void}.
   */
  DoFn.ProcessContinuation invokeProcessElement(ArgumentProvider<InputT, OutputT> extra);

  /** Invoke the appropriate {@link DoFn.OnTimer} method on the bound {@link DoFn}. */
  void invokeOnTimer(
      String timerId, String timerFamilyId, ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.GetInitialRestriction} method on the bound {@link DoFn}. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <RestrictionT> RestrictionT invokeGetInitialRestriction(
      ArgumentProvider<InputT, OutputT> arguments);

  /**
   * Invoke the {@link DoFn.GetRestrictionCoder} method on the bound {@link DoFn}. Called only
   * during pipeline construction time.
   */
  <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder(CoderRegistry coderRegistry);

  /** Invoke the {@link DoFn.SplitRestriction} method on the bound {@link DoFn}. */
  void invokeSplitRestriction(ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link TruncateRestriction} method on the bound {@link DoFn}. */
  <RestrictionT> TruncateResult<RestrictionT> invokeTruncateRestriction(
      ArgumentProvider<InputT, OutputT> arguments);

  /**
   * Invoke the {@link DoFn.GetSize} method on the bound {@link DoFn}. Falls back to:
   *
   * <ol>
   *   <li>get the work remaining from the {@link RestrictionTracker} if it supports {@link
   *       HasProgress}.
   *   <li>returning the constant {@link 1.0}.
   * </ol>
   */
  double invokeGetSize(ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.NewTracker} method on the bound {@link DoFn}. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <RestrictionT, PositionT> RestrictionTracker<RestrictionT, PositionT> invokeNewTracker(
      ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.NewWatermarkEstimator} method on the bound {@link DoFn}. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <WatermarkEstimatorStateT>
      WatermarkEstimator<WatermarkEstimatorStateT> invokeNewWatermarkEstimator(
          ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.GetInitialWatermarkEstimatorState} method on the bound {@link DoFn}. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <WatermarkEstimatorStateT> WatermarkEstimatorStateT invokeGetInitialWatermarkEstimatorState(
      ArgumentProvider<InputT, OutputT> arguments);

  /**
   * Invoke the {@link DoFn.GetWatermarkEstimatorStateCoder} method on the bound {@link DoFn}.
   * Called only during pipeline construction time.
   */
  <WatermarkEstimatorStateT> Coder<WatermarkEstimatorStateT> invokeGetWatermarkEstimatorStateCoder(
      CoderRegistry coderRegistry);

  /** Get the bound {@link DoFn}. */
  DoFn<InputT, OutputT> getFn();

  /**
   * Interface for runner implementors to provide implementations of extra context information.
   *
   * <p>The methods on this interface are called by {@link DoFnInvoker} before invoking an annotated
   * {@link StartBundle}, {@link ProcessElement} or {@link FinishBundle} method that has indicated
   * it needs the given extra context.
   *
   * <p>In the case of {@link ProcessElement} it is called once per invocation of {@link
   * ProcessElement}.
   */
  @Internal
  interface ArgumentProvider<InputT, OutputT> {
    /**
     * Construct the {@link BoundedWindow} to use within a {@link DoFn} that needs it. This is
     * called if the {@link ProcessElement} method has a parameter of type {@link BoundedWindow}.
     *
     * @return {@link BoundedWindow} of the element currently being processed.
     */
    BoundedWindow window();

    /** Provides a {@link PaneInfo}. */
    PaneInfo paneInfo(DoFn<InputT, OutputT> doFn);

    /** Provide {@link PipelineOptions}. */
    PipelineOptions pipelineOptions();

    /** Provide a {@link DoFn.StartBundleContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link DoFn.FinishBundleContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link DoFn.ProcessContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link DoFn.OnTimerContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn);

    /** Provide a reference to the input element. */
    InputT element(DoFn<InputT, OutputT> doFn);

    /**
     * Provide a reference to the input element key in {@link org.apache.beam.sdk.values.KV} pair.
     */
    Object key();

    /** Provide a reference to the input sideInput with the specified tag. */
    Object sideInput(String tagId);

    /**
     * Provide a reference to the selected schema field corresponding to the input argument
     * specified by index.
     */
    Object schemaElement(int index);

    /** Provide a reference to the input element timestamp. */
    Instant timestamp(DoFn<InputT, OutputT> doFn);

    /** Provide a reference to the time domain for a timer firing. */
    TimeDomain timeDomain(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link OutputReceiver} for outputting to the default output. */
    OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link OutputReceiver} for outputting rows to the default output. */
    OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link MultiOutputReceiver} for outputting to the default output. */
    MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn);

    /**
     * Provide a {@link BundleFinalizer} for being able to register a callback after the bundle has
     * been successfully persisted by the runner.
     */
    BundleFinalizer bundleFinalizer();

    /**
     * If this is a splittable {@link DoFn}, returns the associated restriction with the current
     * call.
     */
    Object restriction();

    /**
     * If this is a splittable {@link DoFn}, returns the associated {@link RestrictionTracker} with
     * the current call.
     */
    RestrictionTracker<?, ?> restrictionTracker();

    /**
     * If this is a splittable {@link DoFn}, returns the associated watermark estimator state with
     * the current call.
     */
    Object watermarkEstimatorState();

    /**
     * If this is a splittable {@link DoFn}, returns the associated {@link WatermarkEstimator} with
     * the current call.
     */
    WatermarkEstimator<?> watermarkEstimator();

    /** Returns the state cell for the given {@link StateId}. */
    State state(String stateId, boolean alwaysFetched);

    /** Returns the timer for the given {@link TimerId}. */
    Timer timer(String timerId);

    /**
     * Returns the timerMap for the given {@link org.apache.beam.sdk.transforms.DoFn.TimerFamily}.
     */
    TimerMap timerFamily(String tagId);

    /**
     * Returns the timer id for the current timer of a {@link
     * org.apache.beam.sdk.transforms.DoFn.TimerFamily}.
     */
    String timerId(DoFn<InputT, OutputT> doFn);
  }

  /**
   * This {@link ArgumentProvider} throws {@link UnsupportedOperationException} for all parameters.
   */
  @Internal
  abstract class BaseArgumentProvider<InputT, OutputT>
      implements ArgumentProvider<InputT, OutputT> {
    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("ProcessContext unsupported in %s", getErrorContext()));
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("Element unsupported in %s", getErrorContext()));
    }

    @Override
    public Object key() {
      throw new UnsupportedOperationException(
          "Cannot access key as parameter outside of @OnTimer method.");
    }

    @Override
    public Object sideInput(String tagId) {
      throw new UnsupportedOperationException(
          String.format("SideInput unsupported in %s", getErrorContext()));
    }

    @Override
    public TimerMap timerFamily(String tagId) {
      throw new UnsupportedOperationException(
          String.format("TimerFamily unsupported in %s", getErrorContext()));
    }

    @Override
    public Object schemaElement(int index) {
      throw new UnsupportedOperationException(
          String.format("Schema element unsupported in %s", getErrorContext()));
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("Timestamp unsupported in %s", getErrorContext()));
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("TimerId unsupported in %s", getErrorContext()));
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("TimeDomain unsupported in %s", getErrorContext()));
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("OutputReceiver unsupported in %s", getErrorContext()));
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("Row OutputReceiver unsupported in %s", getErrorContext()));
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("MultiOutputReceiver unsupported in %s", getErrorContext()));
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException(
          String.format("Restriction unsupported in %s", getErrorContext()));
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          String.format("BoundedWindow unsupported in %s", getErrorContext()));
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("PaneInfo unsupported in %s", getErrorContext()));
    }

    @Override
    public PipelineOptions pipelineOptions() {
      throw new UnsupportedOperationException(
          String.format("PipelineOptions unsupported in %s", getErrorContext()));
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("StartBundleContext unsupported in %s", getErrorContext()));
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("FinishBundleContext unsupported in %s", getErrorContext()));
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format("OnTimerContext unsupported in %s", getErrorContext()));
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      throw new UnsupportedOperationException(
          String.format("State unsupported in %s", getErrorContext()));
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          String.format("Timer unsupported in %s", getErrorContext()));
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException(
          String.format("RestrictionTracker unsupported in %s", getErrorContext()));
    }

    @Override
    public Object watermarkEstimatorState() {
      throw new UnsupportedOperationException(
          String.format("WatermarkEstimatorState unsupported in %s", getErrorContext()));
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      throw new UnsupportedOperationException(
          String.format("WatermarkEstimator unsupported in %s", getErrorContext()));
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      throw new UnsupportedOperationException(
          String.format("BundleFinalizer unsupported in %s", getErrorContext()));
    }

    /**
     * Return a human readable representation of the current call context to be used during error
     * reporting.
     */
    public abstract String getErrorContext();
  }

  /** An {@link ArgumentProvider} that forwards all calls to the supplied {@code delegate}. */
  @Internal
  class DelegatingArgumentProvider<InputT, OutputT> extends BaseArgumentProvider<InputT, OutputT> {
    private final ArgumentProvider<InputT, OutputT> delegate;
    private final String errorContext;

    public DelegatingArgumentProvider(
        ArgumentProvider<InputT, OutputT> delegate, String errorContext) {
      this.delegate = delegate;
      this.errorContext = errorContext;
    }

    @Override
    public BoundedWindow window() {
      return delegate.window();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return delegate.paneInfo(doFn);
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return delegate.pipelineOptions();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return delegate.startBundleContext(doFn);
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return delegate.finishBundleContext(doFn);
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return delegate.processContext(doFn);
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      return delegate.onTimerContext(doFn);
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return delegate.element(doFn);
    }

    @Override
    public Object key() {
      return delegate.key();
    }

    @Override
    public Object sideInput(String tagId) {
      return delegate.sideInput(tagId);
    }

    @Override
    public Object schemaElement(int index) {
      return delegate.schemaElement(index);
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return delegate.timestamp(doFn);
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      return delegate.timeDomain(doFn);
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return delegate.outputReceiver(doFn);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return delegate.outputRowReceiver(doFn);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return delegate.taggedOutputReceiver(doFn);
    }

    @Override
    public Object restriction() {
      return delegate.restriction();
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      return delegate.restrictionTracker();
    }

    @Override
    public Object watermarkEstimatorState() {
      return delegate.watermarkEstimatorState();
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      return delegate.watermarkEstimator();
    }

    @Override
    public State state(String stateId, boolean alwaysFetch) {
      return delegate.state(stateId, alwaysFetch);
    }

    @Override
    public Timer timer(String timerId) {
      return delegate.timer(timerId);
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      return delegate.timerFamily(timerFamilyId);
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      return delegate.timerId(doFn);
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return delegate.bundleFinalizer();
    }

    @Override
    public String getErrorContext() {
      return errorContext;
    }
  }

  /**
   * A fake {@link ArgumentProvider} used during testing. Throws {@link
   * UnsupportedOperationException} for all methods.
   */
  @VisibleForTesting
  @Internal
  class FakeArgumentProvider<InputT, OutputT> extends BaseArgumentProvider<InputT, OutputT> {

    @Override
    public String getErrorContext() {
      return "TestContext";
    }
  }
}
