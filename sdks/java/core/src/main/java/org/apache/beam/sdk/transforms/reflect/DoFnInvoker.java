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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Interface for invoking the {@code DoFn} processing methods.
 *
 * <p>Instantiating a {@link DoFnInvoker} associates it with a specific {@link DoFn} instance,
 * referred to as the bound {@link DoFn}.
 */
public interface DoFnInvoker<InputT, OutputT> {
  /** Invoke the {@link DoFn.Setup} method on the bound {@link DoFn}. */
  void invokeSetup();

  /** Invoke the {@link DoFn.StartBundle} method on the bound {@link DoFn}. */
  void invokeStartBundle(DoFn<InputT, OutputT>.StartBundleContext c);

  /** Invoke the {@link DoFn.FinishBundle} method on the bound {@link DoFn}. */
  void invokeFinishBundle(DoFn<InputT, OutputT>.FinishBundleContext c);

  /** Invoke the {@link DoFn.Teardown} method on the bound {@link DoFn}. */
  void invokeTeardown();

  /**
   * Invoke the {@link DoFn.ProcessElement} method on the bound {@link DoFn}.
   *
   * @param extra Factory for producing extra parameter objects (such as window), if necessary.
   * @return The {@link DoFn.ProcessContinuation} returned by the underlying method, or {@link
   *     DoFn.ProcessContinuation#stop()} if it returns {@code void}.
   */
  DoFn.ProcessContinuation invokeProcessElement(ArgumentProvider<InputT, OutputT> extra);

  /** Invoke the appropriate {@link DoFn.OnTimer} method on the bound {@link DoFn}. */
  void invokeOnTimer(String timerId, ArgumentProvider<InputT, OutputT> arguments);

  /** Invoke the {@link DoFn.GetInitialRestriction} method on the bound {@link DoFn}. */
  <RestrictionT> RestrictionT invokeGetInitialRestriction(InputT element);

  /**
   * Invoke the {@link DoFn.GetRestrictionCoder} method on the bound {@link DoFn}. Called only
   * during pipeline construction time.
   */
  <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder(CoderRegistry coderRegistry);

  /** Invoke the {@link DoFn.SplitRestriction} method on the bound {@link DoFn}. */
  <RestrictionT> void invokeSplitRestriction(
      InputT element,
      RestrictionT restriction,
      DoFn.OutputReceiver<RestrictionT> restrictionReceiver);

  /** Invoke the {@link DoFn.NewTracker} method on the bound {@link DoFn}. */
  <RestrictionT, TrackerT extends RestrictionTracker<RestrictionT, ?>> TrackerT invokeNewTracker(
      RestrictionT restriction);

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
  interface ArgumentProvider<InputT, OutputT> {
    /**
     * Construct the {@link BoundedWindow} to use within a {@link DoFn} that needs it. This is
     * called if the {@link ProcessElement} method has a parameter of type {@link BoundedWindow}.
     *
     * @return {@link BoundedWindow} of the element currently being processed.
     */
    BoundedWindow window();

    /** Provide {@link PipelineOptions}. */
    PipelineOptions pipelineOptions();

    /**
     * Provide a {@link DoFn.StartBundleContext} to use with the given {@link DoFn}.
     */
    DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link DoFn.FinishBundleContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link DoFn.ProcessContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn);

    /** Provide a {@link DoFn.OnTimerContext} to use with the given {@link DoFn}. */
    DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn);

    /** Provide a link to the input element. */
    InputT element(DoFn<InputT, OutputT> doFn);

    /** Provide a link to the input element timestamp. */
    Instant timestamp(DoFn<InputT, OutputT> doFn);

    /** Provide a link to the time domain for a timer firing.
     */
    TimeDomain timeDomain(DoFn<InputT, OutputT> doFn);

    /**
     * Provide a {@link OutputReceiver<OutputT>} for outputing to the default output.
     */
    OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn);

    /**
     * Provide a {@link MultiOutputReceiver <OutputT>} for outputing to the default output.
     */
    MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn);

    /**
     * If this is a splittable {@link DoFn}, returns the {@link RestrictionTracker} associated with
     * the current {@link ProcessElement} call.
     */
    RestrictionTracker<?, ?> restrictionTracker();

    /** Returns the state cell for the given {@link StateId}. */
    State state(String stateId);

    /** Returns the timer for the given {@link TimerId}. */
    Timer timer(String timerId);
  }

  /**
   * For testing only, this {@link ArgumentProvider} throws {@link UnsupportedOperationException}
   * for all parameters.
   */
  class FakeArgumentProvider<InputT, OutputT> implements ArgumentProvider<InputT, OutputT> {
    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public PipelineOptions pipelineOptions() {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }

    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException(
          String.format(
              "Should never call non-overridden methods of %s",
              FakeArgumentProvider.class.getSimpleName()));
    }
  }
}
