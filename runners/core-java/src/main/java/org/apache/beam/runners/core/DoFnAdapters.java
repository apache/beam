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

import java.io.IOException;
import org.apache.beam.runners.core.OldDoFn.Context;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.StartBundleContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Utility class containing adapters to/from {@link DoFn} and {@link OldDoFn}.
 *
 * @deprecated This class will go away when we start running {@link DoFn}'s directly (using {@link
 *     DoFnInvoker}) rather than via {@link OldDoFn}.
 */
@Deprecated
public class DoFnAdapters {
  /** Should not be instantiated. */
  private DoFnAdapters() {}

  /** Creates an {@link OldDoFn} that delegates to the {@link DoFn}. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <InputT, OutputT> OldDoFn<InputT, OutputT> toOldDoFn(DoFn<InputT, OutputT> fn) {
    DoFnSignature signature = DoFnSignatures.getSignature((Class) fn.getClass());
    if (signature.processElement().observesWindow()) {
      return new WindowDoFnAdapter<>(fn);
    } else {
      return new SimpleDoFnAdapter<>(fn);
    }
  }

  /**
   * Wraps a {@link DoFn} that doesn't require access to {@link BoundedWindow} as an {@link
   * OldDoFn}.
   */
  private static class SimpleDoFnAdapter<InputT, OutputT> extends OldDoFn<InputT, OutputT> {
    private final DoFn<InputT, OutputT> fn;
    private transient DoFnInvoker<InputT, OutputT> invoker;

    SimpleDoFnAdapter(DoFn<InputT, OutputT> fn) {
      this.fn = fn;
      this.invoker = DoFnInvokers.invokerFor(fn);
    }

    @Override
    public void setup() throws Exception {
      this.invoker.invokeSetup();
    }

    @Override
    public void startBundle(Context c) throws Exception {
      fn.prepareForProcessing();
      invoker.invokeStartBundle(new StartBundleContextAdapter<>(fn, c));
    }

    @Override
    public void finishBundle(Context c) throws Exception {
      invoker.invokeFinishBundle(new FinishBundleContextAdapter<>(fn, c));
    }

    @Override
    public void teardown() throws Exception {
      this.invoker.invokeTeardown();
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      ProcessContextAdapter<InputT, OutputT> adapter = new ProcessContextAdapter<>(fn, c);
      invoker.invokeProcessElement(adapter);
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return fn.getAllowedTimestampSkew();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(fn);
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      this.invoker = DoFnInvokers.invokerFor(fn);
    }
  }

  /** Wraps a {@link DoFn} that requires access to {@link BoundedWindow} as an {@link OldDoFn}. */
  private static class WindowDoFnAdapter<InputT, OutputT> extends SimpleDoFnAdapter<InputT, OutputT>
      implements OldDoFn.RequiresWindowAccess {

    WindowDoFnAdapter(DoFn<InputT, OutputT> fn) {
      super(fn);
    }
  }

  /**
   * Wraps an {@link OldDoFn.Context} as a {@link DoFnInvoker.ArgumentProvider} inside a {@link
   * DoFn.StartBundle} method, which means the extra context is unavailable.
   */
  private static class StartBundleContextAdapter<InputT, OutputT>
      extends DoFn<InputT, OutputT>.StartBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {
    private OldDoFn<InputT, OutputT>.Context context;

    private StartBundleContextAdapter(DoFn<InputT, OutputT> fn, Context context) {
      fn.super();
      this.context = context;
    }

    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public BoundedWindow window() {
      // The OldDoFn doesn't allow us to ask for these outside processElement, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get the window in processElement; elsewhere there is no defined window.");
    }

    @Override
    public StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Can only get a FinishBundleContext in finishBundle");
    }

    @Override
    public ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Can only get a ProcessContext in processElement");
    }

    @Override
    public OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Timers are not supported for OldDoFn");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException("This is a non-splittable DoFn");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException("State is not supported by this runner");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timers are not supported by this runner");
    }
  }

  /**
   * Wraps an {@link OldDoFn.Context} as a {@link DoFnInvoker.ArgumentProvider} inside a {@link
   * DoFn.FinishBundle} method, which means the extra context is unavailable.
   */
  private static class FinishBundleContextAdapter<InputT, OutputT>
      extends DoFn<InputT, OutputT>.FinishBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.Context context;

    private FinishBundleContextAdapter(DoFn<InputT, OutputT> fn, Context context) {
      fn.super();
      this.context = context;
    }

    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public BoundedWindow window() {
      // The OldDoFn doesn't allow us to ask for these outside processElement, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get the window in processElement; elsewhere there is no defined window.");
    }

    @Override
    public StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Can only get a StartBundleContext in startBundle");
    }

    @Override
    public FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Can only get a ProcessContext in processElement");
    }

    @Override
    public OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Timers are not supported for OldDoFn");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException("This is a non-splittable DoFn");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException("State is not supported by this runner");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timers are not supported by this runner");
    }

    @Override
    public void output(
        OutputT output, Instant timestamp, BoundedWindow window) {
      // Not full fidelity conversion. This should be removed as soon as possible.
      context.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void output(
        TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      // Not full fidelity conversion. This should be removed as soon as possible.
      context.outputWithTimestamp(tag, output, timestamp);
    }
  }

  /**
   * Wraps an {@link OldDoFn.ProcessContext} as a {@link DoFnInvoker.ArgumentProvider} method.
   */
  private static class ProcessContextAdapter<InputT, OutputT>
      extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.ProcessContext context;

    private ProcessContextAdapter(
        DoFn<InputT, OutputT> fn, OldDoFn<InputT, OutputT>.ProcessContext context) {
      fn.super();
      this.context = context;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return context.sideInput(view);
    }

    @Override
    public void output(OutputT output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      context.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      context.output(tag, output);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.outputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public InputT element() {
      return context.element();
    }

    @Override
    public Instant timestamp() {
      return context.timestamp();
    }

    @Override
    public PaneInfo pane() {
      return context.pane();
    }

    @Override
    public void updateWatermark(Instant watermark) {
      throw new UnsupportedOperationException("Only splittable DoFn's can use updateWatermark()");
    }

    @Override
    public BoundedWindow window() {
      return context.window();
    }

    @Override
    public StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return null;
    }

    @Override
    public FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
      return null;
    }

    @Override
    public ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Timers are not supported for OldDoFn");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException("This is a non-splittable DoFn");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException("State is not supported by this runner");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timers are not supported by this runner");
    }
  }
}
