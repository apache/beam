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
package org.apache.beam.sdk.transforms;

import java.io.IOException;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn.Context;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
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

  /**
   * If this is an {@link OldDoFn} produced via {@link #toOldDoFn}, returns the class of the
   * original {@link DoFn}, otherwise returns {@code fn.getClass()}.
   */
  public static Class<?> getDoFnClass(OldDoFn<?, ?> fn) {
    if (fn instanceof SimpleDoFnAdapter) {
      return ((SimpleDoFnAdapter<?, ?>) fn).fn.getClass();
    } else {
      return fn.getClass();
    }
  }

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

  /** Creates a {@link OldDoFn.ProcessContext} from a {@link DoFn.ProcessContext}. */
  public static <InputT, OutputT> OldDoFn<InputT, OutputT>.ProcessContext adaptProcessContext(
      OldDoFn<InputT, OutputT> fn,
      final DoFn<InputT, OutputT>.ProcessContext c,
      final DoFnInvoker.ArgumentProvider<InputT, OutputT> extra) {
    return fn.new ProcessContext() {
      @Override
      public InputT element() {
        return c.element();
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        return c.sideInput(view);
      }

      @Override
      public Instant timestamp() {
        return c.timestamp();
      }

      @Override
      public BoundedWindow window() {
        return extra.window();
      }

      @Override
      public PaneInfo pane() {
        return c.pane();
      }

      @Override
      public WindowingInternals<InputT, OutputT> windowingInternals() {
        return extra.windowingInternals();
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return c.getPipelineOptions();
      }

      @Override
      public void output(OutputT output) {
        c.output(output);
      }

      @Override
      public void outputWithTimestamp(OutputT output, Instant timestamp) {
        c.outputWithTimestamp(output, timestamp);
      }

      @Override
      public <T> void sideOutput(TupleTag<T> tag, T output) {
        c.sideOutput(tag, output);
      }

      @Override
      public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        c.sideOutputWithTimestamp(tag, output, timestamp);
      }

      @Override
      protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
          String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
        return c.createAggregator(name, combiner);
      }
    };
  }

  /** Creates a {@link OldDoFn.ProcessContext} from a {@link DoFn.ProcessContext}. */
  public static <InputT, OutputT> OldDoFn<InputT, OutputT>.Context adaptContext(
      OldDoFn<InputT, OutputT> fn,
      final DoFn<InputT, OutputT>.Context c) {
    return fn.new Context() {
      @Override
      public PipelineOptions getPipelineOptions() {
        return c.getPipelineOptions();
      }

      @Override
      public void output(OutputT output) {
        c.output(output);
      }

      @Override
      public void outputWithTimestamp(OutputT output, Instant timestamp) {
        c.outputWithTimestamp(output, timestamp);
      }

      @Override
      public <T> void sideOutput(TupleTag<T> tag, T output) {
        c.sideOutput(tag, output);
      }

      @Override
      public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        c.sideOutputWithTimestamp(tag, output, timestamp);
      }

      @Override
      protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
          String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
        return c.createAggregator(name, combiner);
      }
    };
  }

  /**
   * If the fn was created using {@link #toOldDoFn}, returns the original {@link DoFn}. Otherwise,
   * returns {@code null}.
   */
  @Nullable
  public static <InputT, OutputT> DoFn<InputT, OutputT> getDoFn(OldDoFn<InputT, OutputT> fn) {
    if (fn instanceof SimpleDoFnAdapter) {
      return ((SimpleDoFnAdapter<InputT, OutputT>) fn).fn;
    } else {
      return null;
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
      super(fn.aggregators);
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
      invoker.invokeStartBundle(new ContextAdapter<>(fn, c));
    }

    @Override
    public void finishBundle(Context c) throws Exception {
      invoker.invokeFinishBundle(new ContextAdapter<>(fn, c));
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
    protected TypeDescriptor<InputT> getInputTypeDescriptor() {
      return fn.getInputTypeDescriptor();
    }

    @Override
    protected TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return fn.getOutputTypeDescriptor();
    }

    @Override
    Collection<Aggregator<?, ?>> getAggregators() {
      return fn.getAggregators();
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
   * DoFn.StartBundle} or {@link DoFn.FinishBundle} method, which means the extra context is
   * unavailable.
   */
  private static class ContextAdapter<InputT, OutputT> extends DoFn<InputT, OutputT>.Context
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.Context context;

    private ContextAdapter(DoFn<InputT, OutputT> fn, OldDoFn<InputT, OutputT>.Context context) {
      fn.super();
      this.context = context;
      super.setupDelegateAggregators();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
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
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      context.sideOutput(tag, output);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.sideOutputWithTimestamp(tag, output, timestamp);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        String name,
        CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return context.createAggregatorInternal(name, combiner);
    }

    @Override
    public BoundedWindow window() {
      // The OldDoFn doesn't allow us to ask for these outside processElement, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get the window in processElement; elsewhere there is no defined window.");
    }

    @Override
    public Context context(DoFn<InputT, OutputT> doFn) {
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
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      // The OldDoFn doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get WindowingInternals in processElement");
    }

    @Override
    public DoFn.InputProvider<InputT> inputProvider() {
      throw new UnsupportedOperationException("inputProvider() exists only for testing");
    }

    @Override
    public DoFn.OutputReceiver<OutputT> outputReceiver() {
      throw new UnsupportedOperationException("outputReceiver() exists only for testing");
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
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
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      context.sideOutput(tag, output);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.sideOutputWithTimestamp(tag, output, timestamp);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return context.createAggregatorInternal(name, combiner);
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
    public BoundedWindow window() {
      return context.window();
    }

    @Override
    public Context context(DoFn<InputT, OutputT> doFn) {
      return this;
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
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      return context.windowingInternals();
    }

    @Override
    public DoFn.InputProvider<InputT> inputProvider() {
      throw new UnsupportedOperationException("inputProvider() exists only for testing");
    }

    @Override
    public DoFn.OutputReceiver<OutputT> outputReceiver() {
      throw new UnsupportedOperationException("outputReceiver() exists only for testing");
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
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
