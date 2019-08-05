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
package org.apache.beam.fn.harness;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.fn.harness.DoFnPTransformRunnerFactory.Context;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} specific to integrating with the Fn Api. This is to remove the layers of
 * abstraction caused by StateInternals/TimerInternals since they model state and timer concepts
 * differently.
 */
public class FnApiDoFnRunner<InputT, OutputT>
    implements DoFnPTransformRunnerFactory.DoFnPTransformRunner<InputT> {
  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(PTransformTranslation.PAR_DO_TRANSFORM_URN, new Factory());
    }
  }

  static class Factory<InputT, OutputT>
      extends DoFnPTransformRunnerFactory<
          InputT, InputT, OutputT, FnApiDoFnRunner<InputT, OutputT>> {
    @Override
    public FnApiDoFnRunner<InputT, OutputT> createRunner(Context<InputT, OutputT> context) {
      return new FnApiDoFnRunner<>(context);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final Context<InputT, OutputT> context;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers;
  private FnApiStateAccessor stateAccessor;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final DoFn<InputT, OutputT>.StartBundleContext startBundleContext;
  private final ProcessBundleContext processContext;
  private final OnTimerContext onTimerContext;
  private final DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext;

  /** Only valid during {@link #processElement}, null otherwise. */
  private WindowedValue<InputT> currentElement;

  /** Only valid during {@link #processElement} and {@link #processTimer}, null otherwise. */
  private BoundedWindow currentWindow;

  /** Only valid during {@link #processTimer}, null otherwise. */
  private WindowedValue<KV<Object, Timer>> currentTimer;

  /** Only valid during {@link #processTimer}, null otherwise. */
  private TimeDomain currentTimeDomain;

  private DoFnSchemaInformation doFnSchemaInformation;

  FnApiDoFnRunner(Context<InputT, OutputT> context) {
    this.context = context;

    this.mainOutputConsumers =
        (Collection<FnDataReceiver<WindowedValue<OutputT>>>)
            (Collection) context.localNameToConsumer.get(context.mainOutputTag.getId());
    this.doFnSchemaInformation = ParDoTranslation.getSchemaInformation(context.parDoPayload);
    this.doFnInvoker = DoFnInvokers.invokerFor(context.doFn);
    this.doFnInvoker.invokeSetup();

    this.startBundleContext =
        this.context.doFn.new StartBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return context.pipelineOptions;
          }
        };
    this.processContext = new ProcessBundleContext();
    this.onTimerContext = new OnTimerContext();
    this.finishBundleContext =
        this.context.doFn.new FinishBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return context.pipelineOptions;
          }

          @Override
          public void output(OutputT output, Instant timestamp, BoundedWindow window) {
            outputTo(
                mainOutputConsumers,
                WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
          }

          @Override
          public <T> void output(
              TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
            Collection<FnDataReceiver<WindowedValue<T>>> consumers =
                (Collection) context.localNameToConsumer.get(tag.getId());
            if (consumers == null) {
              throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
            }
            outputTo(consumers, WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
          }
        };
  }

  @Override
  public void startBundle() {
    this.stateAccessor =
        new FnApiStateAccessor(
            context.pipelineOptions,
            context.ptransformId,
            context.processBundleInstructionId,
            context.tagToSideInputSpecMap,
            context.beamFnStateClient,
            context.keyCoder,
            (Coder<BoundedWindow>) context.windowCoder,
            () -> MoreObjects.firstNonNull(currentElement, currentTimer),
            () -> currentWindow);

    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    currentElement = elem;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeProcessElement(processContext);
      }
    } finally {
      currentElement = null;
      currentWindow = null;
    }
  }

  @Override
  public void processTimer(
      String timerId, TimeDomain timeDomain, WindowedValue<KV<Object, Timer>> timer) {
    currentTimer = timer;
    currentTimeDomain = timeDomain;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) timer.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeOnTimer(timerId, onTimerContext);
      }
    } finally {
      currentTimer = null;
      currentTimeDomain = null;
      currentWindow = null;
    }
  }

  @Override
  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(finishBundleContext);

    // TODO: Support caching state data across bundle boundaries.
    this.stateAccessor.finalizeState();
    this.stateAccessor = null;
  }

  /** Outputs the given element to the specified set of consumers wrapping any exceptions. */
  private <T> void outputTo(
      Collection<FnDataReceiver<WindowedValue<T>>> consumers, WindowedValue<T> output) {
    try {
      for (FnDataReceiver<WindowedValue<T>> consumer : consumers) {
        consumer.accept(output);
      }
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  private class FnApiTimer implements org.apache.beam.sdk.state.Timer {
    private final String timerId;
    private final TimeDomain timeDomain;
    private final Instant currentTimestamp;
    private final Duration allowedLateness;
    private final WindowedValue<?> currentElementOrTimer;

    private Duration period = Duration.ZERO;
    private Duration offset = Duration.ZERO;

    FnApiTimer(String timerId, WindowedValue<KV<?, ?>> currentElementOrTimer) {
      this.timerId = timerId;
      this.currentElementOrTimer = currentElementOrTimer;

      TimerDeclaration timerDeclaration = context.doFnSignature.timerDeclarations().get(timerId);
      this.timeDomain =
          DoFnSignatures.getTimerSpecOrThrow(timerDeclaration, context.doFn).getTimeDomain();

      switch (timeDomain) {
        case EVENT_TIME:
          this.currentTimestamp = currentElementOrTimer.getTimestamp();
          break;
        case PROCESSING_TIME:
          this.currentTimestamp = new Instant(DateTimeUtils.currentTimeMillis());
          break;
        case SYNCHRONIZED_PROCESSING_TIME:
          this.currentTimestamp = new Instant(DateTimeUtils.currentTimeMillis());
          break;
        default:
          throw new IllegalArgumentException(String.format("Unknown time domain %s", timeDomain));
      }

      try {
        this.allowedLateness =
            context
                .rehydratedComponents
                .getPCollection(context.pTransform.getInputsOrThrow(timerId))
                .getWindowingStrategy()
                .getAllowedLateness();
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Unable to get allowed lateness for timer %s", timerId));
      }
    }

    @Override
    public void set(Instant absoluteTime) {
      // Verifies that the time domain of this timer is acceptable for absolute timers.
      if (!TimeDomain.EVENT_TIME.equals(timeDomain)) {
        throw new IllegalArgumentException(
            "Can only set relative timers in processing time domain. Use #setRelative()");
      }

      // Ensures that the target time is reasonable. For event time timers this means that the time
      // should be prior to window GC time.
      if (TimeDomain.EVENT_TIME.equals(timeDomain)) {
        Instant windowExpiry = LateDataUtils.garbageCollectionTime(currentWindow, allowedLateness);
        checkArgument(
            !absoluteTime.isAfter(windowExpiry),
            "Attempted to set event time timer for %s but that is after"
                + " the expiration of window %s",
            absoluteTime,
            windowExpiry);
      }

      output(absoluteTime);
    }

    @Override
    public void setRelative() {
      Instant target;
      if (period.equals(Duration.ZERO)) {
        target = currentTimestamp.plus(offset);
      } else {
        long millisSinceStart = currentTimestamp.plus(offset).getMillis() % period.getMillis();
        target =
            millisSinceStart == 0
                ? currentTimestamp
                : currentTimestamp.plus(period).minus(millisSinceStart);
      }
      target = minTargetAndGcTime(target);
      output(target);
    }

    @Override
    public org.apache.beam.sdk.state.Timer offset(Duration offset) {
      this.offset = offset;
      return this;
    }

    @Override
    public org.apache.beam.sdk.state.Timer align(Duration period) {
      this.period = period;
      return this;
    }

    /**
     * For event time timers the target time should be prior to window GC time. So it returns
     * min(time to set, GC Time of window).
     */
    private Instant minTargetAndGcTime(Instant target) {
      if (TimeDomain.EVENT_TIME.equals(timeDomain)) {
        Instant windowExpiry = LateDataUtils.garbageCollectionTime(currentWindow, allowedLateness);
        if (target.isAfter(windowExpiry)) {
          return windowExpiry;
        }
      }
      return target;
    }

    private void output(Instant scheduledTime) {
      Object key = ((KV) currentElementOrTimer.getValue()).getKey();
      Collection<FnDataReceiver<WindowedValue<KV<Object, Timer>>>> consumers =
          (Collection) context.localNameToConsumer.get(timerId);

      outputTo(consumers, currentElementOrTimer.withValue(KV.of(key, Timer.of(scheduledTime))));
    }
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.ProcessElement @ProcessElement}.
   */
  private class ProcessBundleContext extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private ProcessBundleContext() {
      context.doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return pane();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
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
    public Object sideInput(String sideInputTag) {
      throw new UnsupportedOperationException("hello world");
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
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, null);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, null, context.mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this, context.outputCoders);
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
      StateDeclaration stateDeclaration = context.doFnSignature.stateDeclarations().get(stateId);
      checkNotNull(stateDeclaration, "No state declaration found for %s", stateId);
      StateSpec<?> spec;
      try {
        spec = (StateSpec<?>) stateDeclaration.field().get(context.doFn);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return spec.bind(stateId, stateAccessor);
    }

    @Override
    public org.apache.beam.sdk.state.Timer timer(String timerId) {
      checkState(
          currentElement.getValue() instanceof KV,
          "Accessing timer in unkeyed context. Current element is not a KV: %s.",
          currentElement.getValue());

      return new FnApiTimer(timerId, (WindowedValue) currentElement);
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(
              output, currentElement.getTimestamp(), currentWindow, currentElement.getPane()));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, timestamp, currentWindow, currentElement.getPane()));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers,
          WindowedValue.of(
              output, currentElement.getTimestamp(), currentWindow, currentElement.getPane()));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers, WindowedValue.of(output, timestamp, currentWindow, currentElement.getPane()));
    }

    @Override
    public InputT element() {
      return currentElement.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return stateAccessor.get(view, currentWindow);
    }

    @Override
    public Instant timestamp() {
      return currentElement.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return currentElement.getPane();
    }

    @Override
    public void updateWatermark(Instant watermark) {
      throw new UnsupportedOperationException("TODO: Add support for SplittableDoFn");
    }
  }

  /** Provides arguments for a {@link DoFnInvoker} for {@link DoFn.OnTimer @OnTimer}. */
  private class OnTimerContext extends DoFn<InputT, OutputT>.OnTimerContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private OnTimerContext() {
      context.doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access paneInfo outside of @ProcessElement methods.");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
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
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public InputT sideInput(String sideInputTag) {
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
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      return timeDomain();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, null);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, null, context.mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this);
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
      StateDeclaration stateDeclaration = context.doFnSignature.stateDeclarations().get(stateId);
      checkNotNull(stateDeclaration, "No state declaration found for %s", stateId);
      StateSpec<?> spec;
      try {
        spec = (StateSpec<?>) stateDeclaration.field().get(context.doFn);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return spec.bind(stateId, stateAccessor);
    }

    @Override
    public org.apache.beam.sdk.state.Timer timer(String timerId) {
      checkState(
          currentTimer.getValue() instanceof KV,
          "Accessing timer in unkeyed context. Current timer is not a KV: %s.",
          currentTimer);

      return new FnApiTimer(timerId, (WindowedValue) currentTimer);
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, currentTimer.getTimestamp(), currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      checkArgument(
          !currentTimer.getTimestamp().isAfter(timestamp),
          "Output time %s can not be before timer timestamp %s.",
          timestamp,
          currentTimer.getTimestamp());
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, timestamp, currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers,
          WindowedValue.of(output, currentTimer.getTimestamp(), currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkArgument(
          !currentTimer.getTimestamp().isAfter(timestamp),
          "Output time %s can not be before timer timestamp %s.",
          timestamp,
          currentTimer.getTimestamp());
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers, WindowedValue.of(output, timestamp, currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public TimeDomain timeDomain() {
      return currentTimeDomain;
    }

    @Override
    public Instant timestamp() {
      return currentTimer.getTimestamp();
    }
  }
}
