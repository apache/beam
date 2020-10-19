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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class providing simple set up, processing, and tear down for a wrapped {@link
 * GroupAlsoByWindowFn}.
 *
 * <p>Subclasses override just a method to provide a {@link DoFnInfo} for the wrapped {@link
 * GroupAlsoByWindowFn}.
 */
public class SimpleParDoFn<InputT, OutputT> implements ParDoFn {
  // TODO: Remove once Distributions has shipped.
  @VisibleForTesting
  static final String OUTPUTS_PER_ELEMENT_EXPERIMENT = "outputs_per_element_counter";

  private static final String COUNTER_NAME = "per-element-output-count";

  private static final Logger LOG = LoggerFactory.getLogger(SimpleParDoFn.class);

  protected final PipelineOptions options;
  private final DoFnInstanceManager doFnInstanceManager;

  private final SideInputReader sideInputReader;
  private final DataflowOperationContext operationContext;
  private final TupleTag<OutputT> mainOutputTag;
  private final Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices;
  private final List<TupleTag<?>> sideOutputTags;
  private final DataflowExecutionContext.DataflowStepContext stepContext;
  private final DataflowExecutionContext.DataflowStepContext userStepContext;
  private final CounterFactory counterFactory;
  private final DoFnRunnerFactory runnerFactory;
  private final boolean hasStreamingSideInput;
  private final OutputsPerElementTracker outputsPerElementTracker;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  // Various DoFn helpers, null between bundles
  private @Nullable DoFnRunner<InputT, OutputT> fnRunner;
  @Nullable DoFnInfo<InputT, OutputT> fnInfo;
  private Receiver @Nullable [] receivers;

  // This may additionally be null if it is not a real DoFn but an OldDoFn or
  // GroupAlsoByWindowViaWindowSetDoFn
  private @Nullable DoFnSignature fnSignature;

  /** Creates a {@link SimpleParDoFn} using basic information about the step being executed. */
  SimpleParDoFn(
      PipelineOptions options,
      DoFnInstanceManager doFnInstanceManager,
      SideInputReader sideInputReader,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext.DataflowStepContext stepContext,
      DataflowOperationContext operationContext,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      DoFnRunnerFactory runnerFactory) {
    this.options = options;
    this.doFnInstanceManager = doFnInstanceManager;

    // We vend a freshly deserialized version for each run
    this.sideInputReader = sideInputReader;
    this.operationContext = operationContext;
    checkArgument(!outputTupleTagsToReceiverIndices.isEmpty(), "expected at least one output");
    this.mainOutputTag = mainOutputTag;
    this.outputTupleTagsToReceiverIndices = outputTupleTagsToReceiverIndices;
    ImmutableList.Builder<TupleTag<?>> sideOutputTagsBuilder = ImmutableList.builder();
    for (TupleTag<?> tag : outputTupleTagsToReceiverIndices.keySet()) {
      if (!mainOutputTag.equals(tag)) {
        sideOutputTagsBuilder.add(tag);
      }
    }
    this.sideOutputTags = sideOutputTagsBuilder.build();
    this.stepContext = stepContext;

    // StepContext provides a TimerInternals and StateInternals for use by the system - this class.
    // For the user, we request a user-scoped StepContext to provide a user-scoped
    // StateInternals and TimerInternals.
    this.userStepContext = stepContext.namespacedToUser();

    this.counterFactory = operationContext.counterFactory();
    this.runnerFactory = runnerFactory;
    this.hasStreamingSideInput =
        options.as(StreamingOptions.class).isStreaming() && !sideInputReader.isEmpty();
    this.outputsPerElementTracker = createOutputsPerElementTracker();
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  private OutputsPerElementTracker createOutputsPerElementTracker() {
    // TODO: Remove once Distributions has shipped.
    if (!hasExperiment(OUTPUTS_PER_ELEMENT_EXPERIMENT)) {
      return NoopOutputsPerElementTracker.INSTANCE;
    }

    // TODO: Remove log statement when functionality is enabled by default.
    LOG.info("{} counter enabled.", COUNTER_NAME);

    return new OutputsPerElementTrackerImpl();
  }

  private boolean hasExperiment(String experiment) {
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    return experiments != null && experiments.contains(experiment);
  }

  /** Simple state tracker to calculate PerElementOutputCount counter. */
  private interface OutputsPerElementTracker {
    void onOutput();

    void onProcessElement();

    void onProcessElementSuccess();
  }

  private class OutputsPerElementTrackerImpl implements OutputsPerElementTracker {
    private long outputsPerElement;
    private final Counter<Long, CounterFactory.CounterDistribution> counter;

    public OutputsPerElementTrackerImpl() {
      this.counter =
          counterFactory.distribution(
              CounterName.named(COUNTER_NAME).withOriginalName(stepContext.getNameContext()));
    }

    @Override
    public void onProcessElement() {
      reset();
    }

    @Override
    public void onOutput() {
      outputsPerElement++;
    }

    @Override
    public void onProcessElementSuccess() {
      counter.addValue(outputsPerElement);
      reset();
    }

    private void reset() {
      outputsPerElement = 0L;
    }
  }

  /** No-op {@link OutputsPerElementTracker} implementation used when the counter is disabled. */
  private static class NoopOutputsPerElementTracker implements OutputsPerElementTracker {
    private NoopOutputsPerElementTracker() {}

    public static final OutputsPerElementTracker INSTANCE = new NoopOutputsPerElementTracker();

    @Override
    public void onOutput() {}

    @Override
    public void onProcessElement() {}

    @Override
    public void onProcessElementSuccess() {}
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    checkArgument(
        receivers.length == outputTupleTagsToReceiverIndices.size(),
        "unexpected number of receivers for DoFn");

    this.receivers = receivers;
    if (hasStreamingSideInput) {
      // There is non-trivial setup that needs to be performed for watermark propagation
      // even on empty bundles.
      reallyStartBundle();
    }
  }

  private void reallyStartBundle() throws Exception {
    checkState(fnRunner == null, "bundle already started (or not properly finished)");

    OutputManager outputManager =
        new OutputManager() {
          final Map<TupleTag<?>, OutputReceiver> undeclaredOutputs = new HashMap<>();

          private @Nullable Receiver getReceiverOrNull(TupleTag<?> tag) {
            Integer receiverIndex = outputTupleTagsToReceiverIndices.get(tag);
            if (receiverIndex != null) {
              return receivers[receiverIndex];
            } else {
              return undeclaredOutputs.get(tag);
            }
          }

          @Override
          public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            outputsPerElementTracker.onOutput();
            Receiver receiver = getReceiverOrNull(tag);
            if (receiver == null) {
              // A new undeclared output.
              // TODO: plumb through the operationName, so that we can
              // name implicit outputs after it.
              String outputName = "implicit-" + tag.getId();
              // TODO: plumb through the counter prefix, so we can
              // make it available to the OutputReceiver class in case
              // it wants to use it in naming output counterFactory.  (It
              // doesn't today.)
              OutputReceiver undeclaredReceiver = new OutputReceiver();

              ElementCounter outputCounter =
                  new DataflowOutputCounter(
                      outputName, counterFactory, stepContext.getNameContext());
              undeclaredReceiver.addOutputCounter(outputCounter);
              undeclaredOutputs.put(tag, undeclaredReceiver);
              receiver = undeclaredReceiver;
            }

            try {
              receiver.process(output);
            } catch (RuntimeException | Error e) {
              // Rethrow unchecked exceptions as-is to avoid excessive nesting
              // via a chain of DoFn's.
              throw e;
            } catch (Exception e) {
              // This should never happen in practice with DoFn's, but can happen
              // with other Receivers.
              throw new RuntimeException(e);
            }
          }
        };
    fnInfo = (DoFnInfo) doFnInstanceManager.get();
    fnSignature = DoFnSignatures.getSignature(fnInfo.getDoFn().getClass());

    fnRunner =
        runnerFactory.createRunner(
            fnInfo.getDoFn(),
            options,
            mainOutputTag,
            sideOutputTags,
            fnInfo.getSideInputViews(),
            sideInputReader,
            fnInfo.getInputCoder(),
            fnInfo.getOutputCoders(),
            fnInfo.getWindowingStrategy(),
            stepContext,
            userStepContext,
            outputManager,
            doFnSchemaInformation,
            sideInputMapping);

    fnRunner.startBundle();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(Object untypedElem) throws Exception {
    if (fnRunner == null) {
      // If we need to run reallyStartBundle in here, we need to make sure to switch the state
      // sampler into the start state.
      try (Closeable start = operationContext.enterStart()) {
        reallyStartBundle();
      }
    }

    WindowedValue<InputT> elem = (WindowedValue<InputT>) untypedElem;

    if (fnSignature != null && fnSignature.stateDeclarations().size() > 0) {
      registerStateCleanup(
          (WindowingStrategy<?, BoundedWindow>) getDoFnInfo().getWindowingStrategy(),
          (Collection<BoundedWindow>) elem.getWindows());
    }

    outputsPerElementTracker.onProcessElement();
    fnRunner.processElement(elem);
    outputsPerElementTracker.onProcessElementSuccess();
  }

  @Override
  public void processTimers() throws Exception {

    // Note: We need to get windowCoder to decode the timers.  If we haven't already deserialized
    // the fnInfo, we peek at a new instance to retrieve that. If this extra deserialization becomes
    // excessively costly, we could either (1) have the DoFnInstanceManager remember the associated
    // windowCoder (allowing us to get it without a DoFnInfo instance) or (2) check whether timers
    // exist without actually decoding them.
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>)
            (fnInfo != null ? fnInfo : doFnInstanceManager.peek())
                .getWindowingStrategy()
                .getWindowFn()
                .windowCoder();
    processTimers(TimerType.USER, userStepContext, windowCoder);
    processTimers(TimerType.SYSTEM, stepContext, windowCoder);
  }

  private void processUserTimer(TimerData timer) throws Exception {
    if (fnSignature.timerDeclarations().containsKey(timer.getTimerId())
        || fnSignature.timerFamilyDeclarations().containsKey(timer.getTimerFamilyId())) {
      BoundedWindow window = ((WindowNamespace) timer.getNamespace()).getWindow();
      fnRunner.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          this.stepContext.stateInternals().getKey(),
          window,
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain());
    }
  }

  private void processSystemTimer(TimerData timer) throws Exception {

    // Timer owned by this class, for cleaning up state in expired windows
    if (timer.getTimerId().equals(CLEANUP_TIMER_ID)) {
      checkState(
          timer.getDomain().equals(TimeDomain.EVENT_TIME),
          "%s received cleanup timer with domain not EVENT_TIME: %s",
          this,
          timer);

      checkState(
          timer.getNamespace() instanceof WindowNamespace,
          "%s received cleanup timer not for a %s: %s",
          this,
          WindowNamespace.class.getSimpleName(),
          timer);

      BoundedWindow window = ((WindowNamespace) timer.getNamespace()).getWindow();
      Instant targetTime = earliestAllowableCleanupTime(window, fnInfo.getWindowingStrategy());

      checkState(
          !targetTime.isAfter(timer.getTimestamp()),
          "%s received state cleanup timer for window %s "
              + " that is before the appropriate cleanup time %s",
          this,
          window,
          targetTime);

      fnRunner.onWindowExpiration(
          window, timer.getOutputTimestamp(), this.stepContext.stateInternals().getKey());

      // This is for a timer for a window that is expired, so clean it up.
      for (StateDeclaration stateDecl : fnSignature.stateDeclarations().values()) {
        StateTag<?> tag;
        try {
          tag =
              StateTags.tagForSpec(
                  stateDecl.id(), (StateSpec) stateDecl.field().get(fnInfo.getDoFn()));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              String.format(
                  "Error accessing %s for %s",
                  StateSpec.class.getName(), fnInfo.getDoFn().getClass().getName()),
              e);
        }

        StateInternals stateInternals = userStepContext.stateInternals();
        org.apache.beam.sdk.state.State state = stateInternals.state(timer.getNamespace(), tag);
        state.clear();
      }
    }
  }

  @Override
  public void finishBundle() throws Exception {
    if (fnRunner != null) {
      fnRunner.finishBundle();
      doFnInstanceManager.complete(fnInfo);
      fnRunner = null;
      fnInfo = null;
      fnSignature = null;
    }
  }

  @Override
  public void abort() throws Exception {
    doFnInstanceManager.abort(fnInfo);
    fnRunner = null;
    fnInfo = null;
  }

  @VisibleForTesting static final String CLEANUP_TIMER_ID = "cleanup-timer";

  private enum TimerType {
    USER {
      @Override
      public void processTimer(SimpleParDoFn doFn, TimerData timer) throws Exception {
        doFn.processUserTimer(timer);
      }
    },
    SYSTEM {
      @Override
      public void processTimer(SimpleParDoFn doFn, TimerData timer) throws Exception {
        doFn.processSystemTimer(timer);
      }
    };

    public abstract void processTimer(SimpleParDoFn doFn, TimerData timer) throws Exception;
  };

  private void processTimers(
      TimerType mode,
      DataflowExecutionContext.DataflowStepContext context,
      Coder<BoundedWindow> windowCoder)
      throws Exception {
    TimerData timer = context.getNextFiredTimer(windowCoder);

    if (timer != null && fnRunner == null) {
      // If we need to run reallyStartBundle in here, we need to make sure to switch the state
      // sampler into the start state.
      try (Closeable start = operationContext.enterStart()) {
        reallyStartBundle();
      }
    }

    while (timer != null) {
      mode.processTimer(this, timer);
      timer = context.getNextFiredTimer(windowCoder);
    }
  }

  private <W extends BoundedWindow> void registerStateCleanup(
      WindowingStrategy<?, W> windowingStrategy, Collection<W> windowsToCleanup) {
    Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();

    for (W window : windowsToCleanup) {
      // The stepContext is the thing that know if it is batch or streaming, hence
      // whether state needs to be cleaned up or will simply be discarded so the
      // timer can be ignored.
      Instant cleanupTime = earliestAllowableCleanupTime(window, windowingStrategy);
      // Set a cleanup timer for state at the end of the window to trigger onWindowExpiration and
      // garbage collect state. We avoid doing this for the global window if there is no window
      // expiration set as the state will be up when the pipeline terminates. Setting the timer
      // leads to a unbounded growth of timers for pipelines with many unique keys in the global
      // window.
      if (cleanupTime.isBefore(GlobalWindow.INSTANCE.maxTimestamp())
          || fnSignature.onWindowExpiration() != null) {
        // If the DoFn has OnWindowExpiration, then set the watermark hold so that the watermark
        // does
        // not advance until OnWindowExpiration completes.
        Instant cleanupOutputTimestamp =
            fnSignature.onWindowExpiration() == null ? cleanupTime : cleanupTime.minus(1L);
        stepContext.setStateCleanupTimer(
            CLEANUP_TIMER_ID, window, windowCoder, cleanupTime, cleanupOutputTimestamp);
      }
    }
  }

  private Instant earliestAllowableCleanupTime(
      BoundedWindow window, WindowingStrategy windowingStrategy) {
    return window.maxTimestamp().plus(windowingStrategy.getAllowedLateness()).plus(1L);
  }

  /**
   * Returns the {@link DoFnInfo} currently being used by this {@link SimpleParDoFn}.
   *
   * <p>May be null if no element has been processed yet, or if the {@link SimpleParDoFn} has
   * finished.
   */
  @Nullable
  @VisibleForTesting
  DoFnInfo<?, ?> getDoFnInfo() {
    return fnInfo;
  }
}
