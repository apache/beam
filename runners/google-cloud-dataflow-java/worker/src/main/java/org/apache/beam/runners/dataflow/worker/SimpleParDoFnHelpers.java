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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SimpleParDoFnHelpers<InputT, OutputT, W extends BoundedWindow> {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleParDoFnHelpers.class);

  // TODO: Remove once Distributions has shipped.
  @VisibleForTesting
  static final String OUTPUTS_PER_ELEMENT_EXPERIMENT = "outputs_per_element_counter";

  private static final String COUNTER_NAME = "per-element-output-count";

  final PipelineOptions options;
  final DoFnInstanceManager doFnInstanceManager;

  private final SideInputReader sideInputReader;
  final DataflowOperationContext operationContext;
  private final TupleTag<OutputT> mainOutputTag;
  private final Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices;
  private final List<TupleTag<?>> sideOutputTags;
  final DataflowExecutionContext.DataflowStepContext stepContext;
  final DataflowExecutionContext.DataflowStepContext userStepContext;
  private final CounterFactory counterFactory;
  private final DoFnRunnerFactory runnerFactory;
  final boolean isStreaming;
  final boolean hasStreamingSideInput;
  private final int mainReceiverIndex;
  final OutputsPerElementTracker outputsPerElementTracker;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  // Various DoFn helpers, null between bundles
  @Nullable DoFnRunner<InputT, OutputT> fnRunner;
  @Nullable DoFnInfo<InputT, OutputT> activeFnInfo;
  private Receiver @Nullable [] receivers;

  // This may additionally be null if it is not a real DoFn but an OldDoFn or
  // GroupAlsoByWindowViaWindowSetDoFn
  private @Nullable DoFnSignature fnSignature;
  private boolean hasState;

  SimpleParDoFnHelpers(
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
    Integer mainIdx = outputTupleTagsToReceiverIndices.get(mainOutputTag);
    this.mainReceiverIndex = mainIdx != null ? mainIdx : -1;
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
    this.isStreaming = options.as(StreamingOptions.class).isStreaming();
    this.hasStreamingSideInput = this.isStreaming && !sideInputReader.isEmpty();
    this.outputsPerElementTracker = createOutputsPerElementTracker();
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideInputMapping = sideInputMapping;
  }

  boolean hasState() {
    return hasState;
  }

  void startBundle(Receiver... receivers) throws Exception {
    checkState(fnRunner == null, "bundle already started (or not properly finished)");
    checkArgument(
        receivers.length == outputTupleTagsToReceiverIndices.size(),
        "unexpected number of receivers for DoFn");

    this.receivers = receivers;
  }

  void reallyStartBundle() throws Exception {
    checkState(fnRunner == null, "bundle already started (or not properly finished)");

    @Nullable Receiver mainReceiver =
        receivers != null && mainReceiverIndex >= 0 && mainReceiverIndex < receivers.length
            ? receivers[mainReceiverIndex]
            : null;
    WindowedValueMultiReceiver outputManager =
        new OutputManager(
            outputsPerElementTracker,
            mainOutputTag,
            outputTupleTagsToReceiverIndices,
            receivers,
            mainReceiverIndex,
            mainReceiver,
            counterFactory,
            stepContext.getNameContext(),
            isStreaming);
    activeFnInfo = (DoFnInfo) doFnInstanceManager.get();
    fnSignature = DoFnSignatures.getSignature(activeFnInfo.getDoFn().getClass());
    hasState = fnSignature != null && !fnSignature.stateDeclarations().isEmpty();

    fnRunner =
        runnerFactory.createRunner(
            activeFnInfo.getDoFn(),
            options,
            mainOutputTag,
            sideOutputTags,
            activeFnInfo.getSideInputViews(),
            sideInputReader,
            activeFnInfo.getInputCoder(),
            activeFnInfo.getOutputCoders(),
            activeFnInfo.getWindowingStrategy(),
            stepContext,
            userStepContext,
            outputManager,
            doFnSchemaInformation,
            sideInputMapping);
    fnRunner.startBundle();
  }

  void finishBundle(StreamingSideInputProcessor<?, ?> sideInputProcessor) throws Exception {
    if (fnRunner != null) {
      fnRunner.finishBundle();
      if (sideInputProcessor != null) {
        sideInputProcessor.handleFinishBundle();
      }
      doFnInstanceManager.complete(activeFnInfo);
      fnRunner = null;
      activeFnInfo = null;
      fnSignature = null;
      hasState = false;
      sideInputProcessor = null;
    }
  }

  void abort() throws Exception {
    try {
      doFnInstanceManager.abort(activeFnInfo);
    } finally {
      fnRunner = null;
      activeFnInfo = null;
      fnSignature = null;
      hasState = false;
    }
  }

  /**
   * Routes emitted elements to the corresponding {@link Receiver} with minimal per-element
   * overhead. Uses direct reference-equality checks against the declared and most-recently-seen
   * main tag (handling deserialized {@code static final TupleTag} instances without thrashing) and
   * a bounded lazily allocated per-slot tag cache for multi-output {@code DoFn}s.
   */
  private static final class OutputManager implements WindowedValueMultiReceiver {
    private static final int MAX_CACHED_OUTPUT_SLOTS = 8;

    private final OutputsPerElementTracker outputsPerElementTracker;
    private final TupleTag<?> mainOutputTag;
    private final Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices;
    private final Receiver @Nullable [] receivers;
    private final int mainReceiverIndex;
    private final @Nullable Receiver mainReceiver;
    private final CounterFactory counterFactory;
    private final NameContext stepNameContext;
    private final boolean isStreaming;

    // Captures a deserialized equal-but-not-identical TupleTag instance mapping to mainReceiver
    // so both c.output(elem) (mainOutputTag) and c.output(STATIC_TAG, elem) hit without map lookup
    // or write-barrier thrashing. Initialized to mainOutputTag (non-null) so null tags never match.
    private TupleTag<?> altMainTag;
    // Lazily allocated per-receiver-slot identity cache for multi-output DoFns (2..8 outputs).
    private @Nullable TupleTag<?> @Nullable [] cachedTags;
    private @Nullable Map<TupleTag<?>, OutputReceiver> undeclaredOutputs;

    OutputManager(
        OutputsPerElementTracker outputsPerElementTracker,
        TupleTag<?> mainOutputTag,
        Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
        Receiver @Nullable [] receivers,
        int mainReceiverIndex,
        @Nullable Receiver mainReceiver,
        CounterFactory counterFactory,
        NameContext stepNameContext,
        boolean isStreaming) {
      this.outputsPerElementTracker = outputsPerElementTracker;
      this.mainOutputTag = mainOutputTag;
      this.altMainTag = mainOutputTag;
      this.outputTupleTagsToReceiverIndices = outputTupleTagsToReceiverIndices;
      this.receivers = receivers;
      this.mainReceiverIndex = mainReceiverIndex;
      this.mainReceiver = mainReceiver;
      this.counterFactory = counterFactory;
      this.stepNameContext = stepNameContext;
      this.isStreaming = isStreaming;
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public <TagT> void output(TupleTag<TagT> tag, WindowedValue<TagT> output) {
      outputsPerElementTracker.onOutput();
      Receiver receiver;
      if (mainReceiver != null && (tag == mainOutputTag || tag == altMainTag)) {
        receiver = mainReceiver;
      } else {
        receiver = getOtherReceiver(tag);
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

    @SuppressWarnings("ReferenceEquality")
    private Receiver getOtherReceiver(TupleTag<?> tag) {
      TupleTag<?>[] tags = this.cachedTags;
      if (tags != null && tag != null && receivers != null) {
        for (int i = 0; i < tags.length; i++) {
          if (tags[i] == tag) {
            return receivers[i];
          }
        }
      }
      return lookupOrCreateReceiverSlow(tag);
    }

    private Receiver lookupOrCreateReceiverSlow(TupleTag<?> tag) {
      Integer receiverIndex = outputTupleTagsToReceiverIndices.get(tag);
      if (receiverIndex != null) {
        int idx = receiverIndex;
        Receiver receiver = receivers[idx];
        if (receiver != null) {
          if (tag != null) {
            if (idx == mainReceiverIndex) {
              altMainTag = tag;
            } else if (receivers.length <= MAX_CACHED_OUTPUT_SLOTS) {
              TupleTag<?>[] tags = this.cachedTags;
              if (tags == null) {
                tags = new TupleTag<?>[receivers.length];
                this.cachedTags = tags;
              }
              tags[idx] = tag;
            }
          }
          return receiver;
        }
      }
      return getOrCreateUndeclaredReceiver(tag);
    }

    private Receiver getOrCreateUndeclaredReceiver(TupleTag<?> tag) {
      if (undeclaredOutputs != null) {
        OutputReceiver existing = undeclaredOutputs.get(tag);
        if (existing != null) {
          return existing;
        }
      }
      // A new undeclared output.
      // TODO: plumb through the operationName, so that we can
      // name implicit outputs after it.
      String outputName = "implicit-" + tag.getId();
      // TODO: plumb through the counter prefix, so that we can
      // make it available to the OutputReceiver class in case
      // it wants to use it in naming output counterFactory. (It
      // doesn't today.)
      OutputReceiver undeclaredReceiver = new OutputReceiver();
      ElementCounter outputCounter =
          DataflowOutputCounter.create(outputName, counterFactory, stepNameContext, isStreaming);
      undeclaredReceiver.addOutputCounter(outputCounter);
      if (undeclaredOutputs == null) {
        undeclaredOutputs = new HashMap<>();
      }
      undeclaredOutputs.put(tag, undeclaredReceiver);
      return undeclaredReceiver;
    }
  }

  DoFnInfo<InputT, OutputT> getDoFnInfo() throws Exception {
    return activeFnInfo != null
        ? activeFnInfo
        : (DoFnInfo<InputT, OutputT>) doFnInstanceManager.peek();
  }

  @VisibleForTesting static final String CLEANUP_TIMER_ID = "cleanup-timer";

  enum TimerType {
    USER {
      @Override
      public void processTimer(
          SimpleParDoFnHelpers doFn,
          TimerInternals.TimerData timer,
          Supplier<StreamingSideInputProcessor<?, ?>> sideInputProcessor)
          throws Exception {
        doFn.processUserTimer(timer, sideInputProcessor.get());
      }
    },
    FAIL_USER {
      @Override
      public void processTimer(
          SimpleParDoFnHelpers doFn,
          TimerInternals.TimerData timer,
          Supplier<StreamingSideInputProcessor<?, ?>> sideInputProcessor)
          throws Exception {
        throw new UnsupportedOperationException(
            "Attempt to deliver a timer to a DoFn, but timers are not supported here.");
      }
    },
    SYSTEM {
      @Override
      public void processTimer(
          SimpleParDoFnHelpers doFn,
          TimerInternals.TimerData timer,
          Supplier<StreamingSideInputProcessor<?, ?>> sideInputProcessor)
          throws Exception {
        doFn.processSystemTimer(timer, sideInputProcessor.get());
      }
    };

    public abstract void processTimer(
        SimpleParDoFnHelpers doFn,
        TimerInternals.TimerData timer,
        Supplier<StreamingSideInputProcessor<?, ?>> sideInputProcessor)
        throws Exception;
  };

  void processTimers(
      TimerType mode,
      DataflowExecutionContext.DataflowStepContext context,
      Coder<BoundedWindow> windowCoder,
      Runnable startKey,
      Supplier<StreamingSideInputProcessor<?, ?>> sideInputProcessor)
      throws Exception {
    TimerInternals.TimerData timer = context.getNextFiredTimer(windowCoder);

    if (timer != null && fnRunner == null) {
      // If we need to run reallyStartBundle in here, we need to make sure to switch the state
      // sampler into the start state.
      try (Closeable start = operationContext.enterStart()) {
        reallyStartBundle();
        startKey.run();
      }
    }

    while (timer != null) {
      mode.processTimer(this, timer, sideInputProcessor);
      timer = context.getNextFiredTimer(windowCoder);
    }
  }

  protected void processUserTimer(
      TimerInternals.TimerData timer, StreamingSideInputProcessor<InputT, W> sideInputProcessor) {
    if (fnSignature.timerDeclarations().containsKey(timer.getTimerId())
        || fnSignature.timerFamilyDeclarations().containsKey(timer.getTimerFamilyId())) {
      BoundedWindow window = ((StateNamespaces.WindowNamespace) timer.getNamespace()).getWindow();
      if (sideInputProcessor != null) {
        sideInputProcessor.handleProcessTimer(timer);
      }
      fnRunner.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          this.stepContext.stateInternals().getKey(),
          window,
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain(),
          timer.causedByDrain());
    }
  }

  private void processSystemTimer(
      TimerInternals.TimerData timer, StreamingSideInputProcessor<InputT, W> sideInputProcessor)
      throws Exception {
    // Timer owned by this class, for cleaning up state in expired windows
    if (timer.getTimerId().equals(CLEANUP_TIMER_ID)) {
      checkState(
          timer.getDomain().equals(TimeDomain.EVENT_TIME),
          "%s received cleanup timer with domain not EVENT_TIME: %s",
          this,
          timer);

      checkState(
          timer.getNamespace() instanceof StateNamespaces.WindowNamespace,
          "%s received cleanup timer not for a %s: %s",
          this,
          StateNamespaces.WindowNamespace.class.getSimpleName(),
          timer);

      if (sideInputProcessor != null) {
        // We must call this to ensure the side-input is cached for onWindowExpiration. Since we
        // don't set cleanup
        // timers until we actually call processElement, the window must be unblocked here.
        sideInputProcessor.handleProcessTimer(timer);
      }

      BoundedWindow window = ((StateNamespaces.WindowNamespace) timer.getNamespace()).getWindow();
      Instant targetTime =
          earliestAllowableCleanupTime(window, activeFnInfo.getWindowingStrategy());

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
      for (DoFnSignature.StateDeclaration stateDecl : fnSignature.stateDeclarations().values()) {
        StateTag<?> tag;
        try {
          tag =
              StateTags.tagForSpec(
                  stateDecl.id(), (StateSpec) stateDecl.field().get(activeFnInfo.getDoFn()));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(
              String.format(
                  "Error accessing %s for %s",
                  StateSpec.class.getName(), activeFnInfo.getDoFn().getClass().getName()),
              e);
        }

        StateInternals stateInternals = userStepContext.stateInternals();
        State state = stateInternals.state(timer.getNamespace(), tag);
        state.clear();
      }
    }
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
  interface OutputsPerElementTracker {

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

  Instant earliestAllowableCleanupTime(BoundedWindow window, WindowingStrategy windowingStrategy) {
    Instant cleanupTime =
        window
            .maxTimestamp()
            .plus(windowingStrategy.getAllowedLateness())
            .plus(Duration.millis(1L));
    return cleanupTime.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)
        ? BoundedWindow.TIMESTAMP_MAX_VALUE
        : cleanupTime;
  }

  @SuppressWarnings("unchecked")
  void registerStateCleanup(WindowingStrategy<?, W> windowingStrategy, WindowedValue<?> elem) {
    if (DataflowOutputCounter.isKnownGlobalWindowedValue(elem)) {
      Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();
      registerStateCleanupForWindow(windowingStrategy, windowCoder, (W) GlobalWindow.INSTANCE);
    } else {
      registerStateCleanup(windowingStrategy, (Collection<W>) elem.getWindows());
    }
  }

  protected void registerStateCleanup(
      WindowingStrategy<?, W> windowingStrategy, Collection<W> windowsToCleanup) {
    Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();

    for (W window : windowsToCleanup) {
      registerStateCleanupForWindow(windowingStrategy, windowCoder, window);
    }
  }

  private void registerStateCleanupForWindow(
      WindowingStrategy<?, W> windowingStrategy, Coder<W> windowCoder, W window) {
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
          fnSignature.onWindowExpiration() == null
              ? cleanupTime
              : cleanupTime.minus(Duration.millis(1L));
      stepContext.setStateCleanupTimer(
          CLEANUP_TIMER_ID, window, windowCoder, cleanupTime, cleanupOutputTimestamp);
    }
  }
}
