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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.SplittableProcessElementInvoker;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.HeapInternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Flink operator for executing splittable {@link DoFn DoFns}. Specifically, for executing
 * the {@code @ProcessElement} method of a splittable {@link DoFn}.
 */
public class SplittableDoFnOperator<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    extends DoFnOperator<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> {

  private transient ScheduledExecutorService executorService;

  /**
   * The state cell containing a watermark hold for the output of this {@link DoFn}. The hold is
   * acquired during the first {@link DoFn.ProcessElement} call for each element and restriction,
   * and is released when the {@link DoFn.ProcessElement} call returns and there is no residual
   * restriction captured by the {@link SplittableProcessElementInvoker}.
   *
   * <p>A hold is needed to avoid letting the output watermark immediately progress together with
   * the input watermark when the first {@link DoFn.ProcessElement} call for this element
   * completes.
   */
  private static final StateTag<WatermarkHoldState> watermarkHoldTag =
      StateTags.makeSystemTagInternal(
          StateTags.<GlobalWindow>watermarkStateInternal("hold", TimestampCombiner.LATEST));

  /**
   * The state cell containing a copy of the element. Written during the first {@link
   * DoFn.ProcessElement} call and read during subsequent calls in response to timer firings, when
   * the original element is no longer available.
   */
  private final StateTag<ValueState<WindowedValue<InputT>>> elementTag;

  /**
   * The state cell containing a restriction representing the unprocessed part of work for this
   * element.
   */
  private StateTag<ValueState<RestrictionT>> restrictionTag;

  private final DoFn<InputT, OutputT> fn;
  private final Coder<InputT> elementCoder;
  private final Coder<RestrictionT> restrictionCoder;
  private final WindowingStrategy<InputT, ?> inputWindowingStrategy;

  private transient SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT>
      processElementInvoker;

  private transient InternalTimerService<TimerInternals.TimerData> continuationTimerService;

  private transient DoFnInvoker<InputT, OutputT> invoker;

  /**
   * We need this for blocking shutdown in close while we still have pending processing-time timers.
   */
  protected transient Object checkpointingLock;


  public SplittableDoFnOperator(
      ProcessFn<InputT, OutputT, RestrictionT, TrackerT> processFn,
      String stepName,
      Coder<WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>>> inputCoder,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    super(
        processFn,
        stepName,
        inputCoder,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder);

    fn = processFn.getFn();
    elementCoder = processFn.getElementCoder();
    restrictionCoder = processFn.getRestrictionCoder();
    inputWindowingStrategy = processFn.getInputWindowingStrategy();

    this.elementTag =
        StateTags.value(
            "element",
            WindowedValue.getFullCoder(
                elementCoder, inputWindowingStrategy.getWindowFn().windowCoder()));

    this.restrictionTag = StateTags.value("restriction", restrictionCoder);
  }

  @Override
  public void setup(
      StreamTask<?, ?> containingTask,
      StreamConfig config,
      Output<StreamRecord<WindowedValue<OutputT>>> output) {
    super.setup(containingTask, config, output);
    checkpointingLock = containingTask.getCheckpointLock();
  }

  @Override
  public void open() throws Exception {
    super.open();

    checkState(doFn instanceof ProcessFn);

    continuationTimerService =
        getInternalTimerService("continuation", new CoderTypeSerializer<>(timerCoder), this);

    invoker = DoFnInvokers.invokerFor(fn);
    invoker.invokeSetup();

    executorService = Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory());


    processElementInvoker = new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            fn,
            serializedOptions.getPipelineOptions(),
            new OutputWindowedValue<OutputT>() {
              @Override
              public void outputWindowedValue(
                  OutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(
                    mainOutputTag,
                    WindowedValue.of(output, timestamp, windows, pane));
              }

              @Override
              public <AdditionalOutputT> void outputWindowedValue(
                  TupleTag<AdditionalOutputT> tag,
                  AdditionalOutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
              }
            },
            sideInputReader,
            executorService,
            10000,
            Duration.standardSeconds(10));
  }


  @Override
  public void processElement(
      StreamRecord<WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>>> streamRecord)
      throws Exception {

    KeyedWorkItem<String, KV<InputT, RestrictionT>> inputElement =
        streamRecord.getValue().getValue();

    // Initialize state (element and restriction)

    WindowedValue<KV<InputT, RestrictionT>> windowedValue =
        Iterables.getOnlyElement(inputElement.elementsIterable());
    BoundedWindow window = Iterables.getOnlyElement(windowedValue.getWindows());
    StateNamespace stateNamespace =
          StateNamespaces.window(
              (Coder<BoundedWindow>) inputWindowingStrategy.getWindowFn().windowCoder(), window);

    ValueState<WindowedValue<InputT>> elementState =
        stateInternals.state(stateNamespace, elementTag);
    ValueState<RestrictionT> restrictionState =
        stateInternals.state(stateNamespace, restrictionTag);
    WatermarkHoldState holdState = stateInternals.state(stateNamespace, watermarkHoldTag);

    KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction;
    WindowedValue<InputT> element = windowedValue.withValue(windowedValue.getValue().getKey());
    elementState.write(element);
    elementAndRestriction = KV.of(element, windowedValue.getValue().getValue());

    if (processRestriction(elementState, restrictionState, holdState, elementAndRestriction)) {
      return;
    }

    // Set a timer to continue processing this element.
    Instant now = Instant.now();
    continuationTimerService.registerProcessingTimeTimer(
        TimerInternals.TimerData.of(
            "continuation", stateNamespace, now, TimeDomain.PROCESSING_TIME),
        now.getMillis());

    // also register a "last-resort" timer to make sure that we process any pending restrictions
    // when we are shut down
    continuationTimerService.registerEventTimeTimer(
        TimerInternals.TimerData.of(
            "continuation",
            stateNamespace,
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            TimeDomain.EVENT_TIME),
        BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
  }

  /**
   * Process a restriction. Returns true if the restriction is exhausted, false otherwise.
   */
  private boolean processRestriction(
      ValueState<WindowedValue<InputT>> elementState,
      ValueState<RestrictionT> restrictionState,
      WatermarkHoldState holdState,
      KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction) {
    final TrackerT tracker = invoker.invokeNewTracker(elementAndRestriction.getValue());
    SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT>.Result result =
        processElementInvoker.invokeProcessElement(
            invoker, elementAndRestriction.getKey(), tracker);

    // Save state for resuming.
    if (result.getResidualRestriction() == null) {
      // All work for this element/restriction is completed. Clear state and release hold.
      elementState.clear();
      restrictionState.clear();
      holdState.clear();
      return true;
    }

    restrictionState.write(result.getResidualRestriction());
    Instant futureOutputWatermark = result.getFutureOutputWatermark();
    if (futureOutputWatermark == null) {
      futureOutputWatermark = elementAndRestriction.getKey().getTimestamp();
    }
    holdState.add(futureOutputWatermark);

    return false;
  }

  @Override
  public void onEventTime(
      InternalTimer<Object, TimerInternals.TimerData> flinkTimer) throws Exception {
    if (flinkTimer.getTimestamp() != BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // it's not our "last-resort" timer
      return;
    }

    TimerInternals.TimerData timer = flinkTimer.getNamespace();
    StateNamespace stateNamespace = timer.getNamespace();

    ValueState<WindowedValue<InputT>> elementState =
        stateInternals.state(stateNamespace, elementTag);
    ValueState<RestrictionT> restrictionState =
        stateInternals.state(stateNamespace, restrictionTag);
    WatermarkHoldState holdState = stateInternals.state(stateNamespace, watermarkHoldTag);

    boolean restrictionExhausted;
    do {
      // This is not the first ProcessElement call for this element/restriction - rather,
      // this is a timer firing, so we need to fetch the element and restriction from state.
      elementState.readLater();
      restrictionState.readLater();

      KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction =
          KV.of(elementState.read(), restrictionState.read());

      restrictionExhausted =
          processRestriction(elementState, restrictionState, holdState, elementAndRestriction);

    } while (!restrictionExhausted);
  }

  @Override
  public void onProcessingTime(
      InternalTimer<Object, TimerInternals.TimerData> flinkTimer) throws Exception {

    TimerInternals.TimerData timer = flinkTimer.getNamespace();
    StateNamespace stateNamespace = timer.getNamespace();

    ValueState<WindowedValue<InputT>> elementState =
        stateInternals.state(stateNamespace, elementTag);
    ValueState<RestrictionT> restrictionState =
        stateInternals.state(stateNamespace, restrictionTag);
    WatermarkHoldState holdState = stateInternals.state(stateNamespace, watermarkHoldTag);

    // This is not the first ProcessElement call for this element/restriction - rather,
    // this is a timer firing, so we need to fetch the element and restriction from state.
    elementState.readLater();
    restrictionState.readLater();

    WindowedValue<InputT> element = elementState.read();

    if (element == null) {
      // it can happen that the "last-resort" event-time timer already cleaned this on the
      // +Inf watermark
      return;
    }

    KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction =
        KV.of(element, restrictionState.read());

    if (processRestriction(elementState, restrictionState, holdState, elementAndRestriction)) {
      return;
    }

    // Set a timer to continue processing this element.
    Instant now = Instant.now();
    continuationTimerService.registerProcessingTimeTimer(
        TimerInternals.TimerData.of(
            "continuation", stateNamespace, now, TimeDomain.PROCESSING_TIME),
        now.getMillis());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);

    // Also update the watermark on our continuation service. We have to manually do this because
    // DoFnOperator overrides processWatermark() and AbstractStreamOperator therefore doesn't
    // advance all the registered timer services.
    ((HeapInternalTimerService) continuationTimerService).advanceWatermark(mark.getTimestamp());
  }

  @Override
  public void close() throws Exception {
    super.close();

    invoker.invokeTeardown();

    executorService.shutdown();

    long shutdownTimeout = Duration.standardSeconds(10).getMillis();
    try {
      if (!executorService.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.debug("The scheduled executor service did not properly terminate. Shutting "
            + "it down now.");
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.debug("Could not properly await the termination of the scheduled executor service.", e);
      executorService.shutdownNow();
    }
  }

}
