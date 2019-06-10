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

import static org.apache.flink.util.Preconditions.checkArgument;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.ProcessFnRunner;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.utils.FlinkClassloading;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkBroadcastStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkKeyGroupStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkSplitStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.joda.time.Instant;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT> the input type of the {@link DoFn}
 * @param <OutputT> the output type of the {@link DoFn}
 */
public class DoFnOperator<InputT, OutputT> extends AbstractStreamOperator<WindowedValue<OutputT>>
    implements OneInputStreamOperator<WindowedValue<InputT>, WindowedValue<OutputT>>,
        TwoInputStreamOperator<WindowedValue<InputT>, RawUnionValue, WindowedValue<OutputT>>,
        Triggerable<Object, TimerData> {

  protected DoFn<InputT, OutputT> doFn;

  protected final SerializablePipelineOptions serializedOptions;

  protected final TupleTag<OutputT> mainOutputTag;
  protected final List<TupleTag<?>> additionalOutputTags;

  protected final Collection<PCollectionView<?>> sideInputs;
  protected final Map<Integer, PCollectionView<?>> sideInputTagMapping;

  protected final WindowingStrategy<?, ?> windowingStrategy;

  protected final OutputManagerFactory<OutputT> outputManagerFactory;

  protected transient DoFnRunner<InputT, OutputT> doFnRunner;
  protected transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackDoFnRunner;

  protected transient SideInputHandler sideInputHandler;

  protected transient SideInputReader sideInputReader;

  protected transient BufferedOutputManager<OutputT> outputManager;

  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;

  protected transient long currentInputWatermark;

  protected transient long currentSideInputWatermark;

  protected transient long currentOutputWatermark;

  protected transient FlinkStateInternals<?> keyedStateInternals;

  private final String stepName;

  private final Coder<WindowedValue<InputT>> windowedInputCoder;

  private final Coder<InputT> inputCoder;

  private final Map<TupleTag<?>, Coder<?>> outputCoders;

  private final Coder<?> keyCoder;

  private final KeySelector<WindowedValue<InputT>, ?> keySelector;

  private final TimerInternals.TimerDataCoder timerCoder;

  private final long maxBundleSize;

  private final long maxBundleTimeMills;

  protected transient InternalTimerService<TimerData> timerService;

  protected transient FlinkTimerInternals timerInternals;

  private transient StateInternals nonKeyedStateInternals;

  private transient long pushedBackWatermark;

  private transient PushedBackElementsHandler<WindowedValue<InputT>> pushedBackElementsHandler;

  // bundle control
  private transient boolean bundleStarted = false;
  private transient long elementCount;
  private transient long lastFinishBundleTime;
  private transient ScheduledFuture<?> checkFinishBundleTimer;

  public DoFnOperator(
      DoFn<InputT, OutputT> doFn,
      String stepName,
      Coder<WindowedValue<InputT>> inputWindowedCoder,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder,
      KeySelector<WindowedValue<InputT>, ?> keySelector) {
    this.doFn = doFn;
    this.stepName = stepName;
    this.windowedInputCoder = inputWindowedCoder;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputTagMapping = sideInputTagMapping;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;

    setChainingStrategy(ChainingStrategy.ALWAYS);

    this.keyCoder = keyCoder;
    this.keySelector = keySelector;

    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());

    FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);

    this.maxBundleSize = flinkOptions.getMaxBundleSize();
    this.maxBundleTimeMills = flinkOptions.getMaxBundleTimeMills();
  }

  // allow overriding this in WindowDoFnOperator because this one dynamically creates
  // the DoFn
  protected DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }

  // allow overriding this, for example SplittableDoFnOperator will not create a
  // stateful DoFn runner because ProcessFn, which is used for executing a Splittable DoFn
  // doesn't play by the normal DoFn rules and WindowDoFnOperator uses LateDataDroppingDoFnRunner
  protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
      DoFnRunner<InputT, OutputT> wrappedRunner) {

    if (keyCoder != null) {
      StatefulDoFnRunner.CleanupTimer cleanupTimer =
          new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, windowingStrategy);

      // we don't know the window type
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder windowCoder = windowingStrategy.getWindowFn().windowCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      StatefulDoFnRunner.StateCleaner<?> stateCleaner =
          new StatefulDoFnRunner.StateInternalsStateCleaner<>(
              doFn, keyedStateInternals, windowCoder);

      return DoFnRunners.defaultStatefulDoFnRunner(
          doFn, wrappedRunner, windowingStrategy, cleanupTimer, stateCleaner);

    } else {
      return doFnRunner;
    }
  }

  @Override
  public void setup(
      StreamTask<?, ?> containingTask,
      StreamConfig config,
      Output<StreamRecord<WindowedValue<OutputT>>> output) {

    // make sure that FileSystems is initialized correctly
    FlinkPipelineOptions options = serializedOptions.get().as(FlinkPipelineOptions.class);
    FileSystems.setDefaultPipelineOptions(options);

    super.setup(containingTask, config, output);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    ListStateDescriptor<WindowedValue<InputT>> pushedBackStateDescriptor =
        new ListStateDescriptor<>(
            "pushed-back-elements", new CoderTypeSerializer<>(windowedInputCoder));

    if (keySelector != null) {
      pushedBackElementsHandler =
          KeyedPushedBackElementsHandler.create(
              keySelector, getKeyedStateBackend(), pushedBackStateDescriptor);
    } else {
      ListState<WindowedValue<InputT>> listState =
          getOperatorStateBackend().getListState(pushedBackStateDescriptor);
      pushedBackElementsHandler = NonKeyedPushedBackElementsHandler.create(listState);
    }

    setCurrentInputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
    setCurrentSideInputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
    setCurrentOutputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());

    FlinkPipelineOptions options = serializedOptions.get().as(FlinkPipelineOptions.class);
    sideInputReader = NullSideInputReader.of(sideInputs);

    // maybe init by initializeState
    if (nonKeyedStateInternals == null) {
      if (keyCoder != null) {
        nonKeyedStateInternals =
            new FlinkKeyGroupStateInternals<>(keyCoder, getKeyedStateBackend());
      } else {
        nonKeyedStateInternals = new FlinkSplitStateInternals<>(getOperatorStateBackend());
      }
    }

    if (!sideInputs.isEmpty()) {

      FlinkBroadcastStateInternals sideInputStateInternals =
          new FlinkBroadcastStateInternals<>(
              getContainingTask().getIndexInSubtaskGroup(), getOperatorStateBackend());

      sideInputHandler = new SideInputHandler(sideInputs, sideInputStateInternals);
      sideInputReader = sideInputHandler;

      Stream<WindowedValue<InputT>> pushedBack = pushedBackElementsHandler.getElements();
      long min =
          pushedBack.map(v -> v.getTimestamp().getMillis()).reduce(Long.MAX_VALUE, Math::min);
      setPushedBackWatermark(min);
    } else {
      setPushedBackWatermark(Long.MAX_VALUE);
    }

    outputManager = outputManagerFactory.create(output, nonKeyedStateInternals);

    // StatefulPardo or WindowDoFn
    if (keyCoder != null) {
      keyedStateInternals =
          new FlinkStateInternals<>((KeyedStateBackend) getKeyedStateBackend(), keyCoder);

      if (timerService == null) {
        timerService =
            getInternalTimerService("beam-timer", new CoderTypeSerializer<>(timerCoder), this);
      }

      timerInternals = new FlinkTimerInternals();
    }

    // WindowDoFnOperator need use state and timer to get DoFn.
    // So must wait StateInternals and TimerInternals ready.
    this.doFn = getDoFn();
    doFnInvoker = DoFnInvokers.invokerFor(doFn);

    doFnInvoker.invokeSetup();

    StepContext stepContext = new FlinkStepContext();

    doFnRunner =
        DoFnRunners.simpleRunner(
            options,
            doFn,
            sideInputReader,
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            stepContext,
            inputCoder,
            outputCoders,
            windowingStrategy);

    doFnRunner = createWrappingDoFnRunner(doFnRunner);

    if (options.getEnableMetrics()) {
      doFnRunner = new DoFnRunnerWithMetricsUpdate<>(stepName, doFnRunner, getRuntimeContext());
    }

    elementCount = 0L;
    lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();

    // Schedule timer to check timeout of finish bundle.
    long bundleCheckPeriod = (maxBundleTimeMills + 1) / 2;
    checkFinishBundleTimer =
        getProcessingTimeService()
            .scheduleAtFixedRate(
                timestamp -> checkInvokeFinishBundleByTime(), bundleCheckPeriod, bundleCheckPeriod);

    if (doFn instanceof SplittableParDoViaKeyedWorkItems.ProcessFn) {
      pushbackDoFnRunner =
          new ProcessFnRunner<>((DoFnRunner) doFnRunner, sideInputs, sideInputHandler);
    } else {
      pushbackDoFnRunner =
          SimplePushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);
    }
  }

  @Override
  public void dispose() throws Exception {
    try {
      super.dispose();
      checkFinishBundleTimer.cancel(true);
    } finally {
      FlinkClassloading.deleteStaticCaches();
      if (bundleStarted) {
        invokeFinishBundle();
      }
      doFnInvoker.invokeTeardown();
    }
  }

  @Override
  public void close() throws Exception {
    try {

      // This is our last change to block shutdown of this operator while
      // there are still remaining processing-time timers. Flink will ignore pending
      // processing-time timers when upstream operators have shut down and will also
      // shut down this operator with pending processing-time timers.
      while (this.numProcessingTimeTimers() > 0) {
        getContainingTask().getCheckpointLock().wait(100);
      }
      if (this.numProcessingTimeTimers() > 0) {
        throw new RuntimeException(
            "There are still processing-time timers left, this indicates a bug");
      }

      // make sure we send a +Inf watermark downstream. It can happen that we receive +Inf
      // in processWatermark*() but have holds, so we have to re-evaluate here.
      processWatermark(new Watermark(Long.MAX_VALUE));
      if (currentOutputWatermark < Long.MAX_VALUE) {
        if (keyedStateInternals == null) {
          throw new RuntimeException("Current watermark is still " + currentOutputWatermark + ".");

        } else {
          throw new RuntimeException(
              "There are still watermark holds. Watermark held at "
                  + keyedStateInternals.watermarkHold().getMillis()
                  + ".");
        }
      }
    } finally {
      super.close();
    }

    // sanity check: these should have been flushed out by +Inf watermarks
    if (!sideInputs.isEmpty() && nonKeyedStateInternals != null) {

      List<WindowedValue<InputT>> pushedBackElements =
          pushedBackElementsHandler.getElements().collect(Collectors.toList());

      if (pushedBackElements.size() > 0) {
        String pushedBackString = Joiner.on(",").join(pushedBackElements);
        throw new RuntimeException(
            "Leftover pushed-back data: " + pushedBackString + ". This indicates a bug.");
      }
    }
  }

  private long getPushbackWatermarkHold() {
    return pushedBackWatermark;
  }

  @Override
  public final void processElement(StreamRecord<WindowedValue<InputT>> streamRecord)
      throws Exception {
    checkInvokeStartBundle();
    doFnRunner.processElement(streamRecord.getValue());
    checkInvokeFinishBundleByCount();
  }

  private void setPushedBackWatermark(long watermark) {
    pushedBackWatermark = watermark;
  }

  @Override
  public final void processElement1(StreamRecord<WindowedValue<InputT>> streamRecord)
      throws Exception {
    checkInvokeStartBundle();
    Iterable<WindowedValue<InputT>> justPushedBack =
        pushbackDoFnRunner.processElementInReadyWindows(streamRecord.getValue());

    long min = pushedBackWatermark;
    for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
      min = Math.min(min, pushedBackValue.getTimestamp().getMillis());
      pushedBackElementsHandler.pushBack(pushedBackValue);
    }
    setPushedBackWatermark(min);

    checkInvokeFinishBundleByCount();
  }

  /**
   * Add the side input value. Here we are assuming that views have already been materialized and
   * are sent over the wire as {@link Iterable}. Subclasses may elect to perform materialization in
   * state and receive side input incrementally instead.
   *
   * @param streamRecord
   */
  protected void addSideInputValue(StreamRecord<RawUnionValue> streamRecord) {
    @SuppressWarnings("unchecked")
    WindowedValue<Iterable<?>> value =
        (WindowedValue<Iterable<?>>) streamRecord.getValue().getValue();

    PCollectionView<?> sideInput = sideInputTagMapping.get(streamRecord.getValue().getUnionTag());
    sideInputHandler.addSideInputValue(sideInput, value);
  }

  @Override
  public final void processElement2(StreamRecord<RawUnionValue> streamRecord) throws Exception {
    // we finish the bundle because the newly arrived side-input might
    // make a view available that was previously not ready.
    // The PushbackSideInputRunner will only reset it's cache of non-ready windows when
    // finishing a bundle.
    invokeFinishBundle();
    checkInvokeStartBundle();

    // add the side input, which may cause pushed back elements become eligible for processing
    addSideInputValue(streamRecord);

    List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();

    Iterator<WindowedValue<InputT>> it = pushedBackElementsHandler.getElements().iterator();

    while (it.hasNext()) {
      WindowedValue<InputT> element = it.next();
      // we need to set the correct key in case the operator is
      // a (keyed) window operator
      setKeyContextElement1(new StreamRecord<>(element));

      Iterable<WindowedValue<InputT>> justPushedBack =
          pushbackDoFnRunner.processElementInReadyWindows(element);
      Iterables.addAll(newPushedBack, justPushedBack);
    }

    pushedBackElementsHandler.clear();
    long min = Long.MAX_VALUE;
    for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
      min = Math.min(min, pushedBackValue.getTimestamp().getMillis());
      pushedBackElementsHandler.pushBack(pushedBackValue);
    }
    setPushedBackWatermark(min);

    checkInvokeFinishBundleByCount();

    // maybe output a new watermark
    processWatermark1(new Watermark(currentInputWatermark));
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    processWatermark1(mark);
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {

    checkInvokeStartBundle();

    // We do the check here because we are guaranteed to at least get the +Inf watermark on the
    // main input when the job finishes.
    if (currentSideInputWatermark >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // this means we will never see any more side input
      // we also do the check here because we might have received the side-input MAX watermark
      // before receiving any main-input data
      emitAllPushedBackData();
    }

    if (keyCoder == null) {
      setCurrentInputWatermark(mark.getTimestamp());
      long potentialOutputWatermark = Math.min(getPushbackWatermarkHold(), currentInputWatermark);
      if (potentialOutputWatermark > currentOutputWatermark) {
        setCurrentOutputWatermark(potentialOutputWatermark);
        emitWatermark(currentOutputWatermark);
      }
    } else {
      setCurrentInputWatermark(mark.getTimestamp());

      // hold back by the pushed back values waiting for side inputs
      long pushedBackInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

      timeServiceManager.advanceWatermark(new Watermark(pushedBackInputWatermark));

      Instant watermarkHold = keyedStateInternals.watermarkHold();

      long combinedWatermarkHold = Math.min(watermarkHold.getMillis(), getPushbackWatermarkHold());

      long potentialOutputWatermark = Math.min(pushedBackInputWatermark, combinedWatermarkHold);

      if (potentialOutputWatermark > currentOutputWatermark) {
        setCurrentOutputWatermark(potentialOutputWatermark);
        emitWatermark(currentOutputWatermark);
      }
    }
  }

  private void emitWatermark(long watermark) {
    // Must invoke finishBatch before emit the +Inf watermark otherwise there are some late events.
    if (watermark >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      invokeFinishBundle();
    }
    output.emitWatermark(new Watermark(watermark));
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    checkInvokeStartBundle();

    setCurrentSideInputWatermark(mark.getTimestamp());
    if (mark.getTimestamp() >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // this means we will never see any more side input
      emitAllPushedBackData();

      // maybe output a new watermark
      processWatermark1(new Watermark(currentInputWatermark));
    }
  }

  /**
   * Emits all pushed-back data. This should be used once we know that there will not be any future
   * side input, i.e. that there is no point in waiting.
   */
  private void emitAllPushedBackData() throws Exception {

    Iterator<WindowedValue<InputT>> it = pushedBackElementsHandler.getElements().iterator();

    while (it.hasNext()) {
      WindowedValue<InputT> element = it.next();
      // we need to set the correct key in case the operator is
      // a (keyed) window operator
      setKeyContextElement1(new StreamRecord<>(element));

      doFnRunner.processElement(element);
    }

    pushedBackElementsHandler.clear();

    setPushedBackWatermark(Long.MAX_VALUE);
  }

  /**
   * Check whether invoke startBundle, if it is, need to output elements that were buffered as part
   * of finishing a bundle in snapshot() first.
   *
   * <p>In order to avoid having {@link DoFnRunner#processElement(WindowedValue)} or {@link
   * DoFnRunner#onTimer(String, BoundedWindow, Instant, TimeDomain)} not between StartBundle and
   * FinishBundle, this method needs to be called in each processElement and each processWatermark
   * and onProcessingTime. Do not need to call in onEventTime, because it has been guaranteed in the
   * processWatermark.
   */
  private void checkInvokeStartBundle() {
    if (!bundleStarted) {
      outputManager.flushBuffer();
      pushbackDoFnRunner.startBundle();
      bundleStarted = true;
    }
  }

  /** Check whether invoke finishBundle by elements count. Called in processElement. */
  private void checkInvokeFinishBundleByCount() {
    elementCount++;
    if (elementCount >= maxBundleSize) {
      invokeFinishBundle();
    }
  }

  /** Check whether invoke finishBundle by timeout. */
  private void checkInvokeFinishBundleByTime() {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    if (now - lastFinishBundleTime >= maxBundleTimeMills) {
      invokeFinishBundle();
    }
  }

  private void invokeFinishBundle() {
    if (bundleStarted) {
      pushbackDoFnRunner.finishBundle();
      bundleStarted = false;
      elementCount = 0L;
      lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {

    // Forced finish a bundle in checkpoint barrier otherwise may lose data.
    // Careful, it use OperatorState or KeyGroupState to store outputs, So it
    // must be called before their snapshot.
    outputManager.openBuffer();
    invokeFinishBundle();
    outputManager.closeBuffer();

    super.snapshotState(context);
  }

  @Override
  public void onEventTime(InternalTimer<Object, TimerData> timer) throws Exception {
    // We don't have to cal checkInvokeStartBundle() because it's already called in
    // processWatermark*().
    fireTimer(timer);
  }

  @Override
  public void onProcessingTime(InternalTimer<Object, TimerData> timer) throws Exception {
    checkInvokeStartBundle();
    fireTimer(timer);
  }

  // allow overriding this in WindowDoFnOperator
  public void fireTimer(InternalTimer<?, TimerData> timer) {
    TimerInternals.TimerData timerData = timer.getNamespace();
    StateNamespace namespace = timerData.getNamespace();
    // This is a user timer, so namespace must be WindowNamespace
    checkArgument(namespace instanceof WindowNamespace);
    BoundedWindow window = ((WindowNamespace) namespace).getWindow();
    pushbackDoFnRunner.onTimer(
        timerData.getTimerId(), window, timerData.getTimestamp(), timerData.getDomain());
  }

  private void setCurrentInputWatermark(long currentInputWatermark) {
    this.currentInputWatermark = currentInputWatermark;
  }

  private void setCurrentSideInputWatermark(long currentInputWatermark) {
    this.currentSideInputWatermark = currentInputWatermark;
  }

  private void setCurrentOutputWatermark(long currentOutputWatermark) {
    this.currentOutputWatermark = currentOutputWatermark;
  }

  /** Factory for creating an {@link BufferedOutputManager} from a Flink {@link Output}. */
  interface OutputManagerFactory<OutputT> extends Serializable {
    BufferedOutputManager<OutputT> create(
        Output<StreamRecord<WindowedValue<OutputT>>> output, StateInternals stateInternals);
  }

  /**
   * A {@link DoFnRunners.OutputManager} that can buffer its outputs. Use {@link
   * FlinkSplitStateInternals} or {@link FlinkKeyGroupStateInternals} to keep buffer data.
   */
  public static class BufferedOutputManager<OutputT> implements DoFnRunners.OutputManager {

    private TupleTag<OutputT> mainTag;
    private Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags;
    private Map<TupleTag<?>, Integer> tagsToIds;
    private Map<Integer, TupleTag<?>> idsToTags;
    protected Output<StreamRecord<WindowedValue<OutputT>>> output;

    private boolean openBuffer = false;
    private BagState<KV<Integer, WindowedValue<?>>> bufferState;

    BufferedOutputManager(
        Output<StreamRecord<WindowedValue<OutputT>>> output,
        TupleTag<OutputT> mainTag,
        Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
        final Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
        Map<TupleTag<?>, Integer> tagsToIds,
        StateInternals stateInternals) {
      this.output = output;
      this.mainTag = mainTag;
      this.tagsToOutputTags = tagsToOutputTags;
      this.tagsToIds = tagsToIds;
      this.idsToTags = new HashMap<>();
      for (Map.Entry<TupleTag<?>, Integer> entry : tagsToIds.entrySet()) {
        idsToTags.put(entry.getValue(), entry.getKey());
      }

      ImmutableMap.Builder<Integer, Coder<WindowedValue<?>>> idsToCodersBuilder =
          ImmutableMap.builder();
      for (Map.Entry<TupleTag<?>, Integer> entry : tagsToIds.entrySet()) {
        idsToCodersBuilder.put(entry.getValue(), tagsToCoders.get(entry.getKey()));
      }

      StateTag<BagState<KV<Integer, WindowedValue<?>>>> bufferTag =
          StateTags.bag("bundle-buffer-tag", new TaggedKvCoder(idsToCodersBuilder.build()));
      bufferState = stateInternals.state(StateNamespaces.global(), bufferTag);
    }

    void openBuffer() {
      this.openBuffer = true;
    }

    void closeBuffer() {
      this.openBuffer = false;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
      if (!openBuffer) {
        emit(tag, value);
      } else {
        bufferState.add(KV.of(tagsToIds.get(tag), value));
      }
    }

    /**
     * Flush elements of bufferState to Flink Output. This method can't be invoke in {@link
     * #snapshotState(StateSnapshotContext)}
     */
    void flushBuffer() {
      for (KV<Integer, WindowedValue<?>> taggedElem : bufferState.read()) {
        emit(idsToTags.get(taggedElem.getKey()), (WindowedValue) taggedElem.getValue());
      }
      bufferState.clear();
    }

    private <T> void emit(TupleTag<T> tag, WindowedValue<T> value) {
      if (tag.equals(mainTag)) {
        // with tagged outputs we can't get around this because we don't
        // know our own output type...
        @SuppressWarnings("unchecked")
        WindowedValue<OutputT> castValue = (WindowedValue<OutputT>) value;
        output.collect(new StreamRecord<>(castValue));
      } else {
        @SuppressWarnings("unchecked")
        OutputTag<WindowedValue<T>> outputTag = (OutputTag) tagsToOutputTags.get(tag);
        output.collect(outputTag, new StreamRecord<>(value));
      }
    }
  }

  /** Coder for KV of id and value. It will be serialized in Flink checkpoint. */
  private static class TaggedKvCoder extends StructuredCoder<KV<Integer, WindowedValue<?>>> {

    private Map<Integer, Coder<WindowedValue<?>>> idsToCoders;

    TaggedKvCoder(Map<Integer, Coder<WindowedValue<?>>> idsToCoders) {
      this.idsToCoders = idsToCoders;
    }

    @Override
    public void encode(KV<Integer, WindowedValue<?>> kv, OutputStream out) throws IOException {
      Coder<WindowedValue<?>> coder = idsToCoders.get(kv.getKey());
      VarIntCoder.of().encode(kv.getKey(), out);
      coder.encode(kv.getValue(), out);
    }

    @Override
    public KV<Integer, WindowedValue<?>> decode(InputStream in) throws IOException {
      Integer id = VarIntCoder.of().decode(in);
      Coder<WindowedValue<?>> coder = idsToCoders.get(id);
      WindowedValue<?> value = coder.decode(in);
      return KV.of(id, value);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return new ArrayList<>(idsToCoders.values());
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      for (Coder<?> coder : idsToCoders.values()) {
        verifyDeterministic(this, "Coder must be deterministic", coder);
      }
    }
  }

  /**
   * Implementation of {@link OutputManagerFactory} that creates an {@link BufferedOutputManager}
   * that can write to multiple logical outputs by Flink side output.
   */
  public static class MultiOutputOutputManagerFactory<OutputT>
      implements OutputManagerFactory<OutputT> {

    private TupleTag<OutputT> mainTag;
    private Map<TupleTag<?>, Integer> tagsToIds;
    private Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags;
    private Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders;

    // There is no side output.
    @SuppressWarnings("unchecked")
    public MultiOutputOutputManagerFactory(
        TupleTag<OutputT> mainTag, Coder<WindowedValue<OutputT>> mainCoder) {
      this(
          mainTag,
          new HashMap<>(),
          ImmutableMap.<TupleTag<?>, Coder<WindowedValue<?>>>builder()
              .put(mainTag, (Coder) mainCoder)
              .build(),
          ImmutableMap.<TupleTag<?>, Integer>builder().put(mainTag, 0).build());
    }

    public MultiOutputOutputManagerFactory(
        TupleTag<OutputT> mainTag,
        Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
        Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
        Map<TupleTag<?>, Integer> tagsToIds) {
      this.mainTag = mainTag;
      this.tagsToOutputTags = tagsToOutputTags;
      this.tagsToCoders = tagsToCoders;
      this.tagsToIds = tagsToIds;
    }

    @Override
    public BufferedOutputManager<OutputT> create(
        Output<StreamRecord<WindowedValue<OutputT>>> output, StateInternals stateInternals) {
      return new BufferedOutputManager<>(
          output, mainTag, tagsToOutputTags, tagsToCoders, tagsToIds, stateInternals);
    }
  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does not allow accessing
   * state or timer internals.
   */
  protected class FlinkStepContext implements StepContext {

    @Override
    public StateInternals stateInternals() {
      return keyedStateInternals;
    }

    @Override
    public TimerInternals timerInternals() {
      return timerInternals;
    }
  }

  private class FlinkTimerInternals implements TimerInternals {

    @Override
    public void setTimer(
        StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
      setTimer(TimerData.of(timerId, namespace, target, timeDomain));
    }

    /** @deprecated use {@link #setTimer(StateNamespace, String, Instant, TimeDomain)}. */
    @Deprecated
    @Override
    public void setTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      switch (timerKey.getDomain()) {
        case EVENT_TIME:
          timerService.registerEventTimeTimer(timerKey, adjustTimestampForFlink(time));
          break;
        case PROCESSING_TIME:
        case SYNCHRONIZED_PROCESSING_TIME:
          timerService.registerProcessingTimeTimer(timerKey, adjustTimestampForFlink(time));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported time domain: " + timerKey.getDomain());
      }
    }

    /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
    @Deprecated
    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
      throw new UnsupportedOperationException("Canceling of a timer by ID is not yet supported.");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      throw new UnsupportedOperationException("Canceling of a timer by ID is not yet supported.");
    }

    /** @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}. */
    @Deprecated
    @Override
    public void deleteTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      switch (timerKey.getDomain()) {
        case EVENT_TIME:
          timerService.deleteEventTimeTimer(timerKey, adjustTimestampForFlink(time));
          break;
        case PROCESSING_TIME:
        case SYNCHRONIZED_PROCESSING_TIME:
          timerService.deleteProcessingTimeTimer(timerKey, adjustTimestampForFlink(time));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported time domain: " + timerKey.getDomain());
      }
    }

    @Override
    public Instant currentProcessingTime() {
      return new Instant(timerService.currentProcessingTime());
    }

    @Nullable
    @Override
    public Instant currentSynchronizedProcessingTime() {
      return new Instant(timerService.currentProcessingTime());
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return new Instant(Math.min(currentInputWatermark, getPushbackWatermarkHold()));
    }

    @Nullable
    @Override
    public Instant currentOutputWatermarkTime() {
      return new Instant(currentOutputWatermark);
    }

    /**
     * In Beam, a timer with timestamp {@code T} is only illegible for firing when the time has
     * moved past this time stamp, i.e. {@code T < current_time}. In the case of event time,
     * current_time is the watermark, in the case of processing time it is the system time.
     *
     * <p>Flink's TimerService has different semantics because it only ensures {@code T <=
     * current_time}.
     *
     * <p>To make up for this, we need to add one millisecond to Flink's internal timer timestamp.
     * Note that we do not modify Beam's timestamp and we are not exposing Flink's timestamp.
     *
     * <p>See also https://jira.apache.org/jira/browse/BEAM-3863
     */
    private long adjustTimestampForFlink(long beamTimerTimestamp) {
      if (beamTimerTimestamp == Long.MAX_VALUE) {
        // We would overflow, do not adjust timestamp
        return Long.MAX_VALUE;
      }
      return beamTimerTimestamp + 1;
    }
  }
}
