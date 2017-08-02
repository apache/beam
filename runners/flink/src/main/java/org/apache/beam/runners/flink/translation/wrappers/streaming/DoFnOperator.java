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
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkBroadcastStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkKeyGroupStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkSplitStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.KeyGroupCheckpointedOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.HeapInternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.joda.time.Instant;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT> the input type of the {@link DoFn}
 * @param <OutputT> the output type of the {@link DoFn}
 * @param <OutputT> the output type of the operator, this can be different from the fn output
 *                 type when we have additional tagged outputs
 */
public class DoFnOperator<InputT, OutputT>
    extends AbstractStreamOperator<WindowedValue<OutputT>>
    implements OneInputStreamOperator<WindowedValue<InputT>, WindowedValue<OutputT>>,
      TwoInputStreamOperator<WindowedValue<InputT>, RawUnionValue, WindowedValue<OutputT>>,
    KeyGroupCheckpointedOperator, Triggerable<Object, TimerData> {

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

  protected transient DoFnRunners.OutputManager outputManager;

  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;

  protected transient long currentInputWatermark;

  protected transient long currentSideInputWatermark;

  protected transient long currentOutputWatermark;

  private transient StateTag<BagState<WindowedValue<InputT>>> pushedBackTag;

  protected transient FlinkStateInternals<?> stateInternals;

  private final String stepName;

  private final Coder<WindowedValue<InputT>> inputCoder;

  private final Coder<?> keyCoder;

  private final TimerInternals.TimerDataCoder timerCoder;

  protected transient HeapInternalTimerService<?, TimerInternals.TimerData> timerService;

  protected transient FlinkTimerInternals timerInternals;

  private transient StateInternals pushbackStateInternals;

  private transient Optional<Long> pushedBackWatermark;

  public DoFnOperator(
      DoFn<InputT, OutputT> doFn,
      String stepName,
      Coder<WindowedValue<InputT>> inputCoder,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    this.doFn = doFn;
    this.stepName = stepName;
    this.inputCoder = inputCoder;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputTagMapping = sideInputTagMapping;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializablePipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;

    setChainingStrategy(ChainingStrategy.ALWAYS);

    this.keyCoder = keyCoder;

    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());
  }

  private org.apache.beam.runners.core.StepContext createStepContext() {
    return new StepContext();
  }

  // allow overriding this in WindowDoFnOperator because this one dynamically creates
  // the DoFn
  protected DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }

  @Override
  public void open() throws Exception {
    super.open();

    setCurrentInputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
    setCurrentSideInputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
    setCurrentOutputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());

    sideInputReader = NullSideInputReader.of(sideInputs);

    if (!sideInputs.isEmpty()) {

      pushedBackTag = StateTags.bag("pushed-back-values", inputCoder);

      FlinkBroadcastStateInternals sideInputStateInternals =
          new FlinkBroadcastStateInternals<>(
              getContainingTask().getIndexInSubtaskGroup(), getOperatorStateBackend());

      sideInputHandler = new SideInputHandler(sideInputs, sideInputStateInternals);
      sideInputReader = sideInputHandler;

      // maybe init by initializeState
      if (pushbackStateInternals == null) {
        if (keyCoder != null) {
          pushbackStateInternals = new FlinkKeyGroupStateInternals<>(keyCoder,
              getKeyedStateBackend());
        } else {
          pushbackStateInternals =
              new FlinkSplitStateInternals<Object>(getOperatorStateBackend());
        }
      }

      pushedBackWatermark = Optional.absent();

    }

    outputManager = outputManagerFactory.create(output);

    // StatefulPardo or WindowDoFn
    if (keyCoder != null) {
      stateInternals = new FlinkStateInternals<>((KeyedStateBackend) getKeyedStateBackend(),
          keyCoder);

      timerService = (HeapInternalTimerService<?, TimerInternals.TimerData>)
          getInternalTimerService("beam-timer", new CoderTypeSerializer<>(timerCoder), this);

      timerInternals = new FlinkTimerInternals();

    }

    // WindowDoFnOperator need use state and timer to get DoFn.
    // So must wait StateInternals and TimerInternals ready.
    this.doFn = getDoFn();
    doFnInvoker = DoFnInvokers.invokerFor(doFn);

    doFnInvoker.invokeSetup();

    org.apache.beam.runners.core.StepContext stepContext = createStepContext();

    doFnRunner = DoFnRunners.simpleRunner(
        serializedOptions.get(),
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        windowingStrategy);

    if (doFn instanceof GroupAlsoByWindowViaWindowSetNewDoFn) {
      // When the doFn is this, we know it came from WindowDoFnOperator and
      //   InputT = KeyedWorkItem<K, V>
      //   OutputT = KV<K, V>
      //
      // for some K, V


      doFnRunner = DoFnRunners.lateDataDroppingRunner(
          (DoFnRunner) doFnRunner,
          stepContext,
          windowingStrategy);
    } else if (keyCoder != null) {
      // It is a stateful DoFn

      StatefulDoFnRunner.CleanupTimer cleanupTimer =
          new StatefulDoFnRunner.TimeInternalsCleanupTimer(
              stepContext.timerInternals(), windowingStrategy);

      // we don't know the window type
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder windowCoder = windowingStrategy.getWindowFn().windowCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      StatefulDoFnRunner.StateCleaner<?> stateCleaner =
          new StatefulDoFnRunner.StateInternalsStateCleaner<>(
              doFn, stepContext.stateInternals(), windowCoder);

      doFnRunner = DoFnRunners.defaultStatefulDoFnRunner(
          doFn,
          doFnRunner,
          windowingStrategy,
          cleanupTimer,
          stateCleaner);
    }

    if ((serializedOptions.get().as(FlinkPipelineOptions.class))
        .getEnableMetrics()) {
      doFnRunner = new DoFnRunnerWithMetricsUpdate<>(stepName, doFnRunner, getRuntimeContext());
    }

    pushbackDoFnRunner =
        SimplePushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);
  }

  @Override
  public void close() throws Exception {
    super.close();

    // sanity check: these should have been flushed out by +Inf watermarks
    if (pushbackStateInternals != null) {
      BagState<WindowedValue<InputT>> pushedBack =
          pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

      Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.read();
      if (pushedBackContents != null) {
        if (!Iterables.isEmpty(pushedBackContents)) {
          String pushedBackString = Joiner.on(",").join(pushedBackContents);
          throw new RuntimeException(
              "Leftover pushed-back data: " + pushedBackString + ". This indicates a bug.");
        }
      }
    }
    doFnInvoker.invokeTeardown();
  }

  protected final long getPushbackWatermarkHold() {
    // if we don't have side inputs we never hold the watermark
    if (sideInputs.isEmpty()) {
      return Long.MAX_VALUE;
    }

    try {
      checkInitPushedBackWatermark();
      return pushedBackWatermark.get();
    } catch (Exception e) {
      throw new RuntimeException("Error retrieving pushed back watermark state.", e);
    }
  }

  private void checkInitPushedBackWatermark() {
    // init and restore from pushedBack state.
    // Not done in initializeState, because OperatorState is not ready.
    if (!pushedBackWatermark.isPresent()) {

      BagState<WindowedValue<InputT>> pushedBack =
          pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

      long min = Long.MAX_VALUE;
      for (WindowedValue<InputT> value : pushedBack.read()) {
        min = Math.min(min, value.getTimestamp().getMillis());
      }
      setPushedBackWatermark(min);
    }
  }

  @Override
  public final void processElement(
      StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    doFnRunner.startBundle();
    doFnRunner.processElement(streamRecord.getValue());
    doFnRunner.finishBundle();
  }

  private void setPushedBackWatermark(long watermark) {
    pushedBackWatermark = Optional.fromNullable(watermark);
  }

  @Override
  public final void processElement1(
      StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    pushbackDoFnRunner.startBundle();
    Iterable<WindowedValue<InputT>> justPushedBack =
        pushbackDoFnRunner.processElementInReadyWindows(streamRecord.getValue());

    BagState<WindowedValue<InputT>> pushedBack =
        pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

    checkInitPushedBackWatermark();

    long min = pushedBackWatermark.get();
    for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
      min = Math.min(min, pushedBackValue.getTimestamp().getMillis());
      pushedBack.add(pushedBackValue);
    }
    setPushedBackWatermark(min);
    pushbackDoFnRunner.finishBundle();
  }

  @Override
  public final void processElement2(
      StreamRecord<RawUnionValue> streamRecord) throws Exception {
    pushbackDoFnRunner.startBundle();

    @SuppressWarnings("unchecked")
    WindowedValue<Iterable<?>> value =
        (WindowedValue<Iterable<?>>) streamRecord.getValue().getValue();

    PCollectionView<?> sideInput = sideInputTagMapping.get(streamRecord.getValue().getUnionTag());
    sideInputHandler.addSideInputValue(sideInput, value);

    BagState<WindowedValue<InputT>> pushedBack =
        pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

    List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();

    Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.read();
    if (pushedBackContents != null) {
      for (WindowedValue<InputT> elem : pushedBackContents) {

        // we need to set the correct key in case the operator is
        // a (keyed) window operator
        setKeyContextElement1(new StreamRecord<>(elem));

        Iterable<WindowedValue<InputT>> justPushedBack =
            pushbackDoFnRunner.processElementInReadyWindows(elem);
        Iterables.addAll(newPushedBack, justPushedBack);
      }
    }

    pushedBack.clear();
    long min = Long.MAX_VALUE;
    for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
      min = Math.min(min, pushedBackValue.getTimestamp().getMillis());
      pushedBack.add(pushedBackValue);
    }
    setPushedBackWatermark(min);

    pushbackDoFnRunner.finishBundle();

    // maybe output a new watermark
    processWatermark1(new Watermark(currentInputWatermark));
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    processWatermark1(mark);
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
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
      long potentialOutputWatermark =
          Math.min(getPushbackWatermarkHold(), currentInputWatermark);
      if (potentialOutputWatermark > currentOutputWatermark) {
        setCurrentOutputWatermark(potentialOutputWatermark);
        output.emitWatermark(new Watermark(currentOutputWatermark));
      }
    } else {
      // fireTimers, so we need startBundle.
      pushbackDoFnRunner.startBundle();

      setCurrentInputWatermark(mark.getTimestamp());

      // hold back by the pushed back values waiting for side inputs
      long pushedBackInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

      timerService.advanceWatermark(toFlinkRuntimeWatermark(pushedBackInputWatermark));

      Instant watermarkHold = stateInternals.watermarkHold();

      long combinedWatermarkHold = Math.min(watermarkHold.getMillis(), getPushbackWatermarkHold());

      long potentialOutputWatermark = Math.min(pushedBackInputWatermark, combinedWatermarkHold);

      if (potentialOutputWatermark > currentOutputWatermark) {
        setCurrentOutputWatermark(potentialOutputWatermark);
        output.emitWatermark(new Watermark(currentOutputWatermark));
      }
      pushbackDoFnRunner.finishBundle();
    }
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    setCurrentSideInputWatermark(mark.getTimestamp());
    if (mark.getTimestamp() >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // this means we will never see any more side input
      emitAllPushedBackData();

      // maybe output a new watermark
      processWatermark1(new Watermark(currentInputWatermark));
    }
  }

  /**
   * Converts a Beam watermark to a Flink watermark. This is only relevant when considering what
   * event-time timers to fire: in Beam, a watermark {@code T} says there will not be any elements
   * with a timestamp {@code < T} in the future. A Flink watermark {@code T} says there will not be
   * any elements with a timestamp {@code <= T} in the future. We correct this by subtracting
   * {@code 1} from a Beam watermark before passing to any relevant Flink runtime components.
   */
  private static long toFlinkRuntimeWatermark(long beamWatermark) {
    return beamWatermark - 1;
  }

  /**
   * Emits all pushed-back data. This should be used once we know that there will not be
   * any future side input, i.e. that there is no point in waiting.
   */
  private void emitAllPushedBackData() throws Exception {
    pushbackDoFnRunner.startBundle();

    BagState<WindowedValue<InputT>> pushedBack =
        pushbackStateInternals.state(StateNamespaces.global(), pushedBackTag);

    Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.read();
    if (pushedBackContents != null) {
      for (WindowedValue<InputT> elem : pushedBackContents) {

        // we need to set the correct key in case the operator is
        // a (keyed) window operator
        setKeyContextElement1(new StreamRecord<>(elem));

        doFnRunner.processElement(elem);
      }
    }

    pushedBack.clear();

    setPushedBackWatermark(Long.MAX_VALUE);

    pushbackDoFnRunner.finishBundle();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // copy from AbstractStreamOperator
    if (getKeyedStateBackend() != null) {
      KeyedStateCheckpointOutputStream out;

      try {
        out = context.getRawKeyedOperatorStateOutput();
      } catch (Exception exception) {
        throw new Exception("Could not open raw keyed operator state stream for "
            + getOperatorName() + '.', exception);
      }

      try {
        KeyGroupsList allKeyGroups = out.getKeyGroupList();
        for (int keyGroupIdx : allKeyGroups) {
          out.startNewKeyGroup(keyGroupIdx);

          DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);

          // if (this instanceof KeyGroupCheckpointedOperator)
          snapshotKeyGroupState(keyGroupIdx, dov);

          // We can't get all timerServices, so we just snapshot our timerService
          // Maybe this is a normal DoFn that has no timerService
          if (keyCoder != null) {
            timerService.snapshotTimersForKeyGroup(dov, keyGroupIdx);
          }

        }
      } catch (Exception exception) {
        throw new Exception("Could not write timer service of " + getOperatorName()
            + " to checkpoint state stream.", exception);
      } finally {
        try {
          out.close();
        } catch (Exception closeException) {
          LOG.warn("Could not close raw keyed operator state stream for {}. This "
              + "might have prevented deleting some state data.", getOperatorName(),
              closeException);
        }
      }
    }
  }

  @Override
  public void snapshotKeyGroupState(int keyGroupIndex, DataOutputStream out) throws Exception {
    if (!sideInputs.isEmpty() && keyCoder != null) {
      ((FlinkKeyGroupStateInternals) pushbackStateInternals).snapshotKeyGroupState(
          keyGroupIndex, out);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    if (getKeyedStateBackend() != null) {
      int totalKeyGroups = getKeyedStateBackend().getNumberOfKeyGroups();
      KeyGroupsList localKeyGroupRange = getKeyedStateBackend().getKeyGroupRange();

      for (KeyGroupStatePartitionStreamProvider streamProvider : context.getRawKeyedStateInputs()) {
        DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(streamProvider.getStream());

        int keyGroupIdx = streamProvider.getKeyGroupId();
        checkArgument(localKeyGroupRange.contains(keyGroupIdx),
            "Key Group " + keyGroupIdx + " does not belong to the local range.");

        // if (this instanceof KeyGroupRestoringOperator)
        restoreKeyGroupState(keyGroupIdx, div);

        // We just initialize our timerService
        if (keyCoder != null) {
          if (timerService == null) {
            timerService = new HeapInternalTimerService<>(
                totalKeyGroups,
                localKeyGroupRange,
                this,
                getRuntimeContext().getProcessingTimeService());
          }
          timerService.restoreTimersForKeyGroup(div, keyGroupIdx, getUserCodeClassloader());
        }
      }
    }
  }

  @Override
  public void restoreKeyGroupState(int keyGroupIndex, DataInputStream in) throws Exception {
    if (!sideInputs.isEmpty() && keyCoder != null) {
      if (pushbackStateInternals == null) {
        pushbackStateInternals = new FlinkKeyGroupStateInternals<>(keyCoder,
            getKeyedStateBackend());
      }
      ((FlinkKeyGroupStateInternals) pushbackStateInternals)
          .restoreKeyGroupState(keyGroupIndex, in, getUserCodeClassloader());
    }
  }

  @Override
  public void onEventTime(InternalTimer<Object, TimerData> timer) throws Exception {
    fireTimer(timer);
  }

  @Override
  public void onProcessingTime(InternalTimer<Object, TimerData> timer) throws Exception {
    fireTimer(timer);
  }

  // allow overriding this in WindowDoFnOperator
  public void fireTimer(InternalTimer<?, TimerData> timer) {
    TimerInternals.TimerData timerData = timer.getNamespace();
    StateNamespace namespace = timerData.getNamespace();
    // This is a user timer, so namespace must be WindowNamespace
    checkArgument(namespace instanceof WindowNamespace);
    BoundedWindow window = ((WindowNamespace) namespace).getWindow();
    pushbackDoFnRunner.onTimer(timerData.getTimerId(), window,
        timerData.getTimestamp(), timerData.getDomain());
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

  /**
   * Factory for creating an {@link DoFnRunners.OutputManager} from
   * a Flink {@link Output}.
   */
  interface OutputManagerFactory<OutputT> extends Serializable {
    DoFnRunners.OutputManager create(Output<StreamRecord<WindowedValue<OutputT>>> output);
  }

  /**
   * Default implementation of {@link OutputManagerFactory} that creates an
   * {@link DoFnRunners.OutputManager} that only writes to
   * a single logical output.
   */
  public static class DefaultOutputManagerFactory<OutputT>
      implements OutputManagerFactory<OutputT> {
    @Override
    public DoFnRunners.OutputManager create(
        final Output<StreamRecord<WindowedValue<OutputT>>> output) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
          // with tagged outputs we can't get around this because we don't
          // know our own output type...
          @SuppressWarnings("unchecked")
          WindowedValue<OutputT> castValue = (WindowedValue<OutputT>) value;
          output.collect(new StreamRecord<>(castValue));
        }
      };
    }
  }

  /**
   * Implementation of {@link OutputManagerFactory} that creates an
   * {@link DoFnRunners.OutputManager} that can write to multiple logical
   * outputs by unioning them in a {@link RawUnionValue}.
   */
  public static class MultiOutputOutputManagerFactory<OutputT>
      implements OutputManagerFactory<OutputT> {

    private TupleTag<?> mainTag;
    Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping;

    public MultiOutputOutputManagerFactory(
        TupleTag<?> mainTag,
        Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping) {
      this.mainTag = mainTag;
      this.mapping = mapping;
    }

    @Override
    public DoFnRunners.OutputManager create(
        final Output<StreamRecord<WindowedValue<OutputT>>> output) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
          if (tag.equals(mainTag)) {
            @SuppressWarnings("unchecked")
            WindowedValue<OutputT> outputValue = (WindowedValue<OutputT>) value;
            output.collect(new StreamRecord<>(outputValue));
          } else {
            @SuppressWarnings("unchecked")
            OutputTag<WindowedValue<T>> outputTag = (OutputTag) mapping.get(tag);
            output.<WindowedValue<T>>collect(outputTag, new StreamRecord<>(value));
          }
        }
      };
    }
  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does not allow
   * accessing state or timer internals.
   */
  protected class StepContext implements org.apache.beam.runners.core.StepContext {

    @Override
    public StateInternals stateInternals() {
      return stateInternals;
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

    /**
     * @deprecated use {@link #setTimer(StateNamespace, String, Instant, TimeDomain)}.
     */
    @Deprecated
    @Override
    public void setTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      switch (timerKey.getDomain()) {
        case EVENT_TIME:
          timerService.registerEventTimeTimer(timerKey, time);
          break;
        case PROCESSING_TIME:
        case SYNCHRONIZED_PROCESSING_TIME:
          timerService.registerProcessingTimeTimer(timerKey, time);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported time domain: " + timerKey.getDomain());
      }
    }

    /**
     * @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}.
     */
    @Deprecated
    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
      throw new UnsupportedOperationException(
          "Canceling of a timer by ID is not yet supported.");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      throw new UnsupportedOperationException(
          "Canceling of a timer by ID is not yet supported.");
    }

    /**
     * @deprecated use {@link #deleteTimer(StateNamespace, String, TimeDomain)}.
     */
    @Deprecated
    @Override
    public void deleteTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      switch (timerKey.getDomain()) {
        case EVENT_TIME:
          timerService.deleteEventTimeTimer(timerKey, time);
          break;
        case PROCESSING_TIME:
        case SYNCHRONIZED_PROCESSING_TIME:
          timerService.deleteProcessingTimeTimer(timerKey, time);
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
  }
}
