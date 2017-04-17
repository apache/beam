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

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
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
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkBroadcastStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkKeyGroupStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkSplitStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.KeyGroupCheckpointedOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
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
import org.joda.time.Instant;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT> the input type of the {@link DoFn}
 * @param <FnOutputT> the output type of the {@link DoFn}
 * @param <OutputT> the output type of the operator, this can be different from the fn output
 *                 type when we have additional tagged outputs
 */
public class DoFnOperator<InputT, FnOutputT, OutputT>
    extends AbstractStreamOperator<OutputT>
    implements OneInputStreamOperator<WindowedValue<InputT>, OutputT>,
      TwoInputStreamOperator<WindowedValue<InputT>, RawUnionValue, OutputT>,
    KeyGroupCheckpointedOperator, Triggerable<Object, TimerData> {

  protected DoFn<InputT, FnOutputT> doFn;

  protected final SerializedPipelineOptions serializedOptions;

  protected final TupleTag<FnOutputT> mainOutputTag;
  protected final List<TupleTag<?>> additionalOutputTags;

  protected final Collection<PCollectionView<?>> sideInputs;
  protected final Map<Integer, PCollectionView<?>> sideInputTagMapping;

  protected final WindowingStrategy<?, ?> windowingStrategy;

  protected final OutputManagerFactory<OutputT> outputManagerFactory;

  protected transient DoFnRunner<InputT, FnOutputT> doFnRunner;
  protected transient PushbackSideInputDoFnRunner<InputT, FnOutputT> pushbackDoFnRunner;

  protected transient SideInputHandler sideInputHandler;

  protected transient SideInputReader sideInputReader;

  protected transient DoFnRunners.OutputManager outputManager;

  private transient DoFnInvoker<InputT, FnOutputT> doFnInvoker;

  protected transient long currentInputWatermark;

  protected transient long currentOutputWatermark;

  private transient StateTag<Object, BagState<WindowedValue<InputT>>> pushedBackTag;

  protected transient FlinkStateInternals<?> stateInternals;

  private Coder<WindowedValue<InputT>> inputCoder;

  private final Coder<?> keyCoder;

  private final TimerInternals.TimerDataCoder timerCoder;

  protected transient HeapInternalTimerService<?, TimerInternals.TimerData> timerService;

  protected transient FlinkTimerInternals timerInternals;

  private transient StateInternals<?> pushbackStateInternals;

  private transient Optional<Long> pushedBackWatermark;

  public DoFnOperator(
      DoFn<InputT, FnOutputT> doFn,
      Coder<WindowedValue<InputT>> inputCoder,
      TupleTag<FnOutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputTagMapping = sideInputTagMapping;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;

    setChainingStrategy(ChainingStrategy.ALWAYS);

    this.keyCoder = keyCoder;

    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());
  }

  private ExecutionContext.StepContext createStepContext() {
    return new StepContext();
  }

  // allow overriding this in WindowDoFnOperator because this one dynamically creates
  // the DoFn
  protected DoFn<InputT, FnOutputT> getDoFn() {
    return doFn;
  }

  @Override
  public void open() throws Exception {
    super.open();

    currentInputWatermark = Long.MIN_VALUE;
    currentOutputWatermark = Long.MIN_VALUE;

    AggregatorFactory aggregatorFactory = new AggregatorFactory() {
      @Override
      public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
          Class<?> fnClass,
          ExecutionContext.StepContext stepContext,
          String aggregatorName,
          Combine.CombineFn<InputT, AccumT, OutputT> combine) {

        @SuppressWarnings("unchecked")
        SerializableFnAggregatorWrapper<InputT, OutputT> result =
            (SerializableFnAggregatorWrapper<InputT, OutputT>)
                getRuntimeContext().getAccumulator(aggregatorName);

        if (result == null) {
          result = new SerializableFnAggregatorWrapper<>(combine);
          getRuntimeContext().addAccumulator(aggregatorName, result);
        }
        return result;
      }
    };

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

    ExecutionContext.StepContext stepContext = createStepContext();

    doFnRunner = DoFnRunners.simpleRunner(
        serializedOptions.getPipelineOptions(),
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        aggregatorFactory,
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
          windowingStrategy,
          ((GroupAlsoByWindowViaWindowSetNewDoFn) doFn).getDroppedDueToLatenessAggregator());
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
          stepContext,
          aggregatorFactory,
          windowingStrategy,
          cleanupTimer,
          stateCleaner);
    }

    pushbackDoFnRunner =
        SimplePushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);
  }

  @Override
  public void close() throws Exception {
    super.close();
    doFnInvoker.invokeTeardown();
  }

  protected final long getPushbackWatermarkHold() {
    // if we don't have side inputs we never hold the watermark
    if (sideInputs.isEmpty()) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
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

      long min = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
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
    long min = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
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
    if (keyCoder == null) {
      this.currentInputWatermark = mark.getTimestamp();
      long potentialOutputWatermark =
          Math.min(getPushbackWatermarkHold(), currentInputWatermark);
      if (potentialOutputWatermark > currentOutputWatermark) {
        currentOutputWatermark = potentialOutputWatermark;
        output.emitWatermark(new Watermark(currentOutputWatermark));
      }
    } else {
      // fireTimers, so we need startBundle.
      pushbackDoFnRunner.startBundle();

      this.currentInputWatermark = mark.getTimestamp();

      // hold back by the pushed back values waiting for side inputs
      long actualInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

      timerService.advanceWatermark(actualInputWatermark);

      Instant watermarkHold = stateInternals.watermarkHold();

      long combinedWatermarkHold = Math.min(watermarkHold.getMillis(), getPushbackWatermarkHold());

      long potentialOutputWatermark = Math.min(currentInputWatermark, combinedWatermarkHold);

      if (potentialOutputWatermark > currentOutputWatermark) {
        currentOutputWatermark = potentialOutputWatermark;
        output.emitWatermark(new Watermark(currentOutputWatermark));
      }
      pushbackDoFnRunner.finishBundle();
    }
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    // ignore watermarks from the side-input input
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

  /**
   * Factory for creating an {@link DoFnRunners.OutputManager} from
   * a Flink {@link Output}.
   */
  interface OutputManagerFactory<OutputT> extends Serializable {
    DoFnRunners.OutputManager create(Output<StreamRecord<OutputT>> output);
  }

  /**
   * Default implementation of {@link OutputManagerFactory} that creates an
   * {@link DoFnRunners.OutputManager} that only writes to
   * a single logical output.
   */
  public static class DefaultOutputManagerFactory<OutputT>
      implements OutputManagerFactory<OutputT> {
    @Override
    public DoFnRunners.OutputManager create(final Output<StreamRecord<OutputT>> output) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
          // with tagged outputs we can't get around this because we don't
          // know our own output type...
          @SuppressWarnings("unchecked")
          OutputT castValue = (OutputT) value;
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
  public static class MultiOutputOutputManagerFactory
      implements OutputManagerFactory<RawUnionValue> {

    Map<TupleTag<?>, Integer> mapping;

    public MultiOutputOutputManagerFactory(Map<TupleTag<?>, Integer> mapping) {
      this.mapping = mapping;
    }

    @Override
    public DoFnRunners.OutputManager create(final Output<StreamRecord<RawUnionValue>> output) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
          int intTag = mapping.get(tag);
          output.collect(new StreamRecord<>(new RawUnionValue(intTag, value)));
        }
      };
    }
  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does not allow
   * accessing state or timer internals.
   */
  protected class StepContext implements ExecutionContext.StepContext {

    @Override
    public String getStepName() {
      return null;
    }

    @Override
    public String getTransformName() {
      return null;
    }

    @Override
    public void noteOutput(WindowedValue<?> output) {}

    @Override
    public void noteOutput(TupleTag<?> tag, WindowedValue<?> output) {}

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<WindowedValue<T>> data,
        Coder<Iterable<WindowedValue<T>>> dataCoder,
        W window,
        Coder<W> windowCoder) throws IOException {
      throw new UnsupportedOperationException("Writing side-input data is not supported.");
    }

    @Override
    public StateInternals<?> stateInternals() {
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

    @Deprecated
    @Override
    public void setTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
        timerService.registerEventTimeTimer(timerKey, time);
      } else if (timerKey.getDomain().equals(TimeDomain.PROCESSING_TIME)) {
        timerService.registerProcessingTimeTimer(timerKey, time);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported time domain: " + timerKey.getDomain());
      }
    }

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

    @Deprecated
    @Override
    public void deleteTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
        timerService.deleteEventTimeTimer(timerKey, time);
      } else if (timerKey.getDomain().equals(TimeDomain.PROCESSING_TIME)) {
        timerService.deleteProcessingTimeTimer(timerKey, time);
      } else {
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
