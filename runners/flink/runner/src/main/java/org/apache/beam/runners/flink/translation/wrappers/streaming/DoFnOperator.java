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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.joda.time.Instant;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT> the input type of the {@link DoFn}
 * @param <FnOutputT> the output type of the {@link DoFn}
 * @param <OutputT> the output type of the operator, this can be different from the fn output
 *                 type when we have side outputs
 */
public class DoFnOperator<InputT, FnOutputT, OutputT>
    extends AbstractStreamOperator<OutputT>
    implements OneInputStreamOperator<WindowedValue<InputT>, OutputT>,
      TwoInputStreamOperator<WindowedValue<InputT>, RawUnionValue, OutputT>,
      Triggerable {

  protected DoFn<InputT, FnOutputT> doFn;

  protected final SerializedPipelineOptions serializedOptions;

  protected final TupleTag<FnOutputT> mainOutputTag;
  protected final List<TupleTag<?>> sideOutputTags;

  protected final Collection<PCollectionView<?>> sideInputs;
  protected final Map<Integer, PCollectionView<?>> sideInputTagMapping;

  protected final WindowingStrategy<?, ?> windowingStrategy;

  protected final OutputManagerFactory<OutputT> outputManagerFactory;

  protected transient PushbackSideInputDoFnRunner<InputT, FnOutputT> pushbackDoFnRunner;

  protected transient SideInputHandler sideInputHandler;

  protected transient SideInputReader sideInputReader;

  protected transient DoFnRunners.OutputManager outputManager;

  private transient DoFnInvoker<InputT, FnOutputT> doFnInvoker;

  protected transient long currentInputWatermark;

  protected transient long currentOutputWatermark;

  private transient AbstractStateBackend sideInputStateBackend;

  private final ReducingStateDescriptor<Long> pushedBackWatermarkDescriptor;

  private final ListStateDescriptor<WindowedValue<InputT>> pushedBackDescriptor;

  private transient Map<String, KvStateSnapshot<?, ?, ?, ?, ?>> restoredSideInputState;

  protected transient FlinkStateInternals<?> stateInternals;

  // statefulDoFn/windowDoFn or normal doFn
  private final Coder<?> keyCoder;

  protected transient FlinkTimerInternals timerInternals;

  private final TimerInternals.TimerDataCoder timerCoder;

  private transient Set<Tuple2<ByteBuffer, TimerInternals.TimerData>> watermarkTimers;
  private transient Queue<Tuple2<ByteBuffer, TimerInternals.TimerData>> watermarkTimersQueue;

  private transient Queue<Tuple2<ByteBuffer, TimerInternals.TimerData>> processingTimeTimersQueue;
  private transient Set<Tuple2<ByteBuffer, TimerInternals.TimerData>> processingTimeTimers;
  private transient Multiset<Long> processingTimeTimerTimestamps;
  private transient Map<Long, ScheduledFuture<?>> processingTimeTimerFutures;

  public DoFnOperator(
      DoFn<InputT, FnOutputT> doFn,
      TypeInformation<WindowedValue<InputT>> inputType,
      TupleTag<FnOutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    this.doFn = doFn;
    this.mainOutputTag = mainOutputTag;
    this.sideOutputTags = sideOutputTags;
    this.sideInputTagMapping = sideInputTagMapping;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;

    this.pushedBackWatermarkDescriptor =
        new ReducingStateDescriptor<>(
            "pushed-back-elements-watermark-hold",
            new LongMinReducer(),
            LongSerializer.INSTANCE);

    this.pushedBackDescriptor =
        new ListStateDescriptor<>("pushed-back-values", inputType);

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
    currentOutputWatermark = currentInputWatermark;

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
      String operatorIdentifier =
          this.getClass().getSimpleName() + "_"
              + getRuntimeContext().getIndexOfThisSubtask() + "_sideInput";

      sideInputStateBackend = this
          .getContainingTask()
          .createStateBackend(operatorIdentifier,
              new GenericTypeInfo<>(ByteBuffer.class).createSerializer(new ExecutionConfig()));

      checkState(sideInputStateBackend != null, "Side input state backend cannot be null");

      if (restoredSideInputState != null) {
        @SuppressWarnings("unchecked,rawtypes")
        HashMap<String, KvStateSnapshot> castRestored = (HashMap) restoredSideInputState;
        sideInputStateBackend.injectKeyValueStateSnapshots(castRestored);
        restoredSideInputState = null;
      }

      sideInputStateBackend.setCurrentKey(
          ByteBuffer.wrap(CoderUtils.encodeToByteArray(VoidCoder.of(), null)));

      StateInternals<Void> sideInputStateInternals =
          new FlinkStateInternals<>(sideInputStateBackend, VoidCoder.of());

      sideInputHandler = new SideInputHandler(sideInputs, sideInputStateInternals);
      sideInputReader = sideInputHandler;
    }

    outputManager = outputManagerFactory.create(output);

    if (keyCoder != null) {
      stateInternals = new FlinkStateInternals<>(getStateBackend(), keyCoder);

      // might already be initialized from restoreTimers()
      if (watermarkTimers == null) {
        watermarkTimers = new HashSet<>();

        watermarkTimersQueue = new PriorityQueue<>(
            10,
            new Comparator<Tuple2<ByteBuffer, TimerInternals.TimerData>>() {
              @Override
              public int compare(
                  Tuple2<ByteBuffer, TimerInternals.TimerData> o1,
                  Tuple2<ByteBuffer, TimerInternals.TimerData> o2) {
                return o1.f1.compareTo(o2.f1);
              }
            });
      }

      if (processingTimeTimers == null) {
        processingTimeTimers = new HashSet<>();
        processingTimeTimerTimestamps = HashMultiset.create();
        processingTimeTimersQueue = new PriorityQueue<>(
            10,
            new Comparator<Tuple2<ByteBuffer, TimerInternals.TimerData>>() {
              @Override
              public int compare(
                  Tuple2<ByteBuffer, TimerInternals.TimerData> o1,
                  Tuple2<ByteBuffer, TimerInternals.TimerData> o2) {
                return o1.f1.compareTo(o2.f1);
              }
            });
      }

      // ScheduledFutures are not checkpointed
      processingTimeTimerFutures = new HashMap<>();

      timerInternals = new FlinkTimerInternals();
    }

    // getDoFn() requires stateInternals and timerInternals to be inited
    this.doFn = getDoFn();

    doFnInvoker = DoFnInvokers.invokerFor(doFn);

    doFnInvoker.invokeSetup();

    DoFnRunner<InputT, FnOutputT> doFnRunner = DoFnRunners.simpleRunner(
        serializedOptions.getPipelineOptions(),
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        sideOutputTags,
        createStepContext(),
        aggregatorFactory,
        windowingStrategy);

    pushbackDoFnRunner =
        PushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);

  }

  @Override
  public void close() throws Exception {
    super.close();
    doFnInvoker.invokeTeardown();
  }

  private long getPushbackWatermarkHold() {
    // if we don't have side inputs we never hold the watermark
    if (sideInputs.isEmpty()) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    }

    try {
      Long result = sideInputStateBackend.getPartitionedState(
          null,
          VoidSerializer.INSTANCE,
          pushedBackWatermarkDescriptor).get();
      return result != null ? result : BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    } catch (Exception e) {
      throw new RuntimeException("Error retrieving pushed back watermark state.", e);
    }
  }

  @Override
  public final void processElement(
      StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    pushbackDoFnRunner.startBundle();
    pushbackDoFnRunner.processElement(streamRecord.getValue());
    pushbackDoFnRunner.finishBundle();
  }

  @Override
  public final void processElement1(
      StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    pushbackDoFnRunner.startBundle();
    Iterable<WindowedValue<InputT>> justPushedBack =
        pushbackDoFnRunner.processElementInReadyWindows(streamRecord.getValue());

    ListState<WindowedValue<InputT>> pushedBack =
        sideInputStateBackend.getPartitionedState(
            null,
            VoidSerializer.INSTANCE,
            pushedBackDescriptor);

    ReducingState<Long> pushedBackWatermark =
        sideInputStateBackend.getPartitionedState(
            null,
            VoidSerializer.INSTANCE,
            pushedBackWatermarkDescriptor);

    for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
      pushedBackWatermark.add(pushedBackValue.getTimestamp().getMillis());
      pushedBack.add(pushedBackValue);
    }
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

    ListState<WindowedValue<InputT>> pushedBack =
        sideInputStateBackend.getPartitionedState(
            null,
            VoidSerializer.INSTANCE,
            pushedBackDescriptor);

    List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();

    Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.get();
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

    ReducingState<Long> pushedBackWatermark =
        sideInputStateBackend.getPartitionedState(
            null,
            VoidSerializer.INSTANCE,
            pushedBackWatermarkDescriptor);

    pushedBack.clear();
    pushedBackWatermark.clear();
    for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
      pushedBackWatermark.add(pushedBackValue.getTimestamp().getMillis());
      pushedBack.add(pushedBackValue);
    }

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
      pushbackDoFnRunner.startBundle();

      this.currentInputWatermark = mark.getTimestamp();

      // hold back by the pushed back values waiting for side inputs
      long actualInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

      consumeTimers(watermarkTimersQueue, watermarkTimers, actualInputWatermark);

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

  private void consumeTimers(Queue<Tuple2<ByteBuffer, TimerInternals.TimerData>> queue,
                             Set<Tuple2<ByteBuffer, TimerInternals.TimerData>> set,
                             long time) throws Exception {
    boolean fire;
    do {
      Tuple2<ByteBuffer, TimerInternals.TimerData> timer = queue.peek();
      if (timer != null && timer.f1.getTimestamp().getMillis() < time) {
        fire = true;

        queue.remove();
        set.remove(timer);

        setKeyContext(timer.f0);

        fireTimer(timer);

      } else {
        fire = false;
      }
    } while (fire);
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    // ignore watermarks from the side-input input
  }

  @Override
  public StreamTaskState snapshotOperatorState(
      long checkpointId,
      long timestamp) throws Exception {

    StreamTaskState streamTaskState = super.snapshotOperatorState(checkpointId, timestamp);

    if (sideInputStateBackend != null) {
      // we have to manually checkpoint the side-input state backend and store
      // the handle in the "user state" of the task state
      HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> sideInputSnapshot =
          sideInputStateBackend.snapshotPartitionedState(checkpointId, timestamp);

      if (sideInputSnapshot != null) {
        @SuppressWarnings("unchecked,rawtypes")
        StateHandle<Serializable> sideInputStateHandle =
            (StateHandle) sideInputStateBackend.checkpointStateSerializable(
                sideInputSnapshot, checkpointId, timestamp);

        streamTaskState.setFunctionState(sideInputStateHandle);
      }
    }

    if (keyCoder != null) {
      AbstractStateBackend.CheckpointStateOutputView outputView =
          getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

      snapshotTimers(outputView);

      StateHandle<DataInputView> handle = outputView.closeAndGetHandle();

      // this might overwrite stuff that super checkpointed
      streamTaskState.setOperatorState(handle);
    }

    return streamTaskState;
  }

  @Override
  public void restoreState(StreamTaskState state) throws Exception {
    super.restoreState(state);

    @SuppressWarnings("unchecked,rawtypes")
    StateHandle<HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>>> sideInputStateHandle =
        (StateHandle) state.getFunctionState();

    if (sideInputStateHandle != null) {
      restoredSideInputState = sideInputStateHandle.getState(getUserCodeClassloader());
    }

    if (keyCoder != null) {
      @SuppressWarnings("unchecked")
      StateHandle<DataInputView> operatorState =
          (StateHandle<DataInputView>) state.getOperatorState();

      DataInputView in = operatorState.getState(getUserCodeClassloader());

      restoreTimers(new DataInputViewWrapper(in));
    }
  }

  private void restoreTimers(InputStream in) throws IOException {
    DataInputStream dataIn = new DataInputStream(in);

    int numWatermarkTimers = dataIn.readInt();

    watermarkTimers = new HashSet<>(numWatermarkTimers);

    watermarkTimersQueue = new PriorityQueue<>(
        Math.max(numWatermarkTimers, 1),
        new Comparator<Tuple2<ByteBuffer, TimerInternals.TimerData>>() {
          @Override
          public int compare(
              Tuple2<ByteBuffer, TimerInternals.TimerData> o1,
              Tuple2<ByteBuffer, TimerInternals.TimerData> o2) {
            return o1.f1.compareTo(o2.f1);
          }
        });

    for (int i = 0; i < numWatermarkTimers; i++) {
      int length = dataIn.readInt();
      byte[] keyBytes = new byte[length];
      dataIn.readFully(keyBytes);
      TimerInternals.TimerData timerData = timerCoder.decode(dataIn, Coder.Context.NESTED);
      Tuple2<ByteBuffer, TimerInternals.TimerData> keyedTimer =
          new Tuple2<>(ByteBuffer.wrap(keyBytes), timerData);
      if (watermarkTimers.add(keyedTimer)) {
        watermarkTimersQueue.add(keyedTimer);
      }
    }

    int numProcessingTimeTimers = dataIn.readInt();

    processingTimeTimers = new HashSet<>(numProcessingTimeTimers);
    processingTimeTimersQueue = new PriorityQueue<>(
        Math.max(numProcessingTimeTimers, 1),
        new Comparator<Tuple2<ByteBuffer, TimerInternals.TimerData>>() {
          @Override
          public int compare(
              Tuple2<ByteBuffer, TimerInternals.TimerData> o1,
              Tuple2<ByteBuffer, TimerInternals.TimerData> o2) {
            return o1.f1.compareTo(o2.f1);
          }
        });

    processingTimeTimerTimestamps = HashMultiset.create();
    processingTimeTimerFutures = new HashMap<>();

    for (int i = 0; i < numProcessingTimeTimers; i++) {
      int length = dataIn.readInt();
      byte[] keyBytes = new byte[length];
      dataIn.readFully(keyBytes);
      TimerInternals.TimerData timerData = timerCoder.decode(dataIn, Coder.Context.NESTED);
      Tuple2<ByteBuffer, TimerInternals.TimerData> keyedTimer =
          new Tuple2<>(ByteBuffer.wrap(keyBytes), timerData);
      if (processingTimeTimers.add(keyedTimer)) {
        processingTimeTimersQueue.add(keyedTimer);

        //If this is the first timer added for this timestamp register a timer Task
        if (processingTimeTimerTimestamps.add(timerData.getTimestamp().getMillis(), 1) == 0) {
          // this registers a timer with the Flink processing-time service
          ScheduledFuture<?> scheduledFuture =
              registerTimer(timerData.getTimestamp().getMillis(), this);
          processingTimeTimerFutures.put(timerData.getTimestamp().getMillis(), scheduledFuture);
        }

      }
    }
  }

  private void snapshotTimers(OutputStream out) throws IOException {
    DataOutputStream dataOut = new DataOutputStream(out);
    dataOut.writeInt(watermarkTimersQueue.size());
    for (Tuple2<ByteBuffer, TimerInternals.TimerData> timer : watermarkTimersQueue) {
      dataOut.writeInt(timer.f0.limit());
      dataOut.write(timer.f0.array(), 0, timer.f0.limit());
      timerCoder.encode(timer.f1, dataOut, Coder.Context.NESTED);
    }

    dataOut.writeInt(processingTimeTimersQueue.size());
    for (Tuple2<ByteBuffer, TimerInternals.TimerData> timer : processingTimeTimersQueue) {
      dataOut.writeInt(timer.f0.limit());
      dataOut.write(timer.f0.array(), 0, timer.f0.limit());
      timerCoder.encode(timer.f1, dataOut, Coder.Context.NESTED);
    }
  }

  private void registerEventTimeTimer(TimerInternals.TimerData timer) {
    Tuple2<ByteBuffer, TimerInternals.TimerData> keyedTimer =
        new Tuple2<>((ByteBuffer) getStateBackend().getCurrentKey(), timer);
    if (watermarkTimers.add(keyedTimer)) {
      watermarkTimersQueue.add(keyedTimer);
    }
  }

  private void deleteEventTimeTimer(TimerInternals.TimerData timer) {
    Tuple2<ByteBuffer, TimerInternals.TimerData> keyedTimer =
        new Tuple2<>((ByteBuffer) getStateBackend().getCurrentKey(), timer);
    if (watermarkTimers.remove(keyedTimer)) {
      watermarkTimersQueue.remove(keyedTimer);
    }
  }

  private void registerProcessingTimeTimer(TimerInternals.TimerData timer) {
    Tuple2<ByteBuffer, TimerInternals.TimerData> keyedTimer =
        new Tuple2<>((ByteBuffer) getStateBackend().getCurrentKey(), timer);
    if (processingTimeTimers.add(keyedTimer)) {
      processingTimeTimersQueue.add(keyedTimer);

      // If this is the first timer added for this timestamp register a timer Task
      if (processingTimeTimerTimestamps.add(timer.getTimestamp().getMillis(), 1) == 0) {
        ScheduledFuture<?> scheduledFuture = registerTimer(timer.getTimestamp().getMillis(), this);
        processingTimeTimerFutures.put(timer.getTimestamp().getMillis(), scheduledFuture);
      }
    }
  }

  private void deleteProcessingTimeTimer(TimerInternals.TimerData timer) {
    Tuple2<ByteBuffer, TimerInternals.TimerData> keyedTimer =
        new Tuple2<>((ByteBuffer) getStateBackend().getCurrentKey(), timer);
    if (processingTimeTimers.remove(keyedTimer)) {
      processingTimeTimersQueue.remove(keyedTimer);

      // If there are no timers left for this timestamp, remove it from queue and cancel the
      // timer Task
      if (processingTimeTimerTimestamps.remove(timer.getTimestamp().getMillis(), 1) == 1) {
        ScheduledFuture<?> triggerTaskFuture =
            processingTimeTimerFutures.remove(timer.getTimestamp().getMillis());
        if (triggerTaskFuture != null && !triggerTaskFuture.isDone()) {
          triggerTaskFuture.cancel(false);
        }
      }

    }
  }

  @Override
  public void trigger(long time) throws Exception {

    if (keyCoder != null) {
      //Remove information about the triggering task
      processingTimeTimerFutures.remove(time);
      processingTimeTimerTimestamps.setCount(time, 0);

      consumeTimers(processingTimeTimersQueue, processingTimeTimers, time);

    } else {
      throw new RuntimeException("unexpected trigger: " + time);
    }

  }

  protected void fireTimer(Tuple2<ByteBuffer, TimerInternals.TimerData> timer) {
    TimerInternals.TimerData timerData = timer.f1;
    pushbackDoFnRunner.onTimer(timerData.getTimerId(), GlobalWindow.INSTANCE,
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
          // with side outputs we can't get around this because we don't
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
   * For determining the pushback watermark in a {@link ReducingStateDescriptor}.
   */
  private static class LongMinReducer implements ReduceFunction<Long> {
    @Override
    public Long reduce(Long a, Long b) throws Exception {
      return Math.min(a, b);
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
    public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {}

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
      if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
        registerEventTimeTimer(timerKey);
      } else if (timerKey.getDomain().equals(TimeDomain.PROCESSING_TIME)) {
        registerProcessingTimeTimer(timerKey);
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
      if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
        deleteEventTimeTimer(timerKey);
      } else if (timerKey.getDomain().equals(TimeDomain.PROCESSING_TIME)) {
        deleteProcessingTimeTimer(timerKey);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported time domain: " + timerKey.getDomain());
      }
    }

    @Override
    public Instant currentProcessingTime() {
      return new Instant(getCurrentProcessingTime());
    }

    @Nullable
    @Override
    public Instant currentSynchronizedProcessingTime() {
      return new Instant(getCurrentProcessingTime());
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
