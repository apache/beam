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


import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetDoFn;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.joda.time.Instant;

/**
 * Flink operator for executing window {@link DoFn DoFns}.
 *
 * @param <InputT>
 * @param <OutputT>
 */
public class WindowDoFnOperator<K, InputT, OutputT>
    extends DoFnOperator<KeyedWorkItem<K, InputT>, KV<K, OutputT>, WindowedValue<KV<K, OutputT>>>
    implements Triggerable {

  private final Coder<K> keyCoder;
  private final TimerInternals.TimerDataCoder timerCoder;

  private transient Set<Tuple2<ByteBuffer, TimerInternals.TimerData>> watermarkTimers;
  private transient Queue<Tuple2<ByteBuffer, TimerInternals.TimerData>> watermarkTimersQueue;

  private transient Queue<Tuple2<ByteBuffer, TimerInternals.TimerData>> processingTimeTimersQueue;
  private transient Set<Tuple2<ByteBuffer, TimerInternals.TimerData>> processingTimeTimers;
  private transient Multiset<Long> processingTimeTimerTimestamps;
  private transient Map<Long, ScheduledFuture<?>> processingTimeTimerFutures;

  private FlinkStateInternals<K> stateInternals;

  private final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn;

  public WindowDoFnOperator(
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn,
      TypeInformation<WindowedValue<KeyedWorkItem<K, InputT>>> inputType,
      TupleTag<KV<K, OutputT>> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      OutputManagerFactory<WindowedValue<KV<K, OutputT>>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<K> keyCoder) {
    super(
        null,
        inputType,
        mainOutputTag,
        sideOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options);

    this.systemReduceFn = systemReduceFn;

    this.keyCoder = keyCoder;
    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());
  }

  @Override
  protected OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> getDoFn() {
    StateInternalsFactory<K> stateInternalsFactory = new StateInternalsFactory<K>() {
      @Override
      public StateInternals<K> stateInternalsForKey(K key) {
        //this will implicitly be keyed by the key of the incoming
        // element or by the key of a firing timer
        return stateInternals;
      }
    };

    // we have to do the unchecked cast because GroupAlsoByWindowViaWindowSetDoFn.create
    // has the window type as generic parameter while WindowingStrategy is almost always
    // untyped.
    @SuppressWarnings("unchecked")
    OldDoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFn =
        GroupAlsoByWindowViaWindowSetDoFn.create(
            windowingStrategy, stateInternalsFactory, (SystemReduceFn) systemReduceFn);
    return doFn;
  }


  @Override
  public void open() throws Exception {

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

    stateInternals = new FlinkStateInternals<>(getStateBackend(), keyCoder);

    // call super at the end because this will call getDoFn() which requires stateInternals
    // to be set
    super.open();
  }

  @Override
  protected ExecutionContext.StepContext createStepContext() {
    return new WindowDoFnOperator.StepContext();
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

    //Remove information about the triggering task
    processingTimeTimerFutures.remove(time);
    processingTimeTimerTimestamps.setCount(time, 0);

    boolean fire;

    do {
      Tuple2<ByteBuffer, TimerInternals.TimerData> timer = processingTimeTimersQueue.peek();
      if (timer != null && timer.f1.getTimestamp().getMillis() <= time) {
        fire = true;

        processingTimeTimersQueue.remove();
        processingTimeTimers.remove(timer);

        setKeyContext(timer.f0);

        pushbackDoFnRunner.processElement(WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.<K, InputT>timersWorkItem(
                stateInternals.getKey(),
                Collections.singletonList(timer.f1))));

      } else {
        fire = false;
      }
    } while (fire);

  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    processWatermark1(mark);
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
    pushbackDoFnRunner.startBundle();

    this.currentInputWatermark = mark.getTimestamp();

    // hold back by the pushed back values waiting for side inputs
    long actualInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

    boolean fire;

    do {
      Tuple2<ByteBuffer, TimerInternals.TimerData> timer = watermarkTimersQueue.peek();
      if (timer != null && timer.f1.getTimestamp().getMillis() < actualInputWatermark) {
        fire = true;

        watermarkTimersQueue.remove();
        watermarkTimers.remove(timer);

        setKeyContext(timer.f0);

        pushbackDoFnRunner.processElement(WindowedValue.valueInGlobalWindow(
                KeyedWorkItems.<K, InputT>timersWorkItem(
                    stateInternals.getKey(),
                    Collections.singletonList(timer.f1))));

      } else {
        fire = false;
      }
    } while (fire);

    Instant watermarkHold = stateInternals.watermarkHold();

    long combinedWatermarkHold = Math.min(watermarkHold.getMillis(), getPushbackWatermarkHold());

    long potentialOutputWatermark = Math.min(currentInputWatermark, combinedWatermarkHold);

    if (potentialOutputWatermark > currentOutputWatermark) {
      currentOutputWatermark = potentialOutputWatermark;
      output.emitWatermark(new Watermark(currentOutputWatermark));
    }
    pushbackDoFnRunner.finishBundle();

  }

  @Override
  public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
    StreamTaskState result = super.snapshotOperatorState(checkpointId, timestamp);

    AbstractStateBackend.CheckpointStateOutputView outputView =
        getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

    snapshotTimers(outputView);

    StateHandle<DataInputView> handle = outputView.closeAndGetHandle();

    // this might overwrite stuff that super checkpointed
    result.setOperatorState(handle);

    return result;
  }

  @Override
  public void restoreState(StreamTaskState state) throws Exception {
    super.restoreState(state);

    @SuppressWarnings("unchecked")
    StateHandle<DataInputView> operatorState =
        (StateHandle<DataInputView>) state.getOperatorState();

    DataInputView in = operatorState.getState(getUserCodeClassloader());

    restoreTimers(new DataInputViewWrapper(in));
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

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does now allow
   * accessing state or timer internals.
   */
  protected class StepContext extends DoFnOperator.StepContext {

    @Override
    public TimerInternals timerInternals() {
      return new TimerInternals() {
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
      };
    }
  }

}
