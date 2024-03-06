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

import static org.apache.beam.runners.core.StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_TIMER_ID;
import static org.apache.beam.runners.flink.translation.utils.FlinkPortableRunnerUtils.requiresStableInput;
import static org.apache.beam.runners.flink.translation.utils.FlinkPortableRunnerUtils.requiresTimeSortedInput;
import static org.apache.flink.util.Preconditions.checkNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContextFactory;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.utils.Locker;
import org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput.BufferingDoFnRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandler;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandlers;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandlers.StateAndTimerBundleCheckpointHandler;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandler;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandlers;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandlers.InMemoryFinalizer;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.StreamingSideInputHandlerFactory;
import org.apache.beam.runners.fnexecution.wire.ByteStringCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.UserStateReference;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator is the streaming equivalent of the {@link
 * org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction}. It sends all
 * received elements to the SDK harness and emits the received back elements to the downstream
 * operators. It also takes care of handling side inputs and state.
 *
 * <p>TODO Integrate support for progress updates and metrics
 */
// We use Flink's lifecycle methods to initialize transient fields
@SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ExecutableStageDoFnOperator<InputT, OutputT> extends DoFnOperator<InputT, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutableStageDoFnOperator.class);

  private final RunnerApi.ExecutableStagePayload payload;
  private final JobInfo jobInfo;
  private final FlinkExecutableStageContextFactory contextFactory;
  private final Map<String, TupleTag<?>> outputMap;
  private final Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds;
  /** A lock which has to be acquired when concurrently accessing state and timers. */
  private final ReentrantLock stateBackendLock;

  private final SerializablePipelineOptions pipelineOptions;

  private final boolean isStateful;

  private final Coder windowCoder;
  private final Coder<WindowedValue<InputT>> inputCoder;

  private transient ExecutableStageContext stageContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient BundleProgressHandler progressHandler;
  private transient InMemoryFinalizer finalizationHandler;
  private transient BundleCheckpointHandler checkpointHandler;
  private transient boolean hasSdfProcessFn;
  private transient StageBundleFactory stageBundleFactory;
  private transient ExecutableStage executableStage;
  private transient SdkHarnessDoFnRunner<InputT, OutputT> sdkHarnessRunner;

  /** The minimum event time timer timestamp observed during the last bundle. */
  private transient long minEventTimeTimerTimestampInLastBundle;

  /** The minimum event time timer timestamp observed in the current bundle. */
  private transient long minEventTimeTimerTimestampInCurrentBundle;

  /** The input watermark before the current bundle started. */
  private long inputWatermarkBeforeBundleStart = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();

  /** Flag indicating whether the operator has been closed. */
  private transient boolean closed;

  /** Constructor. */
  public ExecutableStageDoFnOperator(
      String stepName,
      Coder<WindowedValue<InputT>> windowedInputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds,
      PipelineOptions options,
      RunnerApi.ExecutableStagePayload payload,
      JobInfo jobInfo,
      FlinkExecutableStageContextFactory contextFactory,
      Map<String, TupleTag<?>> outputMap,
      WindowingStrategy windowingStrategy,
      Coder keyCoder,
      KeySelector<WindowedValue<InputT>, ?> keySelector) {
    super(
        requiresStableInput(payload) ? new StableNoOpDoFn() : new NoOpDoFn(),
        stepName,
        windowedInputCoder,
        outputCoders,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder,
        keySelector,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());
    this.isStateful = payload.getUserStatesCount() > 0 || payload.getTimersCount() > 0;
    this.payload = payload;
    this.jobInfo = jobInfo;
    this.contextFactory = contextFactory;
    this.outputMap = outputMap;
    this.sideInputIds = sideInputIds;
    this.stateBackendLock = new ReentrantLock();
    this.windowCoder = (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder();
    this.inputCoder = windowedInputCoder;
    this.pipelineOptions = new SerializablePipelineOptions(options);

    Preconditions.checkArgument(
        !windowedInputCoder.getCoderArguments().isEmpty(),
        "Empty arguments for WindowedValue Coder %s",
        windowedInputCoder);
  }

  @Override
  <K> @Nullable KeyedStateBackend<K> getBufferingKeyedStateBackend() {
    // do not use keyed backend for buffering if we do not process stateful DoFn
    // ExecutableStage uses keyed backend by default
    return isStateful ? super.getKeyedStateBackend() : null;
  }

  @Override
  protected Lock getLockToAcquireForStateAccessDuringBundles() {
    return stateBackendLock;
  }

  @Override
  public void open() throws Exception {
    executableStage = ExecutableStage.fromPayload(payload);
    hasSdfProcessFn = hasSDF(executableStage);
    initializeUserState(executableStage, getKeyedStateBackend(), pipelineOptions);
    // TODO: Wire this into the distributed cache and make it pluggable.
    // TODO: Do we really want this layer of indirection when accessing the stage bundle factory?
    // It's a little strange because this operator is responsible for the lifetime of the stage
    // bundle "factory" (manager?) but not the job or Flink bundle factories. How do we make
    // ownership of the higher level "factories" explicit? Do we care?
    stageContext = contextFactory.get(jobInfo);

    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
    stateRequestHandler = getStateRequestHandler(executableStage);
    progressHandler =
        new BundleProgressHandler() {
          @Override
          public void onProgress(ProcessBundleProgressResponse progress) {
            if (flinkMetricContainer != null) {
              flinkMetricContainer.updateMetrics(stepName, progress.getMonitoringInfosList());
            }
          }

          @Override
          public void onCompleted(ProcessBundleResponse response) {
            if (flinkMetricContainer != null) {
              flinkMetricContainer.updateMetrics(stepName, response.getMonitoringInfosList());
            }
          }
        };
    finalizationHandler =
        BundleFinalizationHandlers.inMemoryFinalizer(
            stageBundleFactory.getInstructionRequestHandler());

    checkpointHandler = getBundleCheckpointHandler(hasSdfProcessFn);

    minEventTimeTimerTimestampInCurrentBundle = Long.MAX_VALUE;
    minEventTimeTimerTimestampInLastBundle = Long.MAX_VALUE;
    super.setPreBundleCallback(this::preBundleStartCallback);
    super.setBundleFinishedCallback(this::finishBundleCallback);

    // This will call {@code createWrappingDoFnRunner} which needs the above dependencies.
    super.open();
  }

  @Override
  public final void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    finalizationHandler.finalizeAllOutstandingBundles();
  }

  private BundleCheckpointHandler getBundleCheckpointHandler(boolean hasSDF) {
    if (!hasSDF) {
      return response -> {
        throw new UnsupportedOperationException(
            "Self-checkpoint is only supported on splittable DoFn.");
      };
    }

    return new BundleCheckpointHandlers.StateAndTimerBundleCheckpointHandler(
        new SdfFlinkTimerInternalsFactory(),
        new SdfFlinkStateInternalsFactory(),
        inputCoder,
        windowCoder);
  }

  private boolean hasSDF(ExecutableStage executableStage) {
    return executableStage.getTransforms().stream()
        .anyMatch(
            pTransformNode ->
                pTransformNode
                    .getTransform()
                    .getSpec()
                    .getUrn()
                    .equals(
                        PTransformTranslation
                            .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN));
  }

  private StateRequestHandler getStateRequestHandler(ExecutableStage executableStage) {

    final StateRequestHandler sideInputStateHandler;
    if (executableStage.getSideInputs().size() > 0) {
      checkNotNull(super.sideInputHandler);
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          Preconditions.checkNotNull(
              StreamingSideInputHandlerFactory.forStage(
                  executableStage, sideInputIds, super.sideInputHandler));
      try {
        sideInputStateHandler =
            StateRequestHandlers.forSideInputHandlerFactory(
                ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize SideInputHandler", e);
      }
    } else {
      sideInputStateHandler = StateRequestHandler.unsupported();
    }

    final StateRequestHandler userStateRequestHandler;
    if (!executableStage.getUserStates().isEmpty()) {
      if (keyedStateInternals == null) {
        throw new IllegalStateException("Input must be keyed when user state is used");
      }
      userStateRequestHandler =
          StateRequestHandlers.forBagUserStateHandlerFactory(
              stageBundleFactory.getProcessBundleDescriptor(),
              new BagUserStateFactory(
                  keyedStateInternals, getKeyedStateBackend(), stateBackendLock, keyCoder));
    } else {
      userStateRequestHandler = StateRequestHandler.unsupported();
    }

    EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(TypeCase.class);
    handlerMap.put(TypeCase.ITERABLE_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(TypeCase.MULTIMAP_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(TypeCase.MULTIMAP_KEYS_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(TypeCase.BAG_USER_STATE, userStateRequestHandler);

    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  static class BagUserStateFactory<V, W extends BoundedWindow>
      implements StateRequestHandlers.BagUserStateHandlerFactory<ByteString, V, W> {

    private final StateInternals stateInternals;
    private final KeyedStateBackend<ByteBuffer> keyedStateBackend;
    /** Lock to hold whenever accessing the state backend. */
    private final Lock stateBackendLock;
    /** For debugging: The key coder used by the Runner. */
    private final @Nullable Coder runnerKeyCoder;
    /** For debugging: Same as keyedStateBackend but upcasted, to access key group meta info. */
    private final @Nullable AbstractKeyedStateBackend<ByteBuffer> keyStateBackendWithKeyGroupInfo;

    BagUserStateFactory(
        StateInternals stateInternals,
        KeyedStateBackend<ByteBuffer> keyedStateBackend,
        Lock stateBackendLock,
        @Nullable Coder runnerKeyCoder) {
      this.stateInternals = stateInternals;
      this.keyedStateBackend = keyedStateBackend;
      this.stateBackendLock = stateBackendLock;
      if (keyedStateBackend instanceof AbstractKeyedStateBackend) {
        // This will always succeed, unless a custom state backend is used which does not extend
        // AbstractKeyedStateBackend. This is unlikely but we should still consider this case.
        this.keyStateBackendWithKeyGroupInfo =
            (AbstractKeyedStateBackend<ByteBuffer>) keyedStateBackend;
      } else {
        this.keyStateBackendWithKeyGroupInfo = null;
      }
      this.runnerKeyCoder = runnerKeyCoder;
    }

    @Override
    public StateRequestHandlers.BagUserStateHandler<ByteString, V, W> forUserState(
        // Transform id not used because multiple operators with state will not
        // be fused together. See GreedyPCollectionFusers
        String pTransformId,
        String userStateId,
        Coder<ByteString> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      return new StateRequestHandlers.BagUserStateHandler<ByteString, V, W>() {

        @Override
        public Iterable<V> get(ByteString key, W window) {
          try (Locker locker = Locker.locked(stateBackendLock)) {
            prepareStateBackend(key);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "State get for {} {} {} {}",
                  pTransformId,
                  userStateId,
                  Arrays.toString(keyedStateBackend.getCurrentKey().array()),
                  window);
            }
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));

            return bagState.read();
          }
        }

        @Override
        public void append(ByteString key, W window, Iterator<V> values) {
          try (Locker locker = Locker.locked(stateBackendLock)) {
            prepareStateBackend(key);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "State append for {} {} {} {}",
                  pTransformId,
                  userStateId,
                  Arrays.toString(keyedStateBackend.getCurrentKey().array()),
                  window);
            }
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            while (values.hasNext()) {
              bagState.add(values.next());
            }
          }
        }

        @Override
        public void clear(ByteString key, W window) {
          try (Locker locker = Locker.locked(stateBackendLock)) {
            prepareStateBackend(key);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "State clear for {} {} {} {}",
                  pTransformId,
                  userStateId,
                  Arrays.toString(keyedStateBackend.getCurrentKey().array()),
                  window);
            }
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            bagState.clear();
          }
        }

        private void prepareStateBackend(ByteString key) {
          // Key for state request is shipped encoded with NESTED context.
          ByteBuffer encodedKey = FlinkKeyUtils.fromEncodedKey(key);
          keyedStateBackend.setCurrentKey(encodedKey);
          if (keyStateBackendWithKeyGroupInfo != null) {
            int currentKeyGroupIndex = keyStateBackendWithKeyGroupInfo.getCurrentKeyGroupIndex();
            KeyGroupRange keyGroupRange = keyStateBackendWithKeyGroupInfo.getKeyGroupRange();
            Preconditions.checkState(
                keyGroupRange.contains(currentKeyGroupIndex),
                "The current key '%s' with key group index '%s' does not belong to the key group range '%s'. Runner keyCoder: %s. Ptransformid: %s Userstateid: %s",
                Arrays.toString(key.toByteArray()),
                currentKeyGroupIndex,
                keyGroupRange,
                runnerKeyCoder,
                pTransformId,
                userStateId);
          }
        }
      };
    }
  }

  /**
   * Note: This is only relevant when we have a stateful DoFn. We want to control the key of the
   * state backend ourselves and we must avoid any concurrent setting of the current active key. By
   * overwriting this, we also prevent unnecessary serialization as the key has to be encoded as a
   * byte array.
   */
  @Override
  public void setKeyContextElement1(StreamRecord record) {}

  /**
   * We don't want to set anything here. This is due to asynchronous nature of processing elements
   * from the SDK Harness. The Flink runtime sets the current key before calling {@code
   * processElement}, but this does not work when sending elements to the SDK harness which may be
   * processed at an arbitrary later point in time. State for keys is also accessed asynchronously
   * via state requests.
   *
   * <p>We set the key only as it is required for 1) State requests 2) Timers (setting/firing).
   */
  @Override
  public void setCurrentKey(Object key) {}

  @Override
  public ByteBuffer getCurrentKey() {
    // This is the key retrieved by HeapInternalTimerService when setting a Flink timer.
    // Note: Only called by the TimerService. Must be guarded by a lock.
    Preconditions.checkState(
        stateBackendLock.isLocked(),
        "State backend must be locked when retrieving the current key.");
    return this.<ByteBuffer>getKeyedStateBackend().getCurrentKey();
  }

  void setTimer(Timer<?> timerElement, TimerInternals.TimerData timerData) {
    try {
      Preconditions.checkState(
          sdkHarnessRunner.isBundleInProgress(), "Bundle was expected to be in progress!!");
      LOG.debug("Setting timer: {} {}", timerElement, timerData);
      // KvToByteBufferKeySelector returns the key encoded, it doesn't care about the
      // window, timestamp or pane information.
      ByteBuffer encodedKey =
          (ByteBuffer)
              keySelector.getKey(
                  WindowedValue.valueInGlobalWindow(
                      (InputT) KV.of(timerElement.getUserKey(), null)));
      // We have to synchronize to ensure the state backend is not concurrently accessed by the
      // state requests
      try (Locker locker = Locker.locked(stateBackendLock)) {
        getKeyedStateBackend().setCurrentKey(encodedKey);
        if (timerElement.getClearBit()) {
          timerInternals.deleteTimer(timerData);
        } else {
          timerInternals.setTimer(timerData);
          if (!timerData.getTimerId().equals(GC_TIMER_ID)) {
            minEventTimeTimerTimestampInCurrentBundle =
                Math.min(
                    minEventTimeTimerTimestampInCurrentBundle,
                    adjustTimestampForFlink(timerData.getTimestamp().getMillis()));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Couldn't set timer", e);
    }
  }

  /**
   * A {@link TimerInternalsFactory} for Flink operator to create a {@link
   * StateAndTimerBundleCheckpointHandler} to handle {@link
   * org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication}.
   */
  class SdfFlinkTimerInternalsFactory implements TimerInternalsFactory<InputT> {
    @Override
    public TimerInternals timerInternalsForKey(InputT key) {
      try {
        ByteBuffer encodedKey =
            (ByteBuffer) keySelector.getKey(WindowedValue.valueInGlobalWindow(key));
        return new SdfFlinkTimerInternals(encodedKey);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't get a timer internals", e);
      }
    }
  }

  /**
   * A {@link TimerInternals} for rescheduling {@link
   * org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication}.
   */
  class SdfFlinkTimerInternals implements TimerInternals {
    private final ByteBuffer key;

    SdfFlinkTimerInternals(ByteBuffer key) {
      this.key = key;
    }

    @Override
    public void setTimer(
        StateNamespace namespace,
        String timerId,
        String timerFamilyId,
        Instant target,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      setTimer(
          TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
    }

    @Override
    public void setTimer(TimerData timerData) {
      try {
        try (Locker locker = Locker.locked(stateBackendLock)) {
          getKeyedStateBackend().setCurrentKey(key);
          timerInternals.setTimer(timerData);
          minEventTimeTimerTimestampInCurrentBundle =
              Math.min(
                  minEventTimeTimerTimestampInCurrentBundle,
                  adjustTimestampForFlink(timerData.getOutputTimestamp().getMillis()));
        }
      } catch (Exception e) {
        throw new RuntimeException("Couldn't set timer", e);
      }
    }

    @Override
    public void deleteTimer(
        StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
      throw new UnsupportedOperationException(
          "It is not expected to use SdfFlinkTimerInternals to delete a timer");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
      throw new UnsupportedOperationException(
          "It is not expected to use SdfFlinkTimerInternals to delete a timer");
    }

    @Override
    public void deleteTimer(TimerData timerKey) {
      throw new UnsupportedOperationException(
          "It is not expected to use SdfFlinkTimerInternals to delete a timer");
    }

    @Override
    public Instant currentProcessingTime() {
      return timerInternals.currentProcessingTime();
    }

    @Override
    public @Nullable Instant currentSynchronizedProcessingTime() {
      return timerInternals.currentSynchronizedProcessingTime();
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return timerInternals.currentInputWatermarkTime();
    }

    @Override
    public @Nullable Instant currentOutputWatermarkTime() {
      return timerInternals.currentOutputWatermarkTime();
    }
  }

  /**
   * A {@link StateInternalsFactory} for Flink operator to create a {@link
   * StateAndTimerBundleCheckpointHandler} to handle {@link
   * org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication}.
   */
  class SdfFlinkStateInternalsFactory implements StateInternalsFactory<InputT> {
    @Override
    public StateInternals stateInternalsForKey(InputT key) {
      try {
        ByteBuffer encodedKey =
            (ByteBuffer) keySelector.getKey(WindowedValue.valueInGlobalWindow(key));
        return new SdfFlinkStateInternals(encodedKey);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't get a state internals", e);
      }
    }
  }

  /** A {@link StateInternals} for keeping {@link DelayedBundleApplication}s as states. */
  class SdfFlinkStateInternals implements StateInternals {

    private final ByteBuffer key;

    SdfFlinkStateInternals(ByteBuffer key) {
      this.key = key;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public <T extends State> T state(
        StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
      try {
        try (Locker locker = Locker.locked(stateBackendLock)) {
          getKeyedStateBackend().setCurrentKey(key);
          return keyedStateInternals.state(namespace, address);
        }
      } catch (Exception e) {
        throw new RuntimeException("Couldn't set state", e);
      }
    }
  }

  @Override
  protected void fireTimerInternal(ByteBuffer key, TimerInternals.TimerData timer) {
    // We have to synchronize to ensure the state backend is not concurrently accessed by the state
    // requests
    try (Locker locker = Locker.locked(stateBackendLock)) {
      getKeyedStateBackend().setCurrentKey(key);
      fireTimer(timer);
    }
  }

  @Override
  public void flushData() throws Exception {
    closed = true;
    // We might still hold back the watermark and Flink does not trigger the timer
    // callback for watermark advancement anymore.
    processWatermark1(Watermark.MAX_WATERMARK);
    while (getCurrentOutputWatermark() < Watermark.MAX_WATERMARK.getTimestamp()) {
      invokeFinishBundle();
      if (hasSdfProcessFn) {
        // Manually drain processing time timers since Flink will ignore pending
        // processing-time timers when upstream operators have shut down and will also
        // shut down this operator with pending processing-time timers.
        // TODO(https://github.com/apache/beam/issues/20600, FLINK-18647): It doesn't work
        // efficiently when the watermark of upstream advances to MAX_TIMESTAMP
        // immediately.
        if (numProcessingTimeTimers() > 0) {
          timerInternals.processPendingProcessingTimeTimers();
        }
      }
    }
    super.flushData();
  }

  @Override
  public void cleanUp() throws Exception {
    // may be called multiple times when an exception is thrown
    if (stageContext != null) {
      // Remove the reference to stageContext and make stageContext available for garbage
      // collection.
      try (AutoCloseable bundleFactoryCloser = stageBundleFactory;
          AutoCloseable closable = stageContext) {
        // DoFnOperator generates another "bundle" for the final watermark
        // https://issues.apache.org/jira/browse/BEAM-5816
        super.cleanUp();
      } finally {
        stageContext = null;
      }
    }
  }

  @Override
  protected void addSideInputValue(StreamRecord<RawUnionValue> streamRecord) {
    @SuppressWarnings("unchecked")
    WindowedValue<KV<Void, Iterable<?>>> value =
        (WindowedValue<KV<Void, Iterable<?>>>) streamRecord.getValue().getValue();
    PCollectionView<?> sideInput = sideInputTagMapping.get(streamRecord.getValue().getUnionTag());
    sideInputHandler.addSideInputValue(sideInput, value.withValue(value.getValue().getValue()));
  }

  @Override
  DoFnRunner<InputT, OutputT> createBufferingDoFnRunnerIfNeeded(
      DoFnRunner<InputT, OutputT> wrappedRunner) throws Exception {

    if (requiresStableInput) {
      // put this in front of the root FnRunner before any additional wrappers
      KeyedStateBackend<Object> keyedBufferingBackend = getBufferingKeyedStateBackend();
      return this.bufferingDoFnRunner =
          BufferingDoFnRunner.create(
              wrappedRunner,
              "stable-input-buffer",
              windowedInputCoder,
              windowingStrategy.getWindowFn().windowCoder(),
              getOperatorStateBackend(),
              keyedBufferingBackend,
              numConcurrentCheckpoints,
              serializedOptions,
              keyedBufferingBackend != null ? () -> Locker.locked(stateBackendLock) : null,
              keyedBufferingBackend != null
                  ? input -> FlinkKeyUtils.encodeKey(((KV) input).getKey(), (Coder) keyCoder)
                  : null,
              sdkHarnessRunner::emitResults);
    }
    return wrappedRunner;
  }

  @Override
  protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
      DoFnRunner<InputT, OutputT> wrappedRunner, StepContext stepContext) {
    sdkHarnessRunner =
        new SdkHarnessDoFnRunner<>(
            wrappedRunner.getFn(),
            stageBundleFactory,
            stateRequestHandler,
            progressHandler,
            finalizationHandler,
            checkpointHandler,
            outputManager,
            outputMap,
            windowCoder,
            inputCoder,
            this::setTimer,
            () -> FlinkKeyUtils.decodeKey(getCurrentKey(), keyCoder),
            keyedStateInternals);

    return ensureStateDoFnRunner(sdkHarnessRunner, payload, stepContext);
  }

  @Override
  public long applyInputWatermarkHold(long inputWatermark) {
    // We must wait until all elements/timers have been processed (happens async!) before the
    // watermark can be progressed. We can't just advance the input watermark until at least one
    // bundle has been completed since the watermark has been received. Otherwise we potentially
    // violate the Beam timer contract which allows for already set timer to be modified by
    // successive elements.
    //
    // For example, we set a timer at t1, then finish the bundle (e.g. due to the bundle timeout),
    // then receive an element which updates the timer to fire at t2, and then receive a watermark
    // w1, where w1 > t2 > t1. If we do not hold back the input watermark here, w1 would fire the
    // initial timer at t1, but we want to make sure to fire the updated version of the timer at
    // t2.
    if (sdkHarnessRunner.isBundleInProgress()) {
      return inputWatermarkBeforeBundleStart;
    } else {
      return inputWatermark;
    }
  }

  @Override
  public long applyOutputWatermarkHold(long currentOutputWatermark, long potentialOutputWatermark) {
    // Due to the asynchronous communication with the SDK harness,
    // a bundle might still be in progress and not all items have
    // yet been received from the SDK harness. If we just set this
    // watermark as the new output watermark, we could violate the
    // order of the records, i.e. pending items in the SDK harness
    // could become "late" although they were "on time".
    //
    // We can solve this problem using one of the following options:
    //
    // 1) Finish the current bundle and emit this watermark as the
    //    new output watermark. Finishing the bundle ensures that
    //    all the items have been processed by the SDK harness and
    //    received by the outputQueue (see below), where they will
    //    have been emitted to the output stream.
    //
    // 2) Put a hold on the output watermark for as long as the current
    //    bundle has not been finished. We have to remember to manually
    //    finish the bundle in case we receive the final watermark.
    //    To avoid latency, we should process this watermark again as
    //    soon as the current bundle is finished.
    //
    // Approach 1) is the easiest and gives better latency, yet 2)
    // gives better throughput due to the bundle not getting cut on
    // every watermark. So we have implemented 2) below.
    //
    potentialOutputWatermark =
        super.applyOutputWatermarkHold(currentOutputWatermark, potentialOutputWatermark);
    if (sdkHarnessRunner.isBundleInProgress()) {
      if (minEventTimeTimerTimestampInLastBundle < Long.MAX_VALUE) {
        // We can safely advance the watermark to before the last bundle's minimum event timer
        // but not past the potential output watermark which includes holds to the input watermark.
        return Math.min(minEventTimeTimerTimestampInLastBundle - 1, potentialOutputWatermark);
      } else {
        // We don't have any information yet, use the current output watermark for now.
        return currentOutputWatermark;
      }
    } else {
      // No bundle was started when we advanced the input watermark.
      // Thus, we can safely set a new output watermark.
      return potentialOutputWatermark;
    }
  }

  private void preBundleStartCallback() {
    inputWatermarkBeforeBundleStart = getEffectiveInputWatermark();
  }

  private void finishBundleCallback() {
    minEventTimeTimerTimestampInLastBundle = minEventTimeTimerTimestampInCurrentBundle;
    minEventTimeTimerTimestampInCurrentBundle = Long.MAX_VALUE;
    try {
      if (!closed
          && minEventTimeTimerTimestampInLastBundle < Long.MAX_VALUE
          && minEventTimeTimerTimestampInLastBundle <= getEffectiveInputWatermark()) {

        scheduleForCurrentProcessingTime(
            ts -> processWatermark1(new Watermark(getEffectiveInputWatermark())));
      } else {
        processWatermark1(new Watermark(getEffectiveInputWatermark()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to progress watermark to " + getEffectiveInputWatermark(), e);
    }
  }

  private static class SdkHarnessDoFnRunner<InputT, OutputT>
      implements DoFnRunner<InputT, OutputT> {

    private final DoFn<InputT, OutputT> doFn;
    private final LinkedBlockingQueue<KV<String, OutputT>> outputQueue;
    private final StageBundleFactory stageBundleFactory;
    private final StateRequestHandler stateRequestHandler;
    private final BundleProgressHandler progressHandler;
    private final BundleFinalizationHandler finalizationHandler;
    private final BundleCheckpointHandler checkpointHandler;
    private final BufferedOutputManager<OutputT> outputManager;
    private final Map<String, TupleTag<?>> outputMap;
    private final FlinkStateInternals<?> keyedStateInternals;
    private final Coder<BoundedWindow> windowCoder;
    private final Coder<WindowedValue<InputT>> residualCoder;
    private final BiConsumer<Timer<?>, TimerInternals.TimerData> timerRegistration;
    private final Supplier<Object> keyForTimer;

    /**
     * Current active bundle. Volatile to ensure mutually exclusive bundle processing threads see
     * this consistent. Please see the description in DoFnOperator.
     */
    private volatile RemoteBundle remoteBundle;
    /**
     * Current main input receiver. Volatile to ensure mutually exclusive bundle processing threads
     * see this consistent. Please see the description in DoFnOperator.
     */
    private volatile FnDataReceiver<WindowedValue<?>> mainInputReceiver;

    public SdkHarnessDoFnRunner(
        DoFn<InputT, OutputT> doFn,
        StageBundleFactory stageBundleFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler,
        BundleFinalizationHandler finalizationHandler,
        BundleCheckpointHandler checkpointHandler,
        BufferedOutputManager<OutputT> outputManager,
        Map<String, TupleTag<?>> outputMap,
        Coder<BoundedWindow> windowCoder,
        Coder<WindowedValue<InputT>> residualCoder,
        BiConsumer<Timer<?>, TimerInternals.TimerData> timerRegistration,
        Supplier<Object> keyForTimer,
        FlinkStateInternals<?> keyedStateInternals) {

      this.doFn = doFn;
      this.stageBundleFactory = stageBundleFactory;
      this.stateRequestHandler = stateRequestHandler;
      this.progressHandler = progressHandler;
      this.finalizationHandler = finalizationHandler;
      this.checkpointHandler = checkpointHandler;
      this.outputManager = outputManager;
      this.outputMap = outputMap;
      this.timerRegistration = timerRegistration;
      this.keyForTimer = keyForTimer;
      this.windowCoder = windowCoder;
      this.residualCoder = residualCoder;
      this.outputQueue = new LinkedBlockingQueue<>();
      this.keyedStateInternals = keyedStateInternals;
    }

    @Override
    public void startBundle() {
      OutputReceiverFactory receiverFactory =
          new OutputReceiverFactory() {
            @Override
            public FnDataReceiver<OutputT> create(String pCollectionId) {
              return receivedElement -> {
                // handover to queue, do not block the grpc thread
                outputQueue.put(KV.of(pCollectionId, receivedElement));
              };
            }
          };
      TimerReceiverFactory timerReceiverFactory =
          new TimerReceiverFactory(stageBundleFactory, timerRegistration, windowCoder);

      try {
        remoteBundle =
            stageBundleFactory.getBundle(
                receiverFactory,
                timerReceiverFactory,
                stateRequestHandler,
                progressHandler,
                finalizationHandler,
                checkpointHandler);
        mainInputReceiver = Iterables.getOnlyElement(remoteBundle.getInputReceivers().values());
      } catch (Exception e) {
        throw new RuntimeException("Failed to start remote bundle", e);
      }
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      try {
        LOG.debug("Processing value: {}", element);
        mainInputReceiver.accept(element);
      } catch (Exception e) {
        throw new RuntimeException("Failed to process element with SDK harness.", e);
      }
      emitResults();
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      Object timerKey = keyForTimer.get();
      Preconditions.checkNotNull(timerKey, "Key for timer needs to be set before calling onTimer");
      Preconditions.checkNotNull(remoteBundle, "Call to onTimer outside of a bundle");
      if (StateAndTimerBundleCheckpointHandler.isSdfTimer(timerId)) {
        StateNamespace namespace = StateNamespaces.window(windowCoder, window);
        WindowedValue stateValue =
            keyedStateInternals.state(namespace, StateTags.value(timerId, residualCoder)).read();
        processElement(stateValue);
      } else {
        KV<String, String> transformAndTimerFamilyId =
            TimerReceiverFactory.decodeTimerDataTimerId(timerFamilyId);
        LOG.debug(
            "timer callback: {} {} {} {} {}",
            transformAndTimerFamilyId.getKey(),
            transformAndTimerFamilyId.getValue(),
            window,
            timestamp,
            timeDomain);
        FnDataReceiver<Timer> timerReceiver =
            Preconditions.checkNotNull(
                remoteBundle.getTimerReceivers().get(transformAndTimerFamilyId),
                "No receiver found for timer %s %s",
                transformAndTimerFamilyId.getKey(),
                transformAndTimerFamilyId.getValue());
        Timer<?> timerValue =
            Timer.of(
                timerKey,
                timerId,
                Collections.singletonList(window),
                timestamp,
                outputTimestamp,
                // TODO: Support propagating the PaneInfo through.
                PaneInfo.NO_FIRING);
        try {
          timerReceiver.accept(timerValue);
        } catch (Exception e) {
          throw new RuntimeException(
              String.format(Locale.ENGLISH, "Failed to process timer %s", timerReceiver), e);
        }
      }
    }

    @Override
    public void finishBundle() {
      try {
        // TODO: it would be nice to emit results as they arrive, can thread wait non-blocking?
        // close blocks until all results are received
        remoteBundle.close();
        emitResults();
      } catch (Exception e) {
        if (e.getCause() instanceof StatusRuntimeException) {
          throw new RuntimeException("SDK Harness connection lost.", e);
        }
        throw new RuntimeException("Failed to finish remote bundle", e);
      } finally {
        remoteBundle = null;
      }
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

    boolean isBundleInProgress() {
      return remoteBundle != null;
    }

    private void emitResults() {
      KV<String, OutputT> result;
      while ((result = outputQueue.poll()) != null) {
        final String outputPCollectionId = Preconditions.checkNotNull(result.getKey());
        TupleTag<?> tag = outputMap.get(outputPCollectionId);
        WindowedValue windowedValue =
            Preconditions.checkNotNull(
                (WindowedValue) result.getValue(),
                "Received a null value from the SDK harness for %s",
                outputPCollectionId);
        if (tag == null) {
          throw new IllegalStateException(
              String.format("Received output for unknown PCollection %s", outputPCollectionId));
        }
        // process regular elements
        outputManager.output(tag, windowedValue);
      }
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return doFn;
    }
  }

  private DoFnRunner<InputT, OutputT> ensureStateDoFnRunner(
      DoFnRunner<InputT, OutputT> sdkHarnessRunner,
      RunnerApi.ExecutableStagePayload payload,
      StepContext stepContext) {

    if (!isStateful) {
      return sdkHarnessRunner;
    }
    // Takes care of state cleanup via StatefulDoFnRunner
    Coder windowCoder = windowingStrategy.getWindowFn().windowCoder();
    CleanupTimer<InputT> cleanupTimer =
        new CleanupTimer<>(
            timerInternals,
            stateBackendLock,
            windowingStrategy,
            keyCoder,
            windowCoder,
            getKeyedStateBackend());

    List<String> userStates =
        executableStage.getUserStates().stream()
            .map(UserStateReference::localName)
            .collect(Collectors.toList());

    KeyedStateBackend<ByteBuffer> stateBackend = getKeyedStateBackend();

    StateCleaner stateCleaner =
        new StateCleaner(
            userStates,
            windowCoder,
            stateBackend::getCurrentKey,
            timerInternals::hasPendingEventTimeTimers,
            cleanupTimer);

    return new StatefulDoFnRunner<InputT, OutputT, BoundedWindow>(
        sdkHarnessRunner,
        getInputCoder(),
        stepContext,
        windowingStrategy,
        cleanupTimer,
        stateCleaner,
        requiresTimeSortedInput(payload, true)) {

      @Override
      public void finishBundle() {
        // Before cleaning up state, first finish bundle for all underlying DoFnRunners
        super.finishBundle();
        // execute cleanup after the bundle is complete
        if (!stateCleaner.cleanupQueue.isEmpty()) {
          try (Locker locker = Locker.locked(stateBackendLock)) {
            stateCleaner.cleanupState(keyedStateInternals, stateBackend::setCurrentKey);
          } catch (Exception e) {
            throw new RuntimeException("Failed to cleanup state.", e);
          }
        }
      }
    };
  }

  static class CleanupTimer<InputT> implements StatefulDoFnRunner.CleanupTimer<InputT> {
    private static final String GC_TIMER_ID = "__user-state-cleanup__";

    private final TimerInternals timerInternals;
    private final Lock stateBackendLock;
    private final WindowingStrategy windowingStrategy;
    private final Coder keyCoder;
    private final Coder windowCoder;
    private final KeyedStateBackend<ByteBuffer> keyedStateBackend;

    CleanupTimer(
        TimerInternals timerInternals,
        Lock stateBackendLock,
        WindowingStrategy windowingStrategy,
        Coder keyCoder,
        Coder windowCoder,
        KeyedStateBackend<ByteBuffer> keyedStateBackend) {
      this.timerInternals = timerInternals;
      this.stateBackendLock = stateBackendLock;
      this.windowingStrategy = windowingStrategy;
      this.keyCoder = keyCoder;
      this.windowCoder = windowCoder;
      this.keyedStateBackend = keyedStateBackend;
    }

    @Override
    public void setForWindow(InputT input, BoundedWindow window) {
      Preconditions.checkNotNull(input, "Null input passed to CleanupTimer");
      if (window.equals(GlobalWindow.INSTANCE)) {
        // Skip setting a cleanup timer for the global window as these timers
        // lead to potentially unbounded state growth in the runner, depending on key cardinality.
        // Cleanup for global window will be performed upon arrival of the final watermark.
        return;
      }
      // needs to match the encoding in prepareStateBackend for state request handler
      final ByteBuffer key = FlinkKeyUtils.encodeKey(((KV) input).getKey(), keyCoder);
      // Ensure the state backend is not concurrently accessed by the state requests
      try (Locker locker = Locker.locked(stateBackendLock)) {
        keyedStateBackend.setCurrentKey(key);
        setCleanupTimer(window);
      }
    }

    void setCleanupTimer(BoundedWindow window) {
      // make sure this fires after any window.maxTimestamp() timers
      Instant gcTime =
          LateDataUtils.garbageCollectionTime(window, windowingStrategy).plus(Duration.millis(1));
      timerInternals.setTimer(
          StateNamespaces.window(windowCoder, window),
          GC_TIMER_ID,
          "",
          gcTime,
          window.maxTimestamp(),
          TimeDomain.EVENT_TIME);
    }

    @Override
    public boolean isForWindow(
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
      boolean isEventTimer = timeDomain.equals(TimeDomain.EVENT_TIME);
      Instant gcTime =
          LateDataUtils.garbageCollectionTime(window, windowingStrategy).plus(Duration.millis(1));
      return isEventTimer && GC_TIMER_ID.equals(timerId) && gcTime.equals(timestamp);
    }
  }

  static class StateCleaner implements StatefulDoFnRunner.StateCleaner<BoundedWindow> {

    private final List<String> userStateNames;
    private final Coder windowCoder;
    private final ArrayDeque<KV<ByteBuffer, BoundedWindow>> cleanupQueue;
    private final Supplier<ByteBuffer> currentKeySupplier;
    private final ThrowingFunction<Long, Boolean> hasPendingEventTimeTimers;
    private final CleanupTimer cleanupTimer;

    StateCleaner(
        List<String> userStateNames,
        Coder windowCoder,
        Supplier<ByteBuffer> currentKeySupplier,
        ThrowingFunction<Long, Boolean> hasPendingEventTimeTimers,
        CleanupTimer cleanupTimer) {
      this.userStateNames = userStateNames;
      this.windowCoder = windowCoder;
      this.currentKeySupplier = currentKeySupplier;
      this.hasPendingEventTimeTimers = hasPendingEventTimeTimers;
      this.cleanupTimer = cleanupTimer;
      this.cleanupQueue = new ArrayDeque<>();
    }

    @Override
    public void clearForWindow(BoundedWindow window) {
      // Delay cleanup until the end of the bundle to allow stateful processing and new timers.
      // Executed in the context of onTimer(..) where the correct key will be set
      cleanupQueue.add(KV.of(currentKeySupplier.get(), window));
    }

    @SuppressWarnings("ByteBufferBackingArray")
    void cleanupState(StateInternals stateInternals, Consumer<ByteBuffer> keyContextConsumer)
        throws Exception {
      while (!cleanupQueue.isEmpty()) {
        KV<ByteBuffer, BoundedWindow> kv = Preconditions.checkNotNull(cleanupQueue.remove());
        BoundedWindow window = Preconditions.checkNotNull(kv.getValue());
        keyContextConsumer.accept(kv.getKey());
        // Check whether we have pending timers which were set during the bundle.
        if (hasPendingEventTimeTimers.apply(window.maxTimestamp().getMillis())) {
          // Re-add GC timer and let remaining timers fire. Don't cleanup state yet.
          cleanupTimer.setCleanupTimer(window);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("State cleanup for {} {}", Arrays.toString(kv.getKey().array()), window);
          }
          // No more timers (finally!). Time to clean up.
          for (String userState : userStateNames) {
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            StateTag<BagState<Void>> bagStateStateTag = StateTags.bag(userState, VoidCoder.of());
            BagState<?> state = stateInternals.state(namespace, bagStateStateTag);
            state.clear();
          }
        }
      }
    }
  }

  /**
   * Eagerly create the user state to work around https://jira.apache.org/jira/browse/FLINK-12653.
   */
  private static void initializeUserState(
      ExecutableStage executableStage,
      @Nullable KeyedStateBackend keyedStateBackend,
      SerializablePipelineOptions pipelineOptions) {
    executableStage
        .getUserStates()
        .forEach(
            ref -> {
              try {
                keyedStateBackend.getOrCreateKeyedState(
                    StringSerializer.INSTANCE,
                    new ListStateDescriptor<>(
                        ref.localName(),
                        new CoderTypeSerializer<>(ByteStringCoder.of(), pipelineOptions)));
              } catch (Exception e) {
                throw new RuntimeException("Couldn't initialize user states.", e);
              }
            });
  }

  private static class NoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }

  private static class StableNoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @RequiresStableInput
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }
}
