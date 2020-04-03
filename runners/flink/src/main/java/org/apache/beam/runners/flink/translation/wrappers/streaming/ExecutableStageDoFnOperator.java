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

import static org.apache.beam.runners.flink.translation.utils.FlinkPortableRunnerUtils.requiresTimeSortedInput;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContextFactory;
import org.apache.beam.runners.flink.translation.functions.FlinkStreamingSideInputHandlerFactory;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.sdk.v2.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
public class ExecutableStageDoFnOperator<InputT, OutputT> extends DoFnOperator<InputT, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutableStageDoFnOperator.class);

  private final RunnerApi.ExecutableStagePayload payload;
  private final JobInfo jobInfo;
  private final FlinkExecutableStageContextFactory contextFactory;
  private final Map<String, TupleTag<?>> outputMap;
  private final Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds;
  /** A lock which has to be acquired when concurrently accessing state and timers. */
  private final ReentrantLock stateBackendLock;

  private final boolean isStateful;

  private transient ExecutableStageContext stageContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient BundleProgressHandler progressHandler;
  private transient StageBundleFactory stageBundleFactory;
  private transient ExecutableStage executableStage;
  private transient SdkHarnessDoFnRunner<InputT, OutputT> sdkHarnessRunner;

  /**
   * Watermark held back due to async processing. Volatile due to multiple mutually exclusive
   * threads. Please see the description in DoFnOperator.
   */
  private transient volatile long backupWatermarkHold = Long.MIN_VALUE;

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
        new NoOpDoFn(),
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
  }

  @Override
  protected Lock getLockToAcquireForStateAccessDuringBundles() {
    return stateBackendLock;
  }

  @Override
  public void open() throws Exception {
    executableStage = ExecutableStage.fromPayload(payload);
    initializeUserState(executableStage, getKeyedStateBackend());
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

    // This will call {@code createWrappingDoFnRunner} which needs the above dependencies.
    super.open();
  }

  private StateRequestHandler getStateRequestHandler(ExecutableStage executableStage) {

    final StateRequestHandler sideInputStateHandler;
    if (executableStage.getSideInputs().size() > 0) {
      checkNotNull(super.sideInputHandler);
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          Preconditions.checkNotNull(
              FlinkStreamingSideInputHandlerFactory.forStage(
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
                  () -> UUID.randomUUID().toString(),
                  keyedStateInternals,
                  getKeyedStateBackend(),
                  stateBackendLock,
                  keyCoder));
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
    @Nullable private final Coder runnerKeyCoder;
    /** For debugging: Same as keyedStateBackend but upcasted, to access key group meta info. */
    @Nullable private final AbstractKeyedStateBackend<ByteBuffer> keyStateBackendWithKeyGroupInfo;
    /** Holds the valid cache token for user state for this operator. */
    private final ByteString cacheToken;

    BagUserStateFactory(
        IdGenerator cacheTokenGenerator,
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
      this.cacheToken = ByteString.copyFrom(cacheTokenGenerator.getId().getBytes(Charsets.UTF_8));
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

        @Override
        public Optional<ByteString> getCacheToken() {
          // Cache tokens remains valid for the life time of the operator
          return Optional.of(cacheToken);
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

  private void setTimer(WindowedValue<InputT> timerElement, TimerInternals.TimerData timerData) {
    try {
      LOG.debug("Setting timer: {} {}", timerElement, timerData);
      // KvToByteBufferKeySelector returns the key encoded
      ByteBuffer encodedKey = (ByteBuffer) keySelector.getKey(timerElement);
      // We have to synchronize to ensure the state backend is not concurrently accessed by the
      // state requests
      try (Locker locker = Locker.locked(stateBackendLock)) {
        getKeyedStateBackend().setCurrentKey(encodedKey);
        if (timerData.getTimestamp().isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
          timerInternals.deleteTimer(
              timerData.getNamespace(), timerData.getTimerId(), timerData.getDomain());
        } else {
          timerInternals.setTimer(timerData);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Couldn't set timer", e);
    }
  }

  @Override
  protected void fireTimer(InternalTimer<ByteBuffer, TimerInternals.TimerData> timer) {
    final ByteBuffer encodedKey = timer.getKey();
    // We have to synchronize to ensure the state backend is not concurrently accessed by the state
    // requests
    try (Locker locker = Locker.locked(stateBackendLock)) {
      getKeyedStateBackend().setCurrentKey(encodedKey);
      super.fireTimer(timer);
    }
  }

  @Override
  public void dispose() throws Exception {
    // may be called multiple times when an exception is thrown
    if (stageContext != null) {
      // Remove the reference to stageContext and make stageContext available for garbage
      // collection.
      try (AutoCloseable bundleFactoryCloser = stageBundleFactory;
          AutoCloseable closable = stageContext) {
        // DoFnOperator generates another "bundle" for the final watermark
        // https://issues.apache.org/jira/browse/BEAM-5816
        super.dispose();
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
  protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
      DoFnRunner<InputT, OutputT> wrappedRunner, StepContext stepContext) {
    sdkHarnessRunner =
        new SdkHarnessDoFnRunner<>(
            wrappedRunner.getFn(),
            executableStage.getInputPCollection().getId(),
            stageBundleFactory,
            stateRequestHandler,
            progressHandler,
            outputManager,
            outputMap,
            (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder(),
            this::setTimer,
            () -> FlinkKeyUtils.decodeKey(getCurrentKey(), keyCoder));

    return ensureStateDoFnRunner(sdkHarnessRunner, payload, stepContext);
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
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
    if (sdkHarnessRunner.isBundleInProgress()) {
      if (mark.getTimestamp() >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
        invokeFinishBundle();
        setPushedBackWatermark(Long.MAX_VALUE);
      } else {
        // It is not safe to advance the output watermark yet, so add a hold on the current
        // output watermark.
        backupWatermarkHold = Math.max(backupWatermarkHold, getPushbackWatermarkHold());
        setPushedBackWatermark(Math.min(currentOutputWatermark, backupWatermarkHold));
        super.setBundleFinishedCallback(
            () -> {
              try {
                LOG.debug("processing pushed back watermark: {}", mark);
                // at this point the bundle is finished, allow the watermark to pass
                // we are restoring the previous hold in case it was already set for side inputs
                setPushedBackWatermark(backupWatermarkHold);
                super.processWatermark(mark);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to process pushed back watermark after finished bundle.", e);
              }
            });
      }
    }
    super.processWatermark(mark);
  }

  private static class SdkHarnessDoFnRunner<InputT, OutputT>
      implements DoFnRunner<InputT, OutputT> {

    private final DoFn<InputT, OutputT> doFn;
    private final String mainInput;
    private final LinkedBlockingQueue<KV<String, OutputT>> outputQueue;
    private final StageBundleFactory stageBundleFactory;
    private final StateRequestHandler stateRequestHandler;
    private final BundleProgressHandler progressHandler;
    private final BufferedOutputManager<OutputT> outputManager;
    private final Map<String, TupleTag<?>> outputMap;
    /** Timer Output Pcollection id => TimerSpec. */
    private final Map<String, TimerSpec> timerOutputIdToSpecMap;

    private final Coder<BoundedWindow> windowCoder;
    private final BiConsumer<WindowedValue<InputT>, TimerInternals.TimerData> timerRegistration;
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
        String mainInput,
        StageBundleFactory stageBundleFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler,
        BufferedOutputManager<OutputT> outputManager,
        Map<String, TupleTag<?>> outputMap,
        Coder<BoundedWindow> windowCoder,
        BiConsumer<WindowedValue<InputT>, TimerInternals.TimerData> timerRegistration,
        Supplier<Object> keyForTimer) {

      this.doFn = doFn;
      this.mainInput = mainInput;
      this.stageBundleFactory = stageBundleFactory;
      this.stateRequestHandler = stateRequestHandler;
      this.progressHandler = progressHandler;
      this.outputManager = outputManager;
      this.outputMap = outputMap;
      this.timerRegistration = timerRegistration;
      this.timerOutputIdToSpecMap = new HashMap<>();
      this.keyForTimer = keyForTimer;
      // Gather all timers from all transforms by their output pCollectionId which is unique
      for (Map<String, ProcessBundleDescriptors.TimerSpec> transformTimerMap :
          stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().values()) {
        for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
          timerOutputIdToSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
        }
      }
      this.windowCoder = windowCoder;
      this.outputQueue = new LinkedBlockingQueue<>();
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

      try {
        remoteBundle =
            stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
        mainInputReceiver =
            Preconditions.checkNotNull(
                remoteBundle.getInputReceivers().get(mainInput),
                "Failed to retrieve main input receiver.");
      } catch (Exception e) {
        throw new RuntimeException("Failed to start remote bundle", e);
      }
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      try {
        LOG.debug("Sending value: {}", element);
        mainInputReceiver.accept(element);
      } catch (Exception e) {
        throw new RuntimeException("Failed to process element with SDK harness.", e);
      }
      emitResults();
    }

    @Override
    public void onTimer(
        String timerId,
        String timerFamilyId,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      Object timerKey = keyForTimer.get();
      Preconditions.checkNotNull(timerKey, "Key for timer needs to be set before calling onTimer");
      Preconditions.checkNotNull(remoteBundle, "Call to onTimer outside of a bundle");
      LOG.debug("timer callback: {} {} {} {}", timerId, window, timestamp, timeDomain);
      FnDataReceiver<WindowedValue<?>> timerReceiver =
          Preconditions.checkNotNull(
              remoteBundle.getInputReceivers().get(timerId),
              "No receiver found for timer %s",
              timerId);
      WindowedValue<KV<Object, Timer>> timerValue =
          WindowedValue.of(
              KV.of(timerKey, Timer.of(timestamp, new byte[0])),
              outputTimestamp,
              Collections.singleton(window),
              PaneInfo.NO_FIRING);
      try {
        timerReceiver.accept(timerValue);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Failed to process timer %s", timerReceiver), e);
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
        if (tag != null) {
          // process regular elements
          outputManager.output(tag, windowedValue);
        } else {
          TimerSpec timerSpec =
              Preconditions.checkNotNull(
                  timerOutputIdToSpecMap.get(outputPCollectionId),
                  "Unknown Pcollectionid %s",
                  outputPCollectionId);
          Timer timer =
              Preconditions.checkNotNull(
                  (Timer) ((KV) windowedValue.getValue()).getValue(),
                  "Received null Timer from SDK harness: %s",
                  windowedValue);
          LOG.debug("Timer received: {} {}", outputPCollectionId, timer);
          for (Object window : windowedValue.getWindows()) {
            StateNamespace namespace = StateNamespaces.window(windowCoder, (BoundedWindow) window);
            TimerInternals.TimerData timerData =
                TimerInternals.TimerData.of(
                    timerSpec.inputCollectionId(),
                    namespace,
                    timer.getTimestamp(),
                    timerSpec.getTimerSpec().getTimeDomain());
            timerRegistration.accept(windowedValue, timerData);
          }
        }
      }
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return doFn;
    }
  }

  private DoFnRunner<InputT, OutputT> ensureStateDoFnRunner(
      SdkHarnessDoFnRunner<InputT, OutputT> sdkHarnessRunner,
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
        new StateCleaner(userStates, windowCoder, () -> stateBackend.getCurrentKey());

    return new StatefulDoFnRunner<InputT, OutputT, BoundedWindow>(
        sdkHarnessRunner,
        getInputCoder(),
        stepContext,
        windowingStrategy,
        cleanupTimer,
        stateCleaner,
        requiresTimeSortedInput(payload, true)) {

      @Override
      public void processElement(WindowedValue<InputT> input) {
        try (Locker locker = Locker.locked(stateBackendLock)) {
          @SuppressWarnings({"unchecked", "rawtypes"})
          final ByteBuffer key =
              FlinkKeyUtils.encodeKey(((KV) input.getValue()).getKey(), (Coder) keyCoder);
          getKeyedStateBackend().setCurrentKey(key);
          super.processElement(input);
        }
      }

      @Override
      public void finishBundle() {
        // Before cleaning up state, first finish bundle for all underlying DoFnRunners
        super.finishBundle();
        // execute cleanup after the bundle is complete
        if (!stateCleaner.cleanupQueue.isEmpty()) {
          try (Locker locker = Locker.locked(stateBackendLock)) {
            stateCleaner.cleanupState(keyedStateInternals, stateBackend::setCurrentKey);
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
      // make sure this fires after any window.maxTimestamp() timers
      Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy).plus(1);
      // needs to match the encoding in prepareStateBackend for state request handler
      final ByteBuffer key = FlinkKeyUtils.encodeKey(((KV) input).getKey(), keyCoder);
      // Ensure the state backend is not concurrently accessed by the state requests
      try (Locker locker = Locker.locked(stateBackendLock)) {
        keyedStateBackend.setCurrentKey(key);
        timerInternals.setTimer(
            StateNamespaces.window(windowCoder, window),
            GC_TIMER_ID,
            "",
            gcTime,
            window.maxTimestamp(),
            TimeDomain.EVENT_TIME);
      }
    }

    @Override
    public boolean isForWindow(
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
      boolean isEventTimer = timeDomain.equals(TimeDomain.EVENT_TIME);
      Instant gcTime = LateDataUtils.garbageCollectionTime(window, windowingStrategy).plus(1);
      return isEventTimer && GC_TIMER_ID.equals(timerId) && gcTime.equals(timestamp);
    }
  }

  static class StateCleaner implements StatefulDoFnRunner.StateCleaner<BoundedWindow> {

    private final List<String> userStateNames;
    private final Coder windowCoder;
    private final ArrayDeque<KV<ByteBuffer, BoundedWindow>> cleanupQueue;
    private final Supplier<ByteBuffer> currentKeySupplier;

    StateCleaner(
        List<String> userStateNames, Coder windowCoder, Supplier<ByteBuffer> currentKeySupplier) {
      this.userStateNames = userStateNames;
      this.windowCoder = windowCoder;
      this.currentKeySupplier = currentKeySupplier;
      this.cleanupQueue = new ArrayDeque<>();
    }

    @Override
    public void clearForWindow(BoundedWindow window) {
      // Executed in the context of onTimer(..) where the correct key will be set
      cleanupQueue.add(KV.of(currentKeySupplier.get(), window));
    }

    @SuppressWarnings("ByteBufferBackingArray")
    void cleanupState(StateInternals stateInternals, Consumer<ByteBuffer> keyContextConsumer) {
      while (!cleanupQueue.isEmpty()) {
        KV<ByteBuffer, BoundedWindow> kv = cleanupQueue.remove();
        if (LOG.isDebugEnabled()) {
          LOG.debug("State cleanup for {} {}", Arrays.toString(kv.getKey().array()), kv.getValue());
        }
        keyContextConsumer.accept(kv.getKey());
        for (String userState : userStateNames) {
          StateNamespace namespace = StateNamespaces.window(windowCoder, kv.getValue());
          StateTag<BagState<Void>> bagStateStateTag = StateTags.bag(userState, VoidCoder.of());
          BagState<?> state = stateInternals.state(namespace, bagStateStateTag);
          state.clear();
        }
      }
    }
  }

  /**
   * Eagerly create the user state to work around https://jira.apache.org/jira/browse/FLINK-12653.
   */
  private static void initializeUserState(
      ExecutableStage executableStage, @Nullable KeyedStateBackend keyedStateBackend) {
    executableStage
        .getUserStates()
        .forEach(
            ref -> {
              try {
                keyedStateBackend.getOrCreateKeyedState(
                    StringSerializer.INSTANCE,
                    new ListStateDescriptor<>(
                        ref.localName(), new CoderTypeSerializer<>(ByteStringCoder.of())));
              } catch (Exception e) {
                throw new RuntimeException("Couldn't initialize user states.", e);
              }
            });
  }

  private static class NoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }

  private static class Locker implements AutoCloseable {

    public static Locker locked(Lock lock) {
      Locker locker = new Locker(lock);
      lock.lock();
      return locker;
    }

    private final Lock lock;

    Locker(Lock lock) {
      this.lock = lock;
    }

    @Override
    public void close() {
      lock.unlock();
    }
  }
}
