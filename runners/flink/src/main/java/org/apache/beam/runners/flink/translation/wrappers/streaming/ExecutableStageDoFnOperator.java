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

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContext;
import org.apache.beam.runners.flink.translation.functions.FlinkStreamingSideInputHandlerFactory;
import org.apache.beam.runners.flink.translation.utils.NoopLock;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.flink.api.java.functions.KeySelector;
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
  private final FlinkExecutableStageContext.Factory contextFactory;
  private final Map<String, TupleTag<?>> outputMap;
  private final Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds;
  private final boolean usesTimers;
  /** A lock which has to be acquired when concurrently accessing state and setting timers. */
  private final Lock stateBackendLock;

  private transient FlinkExecutableStageContext stageContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient BundleProgressHandler progressHandler;
  private transient StageBundleFactory stageBundleFactory;
  private transient ExecutableStage executableStage;
  private transient SdkHarnessDoFnRunner<InputT, OutputT> sdkHarnessRunner;
  private transient FlinkMetricContainer flinkMetricContainer;
  private transient long backupWatermarkHold = Long.MIN_VALUE;

  /** Constructor. */
  public ExecutableStageDoFnOperator(
      String stepName,
      Coder<WindowedValue<InputT>> windowedInputCoder,
      Coder<InputT> inputCoder,
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
      FlinkExecutableStageContext.Factory contextFactory,
      Map<String, TupleTag<?>> outputMap,
      WindowingStrategy windowingStrategy,
      Coder keyCoder,
      KeySelector<WindowedValue<InputT>, ?> keySelector) {
    super(
        new NoOpDoFn(),
        stepName,
        windowedInputCoder,
        inputCoder,
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
        DoFnSchemaInformation.create());
    this.payload = payload;
    this.jobInfo = jobInfo;
    this.contextFactory = contextFactory;
    this.outputMap = outputMap;
    this.sideInputIds = sideInputIds;
    this.usesTimers = payload.getTimersCount() > 0;
    if (usesTimers) {
      // We only need to lock if we have timers. 1) Timers can
      // interfere with state access. 2) Even without state access,
      // setting timers can interfere with firing timers.
      this.stateBackendLock = new ReentrantLock();
    } else {
      // Plain state access is guaranteed to not interfere with the state
      // backend. The current key of the state backend is set manually before
      // accessing the keyed state. Flink's automatic key setting before
      // processing elements is overridden in this class.
      this.stateBackendLock = NoopLock.get();
    }
  }

  @Override
  public void open() throws Exception {
    executableStage = ExecutableStage.fromPayload(payload);
    // TODO: Wire this into the distributed cache and make it pluggable.
    // TODO: Do we really want this layer of indirection when accessing the stage bundle factory?
    // It's a little strange because this operator is responsible for the lifetime of the stage
    // bundle "factory" (manager?) but not the job or Flink bundle factories. How do we make
    // ownership of the higher level "factories" explicit? Do we care?
    stageContext = contextFactory.get(jobInfo);
    flinkMetricContainer = new FlinkMetricContainer(getRuntimeContext());

    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
    stateRequestHandler = getStateRequestHandler(executableStage);
    progressHandler =
        new BundleProgressHandler() {
          @Override
          public void onProgress(ProcessBundleProgressResponse progress) {
            flinkMetricContainer.updateMetrics(stepName, progress.getMonitoringInfosList());
          }

          @Override
          public void onCompleted(ProcessBundleResponse response) {
            flinkMetricContainer.updateMetrics(stepName, response.getMonitoringInfosList());
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
    if (executableStage.getUserStates().size() > 0) {
      if (keyedStateInternals == null) {
        throw new IllegalStateException("Input must be keyed when user state is used");
      }
      userStateRequestHandler =
          StateRequestHandlers.forBagUserStateHandlerFactory(
              stageBundleFactory.getProcessBundleDescriptor(),
              new BagUserStateFactory(
                  keyedStateInternals, getKeyedStateBackend(), stateBackendLock));
    } else {
      userStateRequestHandler = StateRequestHandler.unsupported();
    }

    EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(TypeCase.class);
    handlerMap.put(TypeCase.MULTIMAP_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(TypeCase.BAG_USER_STATE, userStateRequestHandler);

    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  private static class BagUserStateFactory
      implements StateRequestHandlers.BagUserStateHandlerFactory {

    private final StateInternals stateInternals;
    private final KeyedStateBackend<ByteBuffer> keyedStateBackend;
    private final Lock stateBackendLock;

    private BagUserStateFactory(
        StateInternals stateInternals,
        KeyedStateBackend<ByteBuffer> keyedStateBackend,
        Lock stateBackendLock) {

      this.stateInternals = stateInternals;
      this.keyedStateBackend = keyedStateBackend;
      this.stateBackendLock = stateBackendLock;
    }

    @Override
    public <K, V, W extends BoundedWindow>
        StateRequestHandlers.BagUserStateHandler<K, V, W> forUserState(
            String pTransformId,
            String userStateId,
            Coder<K> keyCoder,
            Coder<V> valueCoder,
            Coder<W> windowCoder) {
      return new StateRequestHandlers.BagUserStateHandler<K, V, W>() {
        @Override
        public Iterable<V> get(K key, W window) {
          try {
            stateBackendLock.lock();
            prepareStateBackend(key, keyCoder);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            return bagState.read();
          } finally {
            stateBackendLock.unlock();
          }
        }

        @Override
        public void append(K key, W window, Iterator<V> values) {
          try {
            stateBackendLock.lock();
            prepareStateBackend(key, keyCoder);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            while (values.hasNext()) {
              bagState.add(values.next());
            }
          } finally {
            stateBackendLock.unlock();
          }
        }

        @Override
        public void clear(K key, W window) {
          try {
            stateBackendLock.lock();
            prepareStateBackend(key, keyCoder);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            bagState.clear();
          } finally {
            stateBackendLock.unlock();
          }
        }

        private void prepareStateBackend(K key, Coder<K> keyCoder) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            keyCoder.encode(key, baos);
          } catch (IOException e) {
            throw new RuntimeException("Failed to encode key for Flink state backend", e);
          }
          keyedStateBackend.setCurrentKey(ByteBuffer.wrap(baos.toByteArray()));
        }
      };
    }
  }

  @Override
  public void setKeyContextElement1(StreamRecord record) throws Exception {
    // Note: This is only relevant when we have a stateful DoFn.
    // We want to control the key of the state backend ourselves and
    // we must avoid any concurrent setting of the current active key.
    // By overwriting this, we also prevent unnecessary serialization
    // as the key has to be encoded as a byte array.
  }

  @Override
  public void setCurrentKey(Object key) {
    // We don't need to set anything, the key is set manually on the state backend in
    // the case of state access. For timers, the key will be extracted from the timer
    // element, i.e. in HeapInternalTimerService
    if (!usesTimers) {
      throw new UnsupportedOperationException(
          "Current key for state backend can only be set by state requests from SDK workers or when processing timers.");
    }
  }

  @Override
  public Object getCurrentKey() {
    // This is the key retrieved by HeapInternalTimerService when setting a Flink timer
    return sdkHarnessRunner.getCurrentTimerKey();
  }

  private void setTimer(WindowedValue<InputT> timerElement, TimerInternals.TimerData timerData) {
    try {
      Object key = keySelector.getKey(timerElement);
      sdkHarnessRunner.setCurrentTimerKey(key);
      // We have to synchronize to ensure the state backend is not concurrently accessed by the
      // state requests
      try {
        stateBackendLock.lock();
        getKeyedStateBackend().setCurrentKey(key);
        timerInternals.setTimer(timerData);
      } finally {
        stateBackendLock.unlock();
      }
    } catch (Exception e) {
      throw new RuntimeException("Couldn't set timer", e);
    } finally {
      sdkHarnessRunner.setCurrentTimerKey(null);
    }
  }

  @Override
  public void fireTimer(InternalTimer<?, TimerInternals.TimerData> timer) {
    // We need to decode the key
    final ByteBuffer encodedKey = (ByteBuffer) timer.getKey();
    @SuppressWarnings("ByteBufferBackingArray")
    byte[] bytes = encodedKey.array();
    final Object decodedKey;
    try {
      decodedKey = CoderUtils.decodeFromByteArray(keyCoder, bytes);
    } catch (CoderException e) {
      throw new RuntimeException(
          String.format(Locale.ENGLISH, "Failed to decode encoded key: %s", Arrays.toString(bytes)),
          e);
    }
    // Prepare the SdkHarnessRunner with the key for the timer
    sdkHarnessRunner.setCurrentTimerKey(decodedKey);
    // We have to synchronize to ensure the state backend is not concurrently accessed by the state
    // requests
    try {
      stateBackendLock.lock();
      getKeyedStateBackend().setCurrentKey(encodedKey);
      super.fireTimer(timer);
    } finally {
      stateBackendLock.unlock();
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
      DoFnRunner<InputT, OutputT> wrappedRunner) {
    sdkHarnessRunner =
        new SdkHarnessDoFnRunner<>(
            executableStage.getInputPCollection().getId(),
            stageBundleFactory,
            stateRequestHandler,
            progressHandler,
            outputManager,
            outputMap,
            (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder(),
            keySelector,
            this::setTimer);
    return sdkHarnessRunner;
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
    private final KeySelector<WindowedValue<InputT>, ?> keySelector;
    private final BiConsumer<WindowedValue<InputT>, TimerInternals.TimerData> timerRegistration;

    private RemoteBundle remoteBundle;
    private FnDataReceiver<WindowedValue<?>> mainInputReceiver;
    // Timer key set before calling Flink's internal timer service to register
    // a timer. The timer service will retrieve this with a call to {@code getCurrentKey}.
    // Before firing a timer, this will be initialized with the current key
    // from the timer element.
    private Object currentTimerKey;

    public SdkHarnessDoFnRunner(
        String mainInput,
        StageBundleFactory stageBundleFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler,
        BufferedOutputManager<OutputT> outputManager,
        Map<String, TupleTag<?>> outputMap,
        Coder<BoundedWindow> windowCoder,
        KeySelector<WindowedValue<InputT>, ?> keySelector,
        BiConsumer<WindowedValue<InputT>, TimerInternals.TimerData> timerRegistration) {
      this.mainInput = mainInput;
      this.stageBundleFactory = stageBundleFactory;
      this.stateRequestHandler = stateRequestHandler;
      this.progressHandler = progressHandler;
      this.outputManager = outputManager;
      this.outputMap = outputMap;
      this.keySelector = keySelector;
      this.timerRegistration = timerRegistration;
      this.timerOutputIdToSpecMap = new HashMap<>();
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
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
      Preconditions.checkNotNull(
          currentTimerKey, "Key for timer needs to be set before calling onTimer");
      Preconditions.checkNotNull(remoteBundle, "Call to onTimer outside of a bundle");
      LOG.debug("timer callback: {} {} {} {}", timerId, window, timestamp, timeDomain);
      FnDataReceiver<WindowedValue<?>> timerReceiver =
          Preconditions.checkNotNull(
              remoteBundle.getInputReceivers().get(timerId),
              "No receiver found for timer %s",
              timerId);
      WindowedValue<KV<Object, Timer>> timerValue =
          WindowedValue.of(
              KV.of(currentTimerKey, Timer.of(timestamp, new byte[0])),
              timestamp,
              Collections.singleton(window),
              PaneInfo.NO_FIRING);
      try {
        timerReceiver.accept(timerValue);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Failed to process timer %s", timerReceiver), e);
      } finally {
        currentTimerKey = null;
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
        throw new RuntimeException("Failed to finish remote bundle", e);
      } finally {
        remoteBundle = null;
      }
    }

    /** Key for timer which has not been registered yet. */
    Object getCurrentTimerKey() {
      return currentTimerKey;
    }

    /** Key for timer which is about to be fired. */
    void setCurrentTimerKey(Object key) {
      this.currentTimerKey = key;
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
      throw new UnsupportedOperationException();
    }
  }

  private static class NoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }
}
