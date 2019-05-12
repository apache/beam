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
package org.apache.beam.runners.flink.translation.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.BatchSideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink operator that passes its input DataSet through an SDK-executed {@link
 * org.apache.beam.runners.core.construction.graph.ExecutableStage}.
 *
 * <p>The output of this operation is a multiplexed DataSet whose elements are tagged with a union
 * coder. The coder's tags are determined by the output coder map. The resulting data set should be
 * further processed by a {@link FlinkExecutableStagePruningFunction}.
 */
public class FlinkExecutableStageFunction<InputT> extends AbstractRichFunction
    implements MapPartitionFunction<WindowedValue<InputT>, RawUnionValue>,
        GroupReduceFunction<WindowedValue<InputT>, RawUnionValue> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutableStageFunction.class);

  // Main constructor fields. All must be Serializable because Flink distributes Functions to
  // task managers via java serialization.

  // The executable stage this function will run.
  private final RunnerApi.ExecutableStagePayload stagePayload;
  // Pipeline options. Used for provisioning api.
  private final JobInfo jobInfo;
  // Map from PCollection id to the union tag used to represent this PCollection in the output.
  private final Map<String, Integer> outputMap;
  private final FlinkExecutableStageContext.Factory contextFactory;
  private final Coder windowCoder;
  // Unique name for namespacing metrics; currently just takes the input ID
  private final String stageName;

  // Worker-local fields. These should only be constructed and consumed on Flink TaskManagers.
  private transient RuntimeContext runtimeContext;
  private transient FlinkMetricContainer container;
  private transient StateRequestHandler stateRequestHandler;
  private transient FlinkExecutableStageContext stageContext;
  private transient StageBundleFactory stageBundleFactory;
  private transient BundleProgressHandler progressHandler;
  // Only initialized when the ExecutableStage is stateful
  private transient InMemoryBagUserStateFactory bagUserStateHandlerFactory;
  private transient ExecutableStage executableStage;
  // In state
  private transient Object currentTimerKey;

  public FlinkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      Map<String, Integer> outputMap,
      FlinkExecutableStageContext.Factory contextFactory,
      Coder windowCoder) {
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
    this.outputMap = outputMap;
    this.contextFactory = contextFactory;
    this.windowCoder = windowCoder;
    this.stageName = stagePayload.getInput();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    // Register standard file systems.
    // TODO Use actual pipeline options.
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    executableStage = ExecutableStage.fromPayload(stagePayload);
    runtimeContext = getRuntimeContext();
    container = new FlinkMetricContainer(getRuntimeContext());
    // TODO: Wire this into the distributed cache and make it pluggable.
    stageContext = contextFactory.get(jobInfo);
    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
    // NOTE: It's safe to reuse the state handler between partitions because each partition uses the
    // same backing runtime context and broadcast variables. We use checkState below to catch errors
    // in backward-incompatible Flink changes.
    stateRequestHandler =
        getStateRequestHandler(
            executableStage, stageBundleFactory.getProcessBundleDescriptor(), runtimeContext);
    progressHandler =
        new BundleProgressHandler() {
          @Override
          public void onProgress(ProcessBundleProgressResponse progress) {
            container.updateMetrics(stageName, progress.getMonitoringInfosList());
          }

          @Override
          public void onCompleted(ProcessBundleResponse response) {
            container.updateMetrics(stageName, response.getMonitoringInfosList());
          }
        };
  }

  private StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage,
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor,
      RuntimeContext runtimeContext) {
    final StateRequestHandler sideInputHandler;
    StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
        BatchSideInputHandlerFactory.forStage(
            executableStage, runtimeContext::getBroadcastVariable);
    try {
      sideInputHandler =
          StateRequestHandlers.forSideInputHandlerFactory(
              ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup state handler", e);
    }

    final StateRequestHandler userStateHandler;
    if (executableStage.getUserStates().size() > 0) {
      bagUserStateHandlerFactory = new InMemoryBagUserStateFactory();
      userStateHandler =
          StateRequestHandlers.forBagUserStateHandlerFactory(
              processBundleDescriptor, bagUserStateHandlerFactory);
    } else {
      userStateHandler = StateRequestHandler.unsupported();
    }

    EnumMap<StateKey.TypeCase, StateRequestHandler> handlerMap =
        new EnumMap<>(StateKey.TypeCase.class);
    handlerMap.put(StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);
    handlerMap.put(StateKey.TypeCase.BAG_USER_STATE, userStateHandler);

    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  /** For non-stateful processing via a simple MapPartitionFunction. */
  @Override
  public void mapPartition(
      Iterable<WindowedValue<InputT>> iterable, Collector<RawUnionValue> collector)
      throws Exception {

    ReceiverFactory receiverFactory = new ReceiverFactory(collector, outputMap);
    try (RemoteBundle bundle =
        stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler)) {
      processElements(iterable, bundle);
    }
  }

  /** For stateful and timer processing via a GroupReduceFunction. */
  @Override
  public void reduce(Iterable<WindowedValue<InputT>> iterable, Collector<RawUnionValue> collector)
      throws Exception {

    // Need to discard the old key's state
    if (bagUserStateHandlerFactory != null) {
      bagUserStateHandlerFactory.resetForNewKey();
    }

    // Used with Batch, we know that all the data is available for this key. We can't use the
    // timer manager from the context because it doesn't exist. So we create one and advance
    // time to the end after processing all elements.
    final InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
    timerInternals.advanceProcessingTime(Instant.now());
    timerInternals.advanceSynchronizedProcessingTime(Instant.now());

    ReceiverFactory receiverFactory =
        new ReceiverFactory(
            collector,
            outputMap,
            new TimerReceiverFactory(
                stageBundleFactory,
                executableStage.getTimers(),
                stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs(),
                (WindowedValue timerElement, TimerInternals.TimerData timerData) -> {
                  currentTimerKey = ((KV) timerElement.getValue()).getKey();
                  timerInternals.setTimer(timerData);
                },
                windowCoder));

    // First process all elements and make sure no more elements can arrive
    try (RemoteBundle bundle =
        stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler)) {
      processElements(iterable, bundle);
    }

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
    // Finally, advance the processing time to infinity to fire any timers.
    timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
    timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

    // Now we fire the timers and process elements generated by timers (which may be timers itself)
    try (RemoteBundle bundle =
        stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler)) {

      fireEligibleTimers(
          timerInternals,
          (String timerId, WindowedValue timerValue) -> {
            FnDataReceiver<WindowedValue<?>> fnTimerReceiver =
                bundle.getInputReceivers().get(timerId);
            Preconditions.checkNotNull(fnTimerReceiver, "No FnDataReceiver found for %s", timerId);
            try {
              fnTimerReceiver.accept(timerValue);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(Locale.ENGLISH, "Failed to process timer: %s", timerValue));
            }
          });
    }
  }

  private void processElements(Iterable<WindowedValue<InputT>> iterable, RemoteBundle bundle)
      throws Exception {
    Preconditions.checkArgument(bundle != null, "RemoteBundle must not be null");

    String inputPCollectionId = executableStage.getInputPCollection().getId();
    FnDataReceiver<WindowedValue<?>> mainReceiver =
        Preconditions.checkNotNull(
            bundle.getInputReceivers().get(inputPCollectionId),
            "Main input receiver for %s could not be initialized",
            inputPCollectionId);
    for (WindowedValue<InputT> input : iterable) {
      mainReceiver.accept(input);
    }
  }

  /**
   * Fires all timers which are ready to be fired. This is done in a loop because timers may itself
   * schedule timers.
   */
  private void fireEligibleTimers(
      InMemoryTimerInternals timerInternals, BiConsumer<String, WindowedValue> timerConsumer) {

    boolean hasFired;
    do {
      hasFired = false;
      TimerInternals.TimerData timer;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        fireTimer(timer, timerConsumer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(timer, timerConsumer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(timer, timerConsumer);
      }
    } while (hasFired);
  }

  private void fireTimer(
      TimerInternals.TimerData timer, BiConsumer<String, WindowedValue> timerConsumer) {
    StateNamespace namespace = timer.getNamespace();
    Preconditions.checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
    BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
    Instant timestamp = timer.getTimestamp();
    WindowedValue<KV<Object, Timer>> timerValue =
        WindowedValue.of(
            KV.of(currentTimerKey, Timer.of(timestamp, new byte[0])),
            timestamp,
            Collections.singleton(window),
            PaneInfo.NO_FIRING);
    timerConsumer.accept(timer.getTimerId(), timerValue);
  }

  @Override
  public void close() throws Exception {
    // close may be called multiple times when an exception is thrown
    if (stageContext != null) {
      try (AutoCloseable bundleFactoryCloser = stageBundleFactory;
          AutoCloseable closable = stageContext) {
      } catch (Exception e) {
        LOG.error("Error in close: ", e);
        throw e;
      }
    }
    stageContext = null;
  }

  /**
   * Receiver factory that wraps outgoing elements with the corresponding union tag for a
   * multiplexed PCollection and optionally handles timer items.
   */
  private static class ReceiverFactory implements OutputReceiverFactory {

    private final Object collectorLock = new Object();

    @GuardedBy("collectorLock")
    private final Collector<RawUnionValue> collector;

    private final Map<String, Integer> outputMap;
    @Nullable private final TimerReceiverFactory timerReceiverFactory;

    ReceiverFactory(Collector<RawUnionValue> collector, Map<String, Integer> outputMap) {
      this(collector, outputMap, null);
    }

    ReceiverFactory(
        Collector<RawUnionValue> collector,
        Map<String, Integer> outputMap,
        @Nullable TimerReceiverFactory timerReceiverFactory) {
      this.collector = collector;
      this.outputMap = outputMap;
      this.timerReceiverFactory = timerReceiverFactory;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String collectionId) {
      Integer unionTag = outputMap.get(collectionId);
      if (unionTag != null) {
        int tagInt = unionTag;
        return receivedElement -> {
          synchronized (collectorLock) {
            collector.collect(new RawUnionValue(tagInt, receivedElement));
          }
        };
      } else if (timerReceiverFactory != null) {
        // Delegate to TimerReceiverFactory
        return timerReceiverFactory.create(collectionId);
      } else {
        throw new IllegalStateException(
            String.format(Locale.ENGLISH, "Unknown PCollectionId %s", collectionId));
      }
    }
  }

  private static class TimerReceiverFactory implements OutputReceiverFactory {

    private final StageBundleFactory stageBundleFactory;
    /** Timer PCollection id => TimerReference. */
    private final HashMap<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToSpecMap;
    /** Timer PCollection id => timer name => TimerSpec. */
    private final Map<String, Map<String, ProcessBundleDescriptors.TimerSpec>> timerSpecMap;

    private final BiConsumer<WindowedValue, TimerInternals.TimerData> timerDataConsumer;
    private final Coder windowCoder;

    TimerReceiverFactory(
        StageBundleFactory stageBundleFactory,
        Collection<TimerReference> timerReferenceCollection,
        Map<String, Map<String, ProcessBundleDescriptors.TimerSpec>> timerSpecMap,
        BiConsumer<WindowedValue, TimerInternals.TimerData> timerDataConsumer,
        Coder windowCoder) {
      this.stageBundleFactory = stageBundleFactory;
      this.timerOutputIdToSpecMap = new HashMap<>();
      // Gather all timers from all transforms by their output pCollectionId which is unique
      for (Map<String, ProcessBundleDescriptors.TimerSpec> transformTimerMap :
          stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().values()) {
        for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
          timerOutputIdToSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
        }
      }
      this.timerSpecMap = timerSpecMap;
      this.timerDataConsumer = timerDataConsumer;
      this.windowCoder = windowCoder;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
      final ProcessBundleDescriptors.TimerSpec timerSpec =
          timerOutputIdToSpecMap.get(pCollectionId);

      return receivedElement -> {
        WindowedValue windowedValue = (WindowedValue) receivedElement;
        Timer timer =
            Preconditions.checkNotNull(
                (Timer) ((KV) windowedValue.getValue()).getValue(),
                "Received null Timer from SDK harness: %s",
                receivedElement);
        LOG.debug("Timer received: {} {}", pCollectionId, timer);
        for (Object window : windowedValue.getWindows()) {
          StateNamespace namespace = StateNamespaces.window(windowCoder, (BoundedWindow) window);
          TimeDomain timeDomain = timerSpec.getTimerSpec().getTimeDomain();
          String timerId = timerSpec.inputCollectionId();
          TimerInternals.TimerData timerData =
              TimerInternals.TimerData.of(timerId, namespace, timer.getTimestamp(), timeDomain);
          timerDataConsumer.accept(windowedValue, timerData);
        }
      };
    }
  }

  /**
   * Holds user state in memory if the ExecutableStage is stateful. Only one key is active at a time
   * due to the GroupReduceFunction being called once per key. Needs to be reset via {@code
   * resetForNewKey()} before processing a new key.
   */
  private static class InMemoryBagUserStateFactory
      implements StateRequestHandlers.BagUserStateHandlerFactory {

    private List<InMemorySingleKeyBagState> handlers;

    private InMemoryBagUserStateFactory() {
      handlers = new ArrayList<>();
    }

    @Override
    public <K, V, W extends BoundedWindow>
        StateRequestHandlers.BagUserStateHandler<K, V, W> forUserState(
            String pTransformId,
            String userStateId,
            Coder<K> keyCoder,
            Coder<V> valueCoder,
            Coder<W> windowCoder) {

      InMemorySingleKeyBagState<K, V, W> bagUserStateHandler =
          new InMemorySingleKeyBagState<>(userStateId, valueCoder, windowCoder);
      handlers.add(bagUserStateHandler);

      return bagUserStateHandler;
    }

    /** Prepares previous emitted state handlers for processing a new key. */
    void resetForNewKey() {
      for (InMemorySingleKeyBagState stateBags : handlers) {
        stateBags.reset();
      }
    }

    static class InMemorySingleKeyBagState<K, V, W extends BoundedWindow>
        implements StateRequestHandlers.BagUserStateHandler<K, V, W> {

      private final StateTag<BagState<V>> stateTag;
      private final Coder<W> windowCoder;

      /* Lazily initialized state internals upon first access */
      private volatile StateInternals stateInternals;

      InMemorySingleKeyBagState(String userStateId, Coder<V> valueCoder, Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
        this.stateTag = StateTags.bag(userStateId, valueCoder);
      }

      @Override
      public Iterable<V> get(K key, W window) {
        initStateInternals(key);
        StateNamespace namespace = StateNamespaces.window(windowCoder, window);
        BagState<V> bagState = stateInternals.state(namespace, stateTag);
        return bagState.read();
      }

      @Override
      public void append(K key, W window, Iterator<V> values) {
        initStateInternals(key);
        StateNamespace namespace = StateNamespaces.window(windowCoder, window);
        BagState<V> bagState = stateInternals.state(namespace, stateTag);
        while (values.hasNext()) {
          bagState.add(values.next());
        }
      }

      @Override
      public void clear(K key, W window) {
        initStateInternals(key);
        StateNamespace namespace = StateNamespaces.window(windowCoder, window);
        BagState<V> bagState = stateInternals.state(namespace, stateTag);
        bagState.clear();
      }

      private void initStateInternals(K key) {
        if (stateInternals == null) {
          stateInternals = InMemoryStateInternals.forKey(key);
        }
      }

      void reset() {
        stateInternals = null;
      }
    }
  }
}
