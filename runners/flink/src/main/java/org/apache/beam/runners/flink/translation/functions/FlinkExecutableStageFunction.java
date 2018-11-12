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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
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
  private final boolean stateful;

  // Worker-local fields. These should only be constructed and consumed on Flink TaskManagers.
  private transient RuntimeContext runtimeContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient FlinkExecutableStageContext stageContext;
  private transient StageBundleFactory stageBundleFactory;
  private transient BundleProgressHandler progressHandler;
  // Only initialized when the ExecutableStage is stateful
  private transient InMemoryBagUserStateFactory bagUserStateHandlerFactory;

  public FlinkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      Map<String, Integer> outputMap,
      FlinkExecutableStageContext.Factory contextFactory,
      boolean stateful) {
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
    this.outputMap = outputMap;
    this.contextFactory = contextFactory;
    this.stateful = stateful;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    // Register standard file systems.
    // TODO Use actual pipeline options.
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
    runtimeContext = getRuntimeContext();
    // TODO: Wire this into the distributed cache and make it pluggable.
    stageContext = contextFactory.get(jobInfo);
    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
    // NOTE: It's safe to reuse the state handler between partitions because each partition uses the
    // same backing runtime context and broadcast variables. We use checkState below to catch errors
    // in backward-incompatible Flink changes.
    stateRequestHandler =
        getStateRequestHandler(
            executableStage, stageBundleFactory.getProcessBundleDescriptor(), runtimeContext);
    progressHandler = BundleProgressHandler.ignored();
  }

  private StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage,
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor,
      RuntimeContext runtimeContext) {
    final StateRequestHandler sideInputHandler;
    StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
        FlinkBatchSideInputHandlerFactory.forStage(executableStage, runtimeContext);
    try {
      sideInputHandler =
          StateRequestHandlers.forSideInputHandlerFactory(
              ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup state handler", e);
    }

    final StateRequestHandler userStateHandler;
    if (stateful) {
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
    processElements(iterable, collector);
  }

  /** For stateful processing via a GroupReduceFunction. */
  @Override
  public void reduce(Iterable<WindowedValue<InputT>> iterable, Collector<RawUnionValue> collector)
      throws Exception {
    bagUserStateHandlerFactory.resetForNewKey();
    processElements(iterable, collector);
  }

  private void processElements(
      Iterable<WindowedValue<InputT>> iterable, Collector<RawUnionValue> collector)
      throws Exception {
    checkState(
        runtimeContext == getRuntimeContext(),
        "RuntimeContext changed from under us. State handler invalid.");
    checkState(
        stageBundleFactory != null, "%s not yet prepared", StageBundleFactory.class.getName());
    checkState(
        stateRequestHandler != null, "%s not yet prepared", StateRequestHandler.class.getName());

    try (RemoteBundle bundle =
        stageBundleFactory.getBundle(
            new ReceiverFactory(collector, outputMap), stateRequestHandler, progressHandler)) {
      // TODO(BEAM-4681): Add support to Flink to support portable timers.
      FnDataReceiver<WindowedValue<?>> receiver =
          Iterables.getOnlyElement(bundle.getInputReceivers().values());
      for (WindowedValue<InputT> input : iterable) {
        receiver.accept(input);
      }
    }
    // NOTE: RemoteBundle.close() blocks on completion of all data receivers. This is necessary to
    // safely reference the partition-scoped Collector from receivers.
  }

  @Override
  public void close() throws Exception {
    // close may be called multiple times when an exception is thrown
    if (stageContext != null) {
      try (@SuppressWarnings("unused")
              AutoCloseable bundleFactoryCloser = stageBundleFactory;
          @SuppressWarnings("unused")
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
   * multiplexed PCollection.
   */
  private static class ReceiverFactory implements OutputReceiverFactory {

    private final Object collectorLock = new Object();

    @GuardedBy("collectorLock")
    private final Collector<RawUnionValue> collector;

    private final Map<String, Integer> outputMap;

    ReceiverFactory(Collector<RawUnionValue> collector, Map<String, Integer> outputMap) {
      this.collector = collector;
      this.outputMap = outputMap;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String collectionId) {
      Integer unionTag = outputMap.get(collectionId);
      checkArgument(unionTag != null, "Unknown PCollection id: %s", collectionId);
      int tagInt = unionTag;
      return receivedElement -> {
        synchronized (collectorLock) {
          collector.collect(new RawUnionValue(tagInt, receivedElement));
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
