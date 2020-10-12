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
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandler;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.InMemoryBagUserStateFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.translation.BatchSideInputHandlerFactory;
import org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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

  // Pipeline options for initializing the FileSystems
  private final SerializablePipelineOptions pipelineOptions;
  // The executable stage this function will run.
  private final RunnerApi.ExecutableStagePayload stagePayload;
  // Pipeline options. Used for provisioning api.
  private final JobInfo jobInfo;
  // Map from PCollection id to the union tag used to represent this PCollection in the output.
  private final Map<String, Integer> outputMap;
  private final FlinkExecutableStageContextFactory contextFactory;
  private final Coder windowCoder;
  // Unique name for namespacing metrics
  private final String stepName;

  // Worker-local fields. These should only be constructed and consumed on Flink TaskManagers.
  private transient RuntimeContext runtimeContext;
  private transient FlinkMetricContainer metricContainer;
  private transient StateRequestHandler stateRequestHandler;
  private transient ExecutableStageContext stageContext;
  private transient StageBundleFactory stageBundleFactory;
  private transient BundleProgressHandler progressHandler;
  private transient BundleFinalizationHandler finalizationHandler;
  // Only initialized when the ExecutableStage is stateful
  private transient InMemoryBagUserStateFactory bagUserStateHandlerFactory;
  private transient ExecutableStage executableStage;
  // In state
  private transient Object currentTimerKey;

  public FlinkExecutableStageFunction(
      String stepName,
      PipelineOptions pipelineOptions,
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      Map<String, Integer> outputMap,
      FlinkExecutableStageContextFactory contextFactory,
      Coder windowCoder) {
    this.stepName = stepName;
    this.pipelineOptions = new SerializablePipelineOptions(pipelineOptions);
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
    this.outputMap = outputMap;
    this.contextFactory = contextFactory;
    this.windowCoder = windowCoder;
  }

  @Override
  public void open(Configuration parameters) {
    FlinkPipelineOptions options = pipelineOptions.get().as(FlinkPipelineOptions.class);
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    executableStage = ExecutableStage.fromPayload(stagePayload);
    runtimeContext = getRuntimeContext();
    metricContainer = new FlinkMetricContainer(runtimeContext);
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
            metricContainer.updateMetrics(stepName, progress.getMonitoringInfosList());
          }

          @Override
          public void onCompleted(ProcessBundleResponse response) {
            metricContainer.updateMetrics(stepName, response.getMonitoringInfosList());
          }
        };
    // TODO(BEAM-11021): Support bundle finalization in portable batch.
    finalizationHandler =
        bundleId -> {
          throw new UnsupportedOperationException(
              "Portable Flink runner doesn't support bundle finalization in batch mode. For more details, please refer to https://issues.apache.org/jira/browse/BEAM-11021.");
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
      bagUserStateHandlerFactory = new InMemoryBagUserStateFactory<>();
      userStateHandler =
          StateRequestHandlers.forBagUserStateHandlerFactory(
              processBundleDescriptor, bagUserStateHandlerFactory);
    } else {
      userStateHandler = StateRequestHandler.unsupported();
    }

    EnumMap<StateKey.TypeCase, StateRequestHandler> handlerMap =
        new EnumMap<>(StateKey.TypeCase.class);
    handlerMap.put(StateKey.TypeCase.ITERABLE_SIDE_INPUT, sideInputHandler);
    handlerMap.put(StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);
    handlerMap.put(StateKey.TypeCase.MULTIMAP_KEYS_SIDE_INPUT, sideInputHandler);
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
        stageBundleFactory.getBundle(
            receiverFactory, stateRequestHandler, progressHandler, finalizationHandler)) {
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

    ReceiverFactory receiverFactory = new ReceiverFactory(collector, outputMap);

    TimerReceiverFactory timerReceiverFactory =
        new TimerReceiverFactory(
            stageBundleFactory,
            (Timer<?> timer, TimerInternals.TimerData timerData) -> {
              currentTimerKey = timer.getUserKey();
              if (timer.getClearBit()) {
                timerInternals.deleteTimer(timerData);
              } else {
                timerInternals.setTimer(timerData);
              }
            },
            windowCoder);

    // First process all elements and make sure no more elements can arrive
    try (RemoteBundle bundle =
        stageBundleFactory.getBundle(
            receiverFactory, timerReceiverFactory, stateRequestHandler, progressHandler)) {
      processElements(iterable, bundle);
    }

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
    // Finally, advance the processing time to infinity to fire any timers.
    timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
    timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

    // Now we fire the timers and process elements generated by timers (which may be timers itself)
    while (timerInternals.hasPendingTimers()) {
      try (RemoteBundle bundle =
          stageBundleFactory.getBundle(
              receiverFactory, timerReceiverFactory, stateRequestHandler, progressHandler)) {

        PipelineTranslatorUtils.fireEligibleTimers(
            timerInternals, bundle.getTimerReceivers(), currentTimerKey);
      }
    }
  }

  private void processElements(Iterable<WindowedValue<InputT>> iterable, RemoteBundle bundle)
      throws Exception {
    Preconditions.checkArgument(bundle != null, "RemoteBundle must not be null");

    FnDataReceiver<WindowedValue<?>> mainReceiver =
        Iterables.getOnlyElement(bundle.getInputReceivers().values());
    for (WindowedValue<InputT> input : iterable) {
      mainReceiver.accept(input);
    }
  }

  @Override
  public void close() throws Exception {
    metricContainer.registerMetricsForPipelineResult();
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

    ReceiverFactory(Collector<RawUnionValue> collector, Map<String, Integer> outputMap) {
      this.collector = collector;
      this.outputMap = outputMap;
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
      } else {
        throw new IllegalStateException(
            String.format(Locale.ENGLISH, "Unknown PCollectionId %s", collectionId));
      }
    }
  }
}
