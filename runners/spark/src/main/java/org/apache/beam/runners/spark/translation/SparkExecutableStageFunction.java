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
package org.apache.beam.runners.spark.translation;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandler;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandlers;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandler;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
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
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues.WindowedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.Instant;
import scala.Tuple2;

/**
 * Spark function that passes its input through an SDK-executed {@link
 * org.apache.beam.sdk.util.construction.graph.ExecutableStage}.
 *
 * <p>The output of this operation is a multiplexed {@link Dataset} whose elements are tagged with a
 * union coder. The coder's tags are determined by {@link SparkExecutableStageFunction#outputMap}.
 * The resulting data set should be further processed by a {@link
 * SparkExecutableStageExtractionFunction}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SparkExecutableStageFunction<InputT, SideInputT>
    implements FlatMapFunction<Iterator<WindowedValue<InputT>>, RawUnionValue> {

  // Pipeline options for initializing the FileSystems
  private final SerializablePipelineOptions pipelineOptions;
  private final RunnerApi.ExecutableStagePayload stagePayload;
  private final Map<String, Integer> outputMap;
  private final SparkExecutableStageContextFactory contextFactory;
  // map from pCollection id to tuple of serialized bytes and coder to decode the bytes
  private final Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>>
      sideInputs;
  private final MetricsContainerStepMapAccumulator metricsAccumulator;
  private final Coder windowCoder;
  // Coder for this stage's input, used to hold and replay splittable DoFn residuals.
  private final Coder<WindowedValue<InputT>> inputCoder;
  // Batch replays residuals in place. Streaming has nowhere to hold them across micro-batches yet.
  private final boolean batch;
  private final JobInfo jobInfo;

  private transient InMemoryBagUserStateFactory bagUserStateHandlerFactory;
  private transient Object currentTimerKey;
  private transient InMemoryTimerInternals sdfTimerInternals;
  private transient StateInternals sdfStateInternals;

  SparkExecutableStageFunction(
      SerializablePipelineOptions pipelineOptions,
      RunnerApi.ExecutableStagePayload stagePayload,
      JobInfo jobInfo,
      Map<String, Integer> outputMap,
      SparkExecutableStageContextFactory contextFactory,
      Map<String, Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>>> sideInputs,
      MetricsContainerStepMapAccumulator metricsAccumulator,
      Coder windowCoder,
      Coder<WindowedValue<InputT>> inputCoder,
      boolean batch) {
    this.pipelineOptions = pipelineOptions;
    this.stagePayload = stagePayload;
    this.jobInfo = jobInfo;
    this.outputMap = outputMap;
    this.contextFactory = contextFactory;
    this.sideInputs = sideInputs;
    this.metricsAccumulator = metricsAccumulator;
    this.windowCoder = windowCoder;
    this.inputCoder = inputCoder;
    this.batch = batch;
  }

  /** Call the executable stage function on the values of a PairRDD, ignoring the key. */
  FlatMapFunction<Tuple2<ByteArray, Iterable<WindowedValue<InputT>>>, RawUnionValue> forPair() {
    return (input) -> call(input._2.iterator());
  }

  @Override
  public Iterator<RawUnionValue> call(Iterator<WindowedValue<InputT>> inputs) throws Exception {
    SparkPipelineOptions options = pipelineOptions.get().as(SparkPipelineOptions.class);
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(options);

    // Do not call processElements if there are no inputs
    // Otherwise, this may cause validation errors (e.g. ParDoTest)
    if (!inputs.hasNext()) {
      return Collections.emptyIterator();
    }

    try (ExecutableStageContext stageContext = contextFactory.get(jobInfo)) {
      ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
      try (StageBundleFactory stageBundleFactory =
          stageContext.getStageBundleFactory(executableStage)) {
        ConcurrentLinkedQueue<RawUnionValue> collector = new ConcurrentLinkedQueue<>();
        StateRequestHandler stateRequestHandler =
            getStateRequestHandler(
                executableStage, stageBundleFactory.getProcessBundleDescriptor());
        BundleCheckpointHandler checkpointHandler = getBundleCheckpointHandler(executableStage);
        if (executableStage.getTimers().size() == 0) {
          ReceiverFactory receiverFactory = new ReceiverFactory(collector, outputMap);
          processElements(
              stateRequestHandler,
              receiverFactory,
              null,
              stageBundleFactory,
              inputs,
              checkpointHandler);
          replaySdfResiduals(
              stateRequestHandler, receiverFactory, null, stageBundleFactory, checkpointHandler);
          return collector.iterator();
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

        // Process inputs.
        processElements(
            stateRequestHandler,
            receiverFactory,
            timerReceiverFactory,
            stageBundleFactory,
            inputs,
            checkpointHandler);

        // Finish any pending windows by advancing the input watermark to infinity.
        timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
        // Finally, advance the processing time to infinity to fire any timers.
        timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
        timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

        // Now we fire the timers and process elements generated by timers (which may be timers
        // itself). A replayed splittable DoFn residual can set a timer, and a fired timer can
        // produce a residual, so alternate until neither has anything left.
        do {
          while (timerInternals.hasPendingTimers()) {
            try (RemoteBundle bundle =
                stageBundleFactory.getBundle(
                    receiverFactory,
                    timerReceiverFactory,
                    stateRequestHandler,
                    getBundleProgressHandler(),
                    getBundleFinalizationHandler(),
                    checkpointHandler)) {

              PipelineTranslatorUtils.fireEligibleTimers(
                  timerInternals, bundle.getTimerReceivers(), currentTimerKey);
            }
          }
          replaySdfResiduals(
              stateRequestHandler,
              receiverFactory,
              timerReceiverFactory,
              stageBundleFactory,
              checkpointHandler);
        } while (timerInternals.hasPendingTimers());
        return collector.iterator();
      }
    }
  }

  // Processes the inputs of the executable stage. Output is returned via side effects on the
  // receiver.
  private void processElements(
      StateRequestHandler stateRequestHandler,
      ReceiverFactory receiverFactory,
      TimerReceiverFactory timerReceiverFactory,
      StageBundleFactory stageBundleFactory,
      Iterator<WindowedValue<InputT>> inputs,
      BundleCheckpointHandler checkpointHandler)
      throws Exception {
    try (RemoteBundle bundle =
        stageBundleFactory.getBundle(
            receiverFactory,
            timerReceiverFactory,
            stateRequestHandler,
            getBundleProgressHandler(),
            getBundleFinalizationHandler(),
            checkpointHandler)) {
      FnDataReceiver<WindowedValue<?>> mainReceiver =
          Iterables.getOnlyElement(bundle.getInputReceivers().values());
      while (inputs.hasNext()) {
        WindowedValue<InputT> input = inputs.next();
        mainReceiver.accept(input);
      }
    }
  }

  private static boolean hasSdf(ExecutableStage executableStage) {
    return executableStage.getTransforms().stream()
        .anyMatch(
            transform ->
                transform
                    .getTransform()
                    .getSpec()
                    .getUrn()
                    .equals(
                        PTransformTranslation
                            .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN));
  }

  // Holds a splittable DoFn's self-checkpoint residual in memory under a processing time timer, so
  // it can be replayed once this stage has drained its inputs.
  private BundleCheckpointHandler getBundleCheckpointHandler(ExecutableStage executableStage) {
    sdfTimerInternals = null;
    sdfStateInternals = null;
    if (!batch) {
      return response -> {
        throw new UnsupportedOperationException(
            "Splittable DoFn self-checkpointing is not supported on the portable Spark runner in "
                + "streaming mode. For more details, please refer to "
                + "https://github.com/apache/beam/issues/19468.");
      };
    }
    if (!hasSdf(executableStage)) {
      return response -> {
        throw new UnsupportedOperationException(
            "Self-checkpoint is only supported on splittable DoFn.");
      };
    }
    sdfTimerInternals = new InMemoryTimerInternals();
    sdfStateInternals = InMemoryStateInternals.forKey("sdf_state");
    return new BundleCheckpointHandlers.StateAndTimerBundleCheckpointHandler<>(
        key -> sdfTimerInternals, key -> sdfStateInternals, inputCoder, windowCoder);
  }

  // Bundle finalization needs the runner to have durably committed the bundle's output first, which
  // this runner cannot report, so it is rejected rather than silently finalized early.
  private BundleFinalizationHandler getBundleFinalizationHandler() {
    return bundleId -> {
      throw new UnsupportedOperationException(
          "The portable Spark runner does not support bundle finalization. For more details, please "
              + "refer to https://github.com/apache/beam/issues/19517.");
    };
  }

  // Replays held residuals until the splittable DoFn stops asking to resume. Processing time is at
  // infinity, so every residual is due immediately and a bounded restriction always runs out.
  private void replaySdfResiduals(
      StateRequestHandler stateRequestHandler,
      ReceiverFactory receiverFactory,
      TimerReceiverFactory timerReceiverFactory,
      StageBundleFactory stageBundleFactory,
      BundleCheckpointHandler checkpointHandler)
      throws Exception {
    if (sdfTimerInternals == null) {
      return;
    }
    sdfTimerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
    sdfTimerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
    while (sdfTimerInternals.hasPendingTimers()) {
      try (RemoteBundle bundle =
          stageBundleFactory.getBundle(
              receiverFactory,
              timerReceiverFactory,
              stateRequestHandler,
              getBundleProgressHandler(),
              getBundleFinalizationHandler(),
              checkpointHandler)) {
        List<WindowedValue<InputT>> residuals = new ArrayList<>();
        TimerInternals.TimerData timer;
        while ((timer = sdfTimerInternals.removeNextProcessingTimer()) != null) {
          MapState<String, WindowedValue<InputT>> residualState =
              sdfStateInternals.state(
                  timer.getNamespace(),
                  BundleCheckpointHandlers.StateAndTimerBundleCheckpointHandler.residualStateTag(
                      inputCoder));
          WindowedValue<InputT> residual = residualState.get(timer.getTimerId()).read();
          residualState.remove(timer.getTimerId());
          if (residual != null) {
            residuals.add(residual);
          }
        }
        FnDataReceiver<WindowedValue<?>> mainReceiver =
            Iterables.getOnlyElement(bundle.getInputReceivers().values());
        for (WindowedValue<InputT> residual : residuals) {
          mainReceiver.accept(residual);
        }
      }
    }
  }

  private BundleProgressHandler getBundleProgressHandler() {
    String stageName = stagePayload.getInput();
    MetricsContainerImpl container = metricsAccumulator.value().getContainer(stageName);
    return new BundleProgressHandler() {
      @Override
      public void onProgress(ProcessBundleProgressResponse progress) {
        container.update(progress.getMonitoringInfosList());
      }

      @Override
      public void onCompleted(ProcessBundleResponse response) {
        container.update(response.getMonitoringInfosList());
      }
    };
  }

  private StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage,
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor) {
    EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(StateKey.TypeCase.class);
    final StateRequestHandler sideInputHandler;
    StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
        BatchSideInputHandlerFactory.forStage(
            executableStage,
            new BatchSideInputHandlerFactory.SideInputGetter() {
              @Override
              public <T> List<T> getSideInput(String pCollectionId) {
                Tuple2<Broadcast<List<byte[]>>, WindowedValueCoder<SideInputT>> tuple2 =
                    sideInputs.get(pCollectionId);
                Broadcast<List<byte[]>> broadcast = tuple2._1;
                WindowedValueCoder<SideInputT> coder = tuple2._2;
                return (List<T>)
                    broadcast.value().stream()
                        .map(bytes -> CoderHelpers.fromByteArray(bytes, coder))
                        .collect(Collectors.toList());
              }
            });
    try {
      sideInputHandler =
          StateRequestHandlers.forSideInputHandlerFactory(
              ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup state handler", e);
    }

    if (bagUserStateHandlerFactory == null) {
      bagUserStateHandlerFactory = new InMemoryBagUserStateFactory();
    }

    final StateRequestHandler userStateHandler;
    if (executableStage.getUserStates().size() > 0) {
      // Need to discard the old key's state
      bagUserStateHandlerFactory.resetForNewKey();
      userStateHandler =
          StateRequestHandlers.forBagUserStateHandlerFactory(
              processBundleDescriptor, bagUserStateHandlerFactory);
    } else {
      userStateHandler = StateRequestHandler.unsupported();
    }

    handlerMap.put(StateKey.TypeCase.ITERABLE_SIDE_INPUT, sideInputHandler);
    handlerMap.put(StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);
    handlerMap.put(StateKey.TypeCase.MULTIMAP_KEYS_SIDE_INPUT, sideInputHandler);
    handlerMap.put(StateKey.TypeCase.BAG_USER_STATE, userStateHandler);
    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  interface JobBundleFactoryCreator extends Serializable {
    JobBundleFactory create();
  }

  /**
   * Receiver factory that wraps outgoing elements with the corresponding union tag for a
   * multiplexed PCollection.
   */
  private static class ReceiverFactory implements OutputReceiverFactory {

    private final ConcurrentLinkedQueue<RawUnionValue> collector;
    private final Map<String, Integer> outputMap;

    ReceiverFactory(
        ConcurrentLinkedQueue<RawUnionValue> collector, Map<String, Integer> outputMap) {
      this.collector = collector;
      this.outputMap = outputMap;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
      Integer unionTag = outputMap.get(pCollectionId);
      if (unionTag != null) {
        int tagInt = unionTag;
        return receivedElement -> collector.add(new RawUnionValue(tagInt, receivedElement));
      } else {
        throw new IllegalStateException(
            String.format(Locale.ENGLISH, "Unknown PCollectionId %s", pCollectionId));
      }
    }
  }
}
