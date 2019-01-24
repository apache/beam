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
package org.apache.beam.runners.dataflow.worker.fn.control;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.control.*;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link org.apache.beam.runners.dataflow.worker.util.common.worker.Operation} is responsible
 * for communicating with the SDK harness and asking it to process a bundle of work. This operation
 * requests a {@link org.apache.beam.runners.fnexecution.control.RemoteBundle}, sends elements to
 * SDK and receive processed results from SDK, passing these elements downstream.
 */
public class ProcessRemoteBundleOperation<InputT> extends ReceivingOperation {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessRemoteBundleOperation.class);
  private final StageBundleFactory stageBundleFactory;
  private static final OutputReceiver[] EMPTY_RECEIVER_ARRAY = new OutputReceiver[0];
  private final Map<String, OutputReceiver> outputReceiverMap;
  private final OutputReceiverFactory receiverFactory =
      new OutputReceiverFactory() {
        @Override
        public FnDataReceiver<?> create(String pCollectionId) {
          return receivedElement -> receive(pCollectionId, receivedElement);
        }
      };
  private final StateRequestHandler stateRequestHandler;
  private final BundleProgressHandler progressHandler;
  private RemoteBundle remoteBundle;
  private final DataflowExecutionContext<?> executionContext;
  private ExecutableStage executableStage;

  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToSpecMap;
  private final Map<String, Coder<BoundedWindow>> timerWindowCodersMap;
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap;
  private final Map<String, Object> timerIdToKey;
  private final Map<String, Object> timerIdToPayload;

  public ProcessRemoteBundleOperation(
      ExecutableStage executableStage,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext,
      StageBundleFactory stageBundleFactory,
      Map<String, OutputReceiver> outputReceiverMap) {
    super(EMPTY_RECEIVER_ARRAY, operationContext);

    this.stageBundleFactory = stageBundleFactory;
    this.stateRequestHandler = StateRequestHandler.unsupported();
    this.progressHandler = BundleProgressHandler.ignored();
    this.executionContext = executionContext;
    this.executableStage = executableStage;
    this.outputReceiverMap = outputReceiverMap;

    this.timerOutputIdToSpecMap = new HashMap<>();
    this.timerIdToKey = new HashMap<>();
    this.timerIdToPayload = new HashMap<>();
    this.timerIdToTimerSpecMap = new HashMap<>();

    ProcessBundleDescriptors.ExecutableProcessBundleDescriptor executableProcessBundleDescriptor =
        stageBundleFactory.getProcessBundleDescriptor();

    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        executableProcessBundleDescriptor.getProcessBundleDescriptor();

    // Create and cache lookups so that we don't have to dive into the ProcessBundleDescriptor
    // later.
    fillTimerSpecMaps(
        executableProcessBundleDescriptor, this.timerIdToTimerSpecMap, this.timerOutputIdToSpecMap);
    this.timerWindowCodersMap =
        fillTimerWindowCodersMap(
            processBundleDescriptor, this.timerIdToTimerSpecMap, this.executableStage);
  }

  // Fills the given maps from timer the TimerSpec definitions in the process bundle descriptor.
  private void fillTimerSpecMaps(
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor,
      Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap,
      Map<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToSpecMap) {
    processBundleDescriptor
        .getTimerSpecs()
        .values()
        .forEach(
            transformTimerMap -> {
              for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
                timerIdToTimerSpecMap.put(timerSpec.timerId(), timerSpec);
                timerOutputIdToSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
              }
            });
  }

  // Retrieves all window coders for all TimerSpecs.
  private Map<String, Coder<BoundedWindow>> fillTimerWindowCodersMap(
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor,
      Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap,
      ExecutableStage executableStage) {
    Map<String, Coder<BoundedWindow>> timerWindowCodersMap = new HashMap<>();
    for (RunnerApi.PTransform pTransform : processBundleDescriptor.getTransformsMap().values()) {
      for (String timerId : timerIdToTimerSpecMap.keySet()) {
        if (!pTransform.getInputsMap().containsKey(timerId)) {
          continue;
        }

        RunnerApi.Coder windowingCoder =
            getTimerWindowingCoder(pTransform, timerId, processBundleDescriptor);
        RehydratedComponents components =
            RehydratedComponents.forComponents(executableStage.getComponents());
        try {
          timerWindowCodersMap.put(
              timerId,
              (Coder<BoundedWindow>) CoderTranslation.fromProto(windowingCoder, components));
        } catch (IOException e) {
          LOG.error(
              "Failed to retrieve window coder for timerId {}. Failed with error: {}",
              timerId,
              e.getMessage());
        }
      }
    }
    return timerWindowCodersMap;
  }

  // Retrieves the window coder for the given timer.
  private RunnerApi.Coder getTimerWindowingCoder(
      RunnerApi.PTransform pTransform,
      String timerId,
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor) {
    String timerPCollectionId = pTransform.getInputsMap().get(timerId);
    RunnerApi.PCollection timerPCollection =
        processBundleDescriptor.getPcollectionsMap().get(timerPCollectionId);

    String windowingStrategyId = timerPCollection.getWindowingStrategyId();
    RunnerApi.WindowingStrategy windowingStrategy =
        processBundleDescriptor.getWindowingStrategiesMap().get(windowingStrategyId);

    String windowingCoderId = windowingStrategy.getWindowCoderId();
    return processBundleDescriptor.getCodersMap().get(windowingCoderId);
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();
      try {
        remoteBundle =
            stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
      } catch (Exception e) {
        throw new RuntimeException("Failed to start remote bundle", e);
      }
    }
  }

  @Override
  public void process(Object inputElement) throws Exception {
    LOG.error("Sending element: {}", inputElement);
    String mainInputPCollectionId = executableStage.getInputPCollection().getId();
    FnDataReceiver<WindowedValue<?>> mainInputReceiver =
        remoteBundle.getInputReceivers().get(mainInputPCollectionId);

    try (Closeable scope = context.enterProcess()) {
      mainInputReceiver.accept((WindowedValue<InputT>) inputElement);
    } catch (Exception e) {
      LOG.error(
          "Could not process element {} to receiver {} for pcollection {} with error {}",
          inputElement,
          mainInputReceiver,
          mainInputPCollectionId,
          e.getMessage());
    }
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      try {
        // close blocks until all results are received
        remoteBundle.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish remote bundle", e);
      }

      try {
        // In Batch, the input watermark advances from -inf to +inf once all elements are processed.
        // For timers, this means we only fire them at the end of the bundle.
        fireTimers();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish remote bundle", e);
      }
    }
  }

  // Recursively fires all scheduled timers from the SDK Harness.
  private void fireTimers() throws Exception {
    DataflowExecutionContext.DataflowStepContext stepContext =
        executionContext.getStepContext((DataflowOperationContext) this.context).namespacedToUser();

    TimerInternals.TimerData timerData = stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
    while (timerData != null) {
      StateNamespaces.WindowNamespace windowNamespace =
          (StateNamespaces.WindowNamespace) timerData.getNamespace();
      BoundedWindow window = windowNamespace.getWindow();

      WindowedValue<KV<Object, Timer>> timerValue =
          WindowedValue.of(
              KV.of(
                  timerIdToKey.get(timerData.getTimerId()),
                  Timer.of(timerData.getTimestamp(), timerIdToPayload.get(timerData.getTimerId()))),
              timerData.getTimestamp(),
              Collections.singleton(window),
              PaneInfo.NO_FIRING);

      String timerInputPCollection =
          timerIdToTimerSpecMap.get(timerData.getTimerId()).inputCollectionId();

      // TODO(BEAM-6274): check this for performance considerations.
      // Timers are allowed to A) fire other timers, and B) fire themselves. To implement this,
      // we send the timer to the SDK Harness, wait for any elements, then check if any additional
      // timers were fired.
      RemoteBundle timerRemoteBundle =
          stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
      timerRemoteBundle.getInputReceivers().get(timerInputPCollection).accept(timerValue);
      timerRemoteBundle.close();

      timerData = stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
    }
  }

  private void receive(String pCollectionId, Object receivedElement) throws Exception {
    LOG.error("Received element {} for pcollection {}", receivedElement, pCollectionId);
    // TODO(BEAM-6274): move this out into its own receiver class
    if (timerOutputIdToSpecMap.containsKey(pCollectionId)) {
      WindowedValue<KV<Object, Timer>> windowedValue =
          (WindowedValue<KV<Object, Timer>>) receivedElement;
      ProcessBundleDescriptors.TimerSpec timerSpec = timerOutputIdToSpecMap.get(pCollectionId);
      Timer timer = windowedValue.getValue().getValue();

      for (BoundedWindow window : windowedValue.getWindows()) {
        Coder<BoundedWindow> windowCoder = timerWindowCodersMap.get(timerSpec.timerId());
        StateNamespace namespace = StateNamespaces.window(windowCoder, window);

        TimeDomain timeDomain = timerSpec.getTimerSpec().getTimeDomain();
        String timerId = timerSpec.timerId();

        DataflowExecutionContext.DataflowStepContext stepContext =
            executionContext.getStepContext((DataflowOperationContext) this.context);

        TimerInternals timerInternals = stepContext.namespacedToUser().timerInternals();
        timerInternals.setTimer(namespace, timerId, timer.getTimestamp(), timeDomain);

        timerIdToKey.put(timerId, windowedValue.getValue().getKey());
        timerIdToPayload.put(timerId, timer.getPayload());
      }
    } else {
      outputReceiverMap.get(pCollectionId).process((WindowedValue<?>) receivedElement);
    }
  }
}
