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
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
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
  private RemoteBundle timerRemoteBundle;
  private final DataflowExecutionContext<?> executionContext;
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToSpecMap;
  private final Map<String, Coder<BoundedWindow>> timerWindowCodersMap;
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap;
  private final Map<String, Object> timerIdToKey;
  private final Map<String, Object> timerIdToPayload;
  private ExecutableStage executableStage;
  private String loggingName;

  public ProcessRemoteBundleOperation(
      ExecutableStage executableStage,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext,
      StageBundleFactory stageBundleFactory,
      Map<String, OutputReceiver> outputReceiverMap) {
    super(EMPTY_RECEIVER_ARRAY, operationContext);

    // TODO: Remove this
    loggingName = executionContext.getStepContext(operationContext).getNameContext().toString();

    this.stageBundleFactory = stageBundleFactory;
    this.stateRequestHandler = StateRequestHandler.unsupported();
    this.progressHandler = BundleProgressHandler.ignored();
    this.executionContext = executionContext;
    this.timerOutputIdToSpecMap = new HashMap<>();
    this.timerWindowCodersMap = new HashMap<>();
    this.executableStage = executableStage;
    this.timerIdToKey = new HashMap<>();
    this.timerIdToPayload = new HashMap<>();
    this.outputReceiverMap = outputReceiverMap;
    this.timerIdToTimerSpecMap = new HashMap<>();

    ProcessBundleDescriptors.ExecutableProcessBundleDescriptor executableProcessBundleDescriptor =
        stageBundleFactory.getProcessBundleDescriptor();

    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        executableProcessBundleDescriptor.getProcessBundleDescriptor();

    executableProcessBundleDescriptor
        .getTimerSpecs()
        .values()
        .forEach(
            transformTimerMap -> {
              for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
                timerIdToTimerSpecMap.put(timerSpec.timerId(), timerSpec);
                timerOutputIdToSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
              }
            });

    for (RunnerApi.PTransform pTransform : processBundleDescriptor.getTransformsMap().values()) {
      for (String timerId : timerIdToTimerSpecMap.keySet()) {
        if (!pTransform.getInputsMap().containsKey(timerId)) {
          continue;
        }

        String timerPCollectionId = pTransform.getInputsMap().get(timerId);
        RunnerApi.PCollection timerPCollection =
            processBundleDescriptor.getPcollectionsMap().get(timerPCollectionId);

        String windowingStrategyId = timerPCollection.getWindowingStrategyId();
        RunnerApi.WindowingStrategy windowingStrategy =
            processBundleDescriptor.getWindowingStrategiesMap().get(windowingStrategyId);

        String windowingCoderId = windowingStrategy.getWindowCoderId();
        RunnerApi.Coder windowingCoder =
            processBundleDescriptor.getCodersMap().get(windowingCoderId);

        RehydratedComponents components =
            RehydratedComponents.forComponents(executableStage.getComponents());
        try {
          timerWindowCodersMap.put(
              timerId,
              (Coder<BoundedWindow>) CoderTranslation.fromProto(windowingCoder, components));
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
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

      try {
        timerRemoteBundle =
            stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
      } catch (Exception e) {
        throw new RuntimeException("Failed to start timer remote bundle", e);
      }
    }
  }

  @Override
  public void process(Object inputElement) throws Exception {
    LOG.error("[{}] Sending element: {}", loggingName, inputElement);
    String mainInputPCollectionId = executableStage.getInputPCollection().getId();
    FnDataReceiver<WindowedValue<?>> mainInputReceiver =
        remoteBundle.getInputReceivers().get(mainInputPCollectionId);

    // TODO(BEAM-6274): Is this always true? Do we always send the input element to the main input receiver?
    try (Closeable scope = context.enterProcess()) {
      mainInputReceiver.accept((WindowedValue<?>) inputElement);
    } catch (Exception e) {
      LOG.error(
          "[{}] Could not process element {} to receiver {} for pcollection {} with error {}",
          loggingName,
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
        // close blocks until all results are received
        timerRemoteBundle.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish remote bundle", e);
      }
/*
      // TODO(BEAM-6274): do we have to put this in the "start" method as well?
      // The ProcessRemoteBundleOperation has to wait until it has received all elements from the
      // SDK in case the SDK generated a timer.
      try (RemoteBundle bundle =
          stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler)) {

        // TODO(BEAM-6274): Why do we need to namespace this to "user"?
        DataflowExecutionContext.DataflowStepContext stepContext =
            executionContext
                .getStepContext((DataflowOperationContext) this.context)
                .namespacedToUser();

        // TODO(BEAM-6274): investigate if this is the correct window
        TimerInternals.TimerData timerData =
            stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
        while (timerData != null) {
          LOG.debug("Found fired timer in start {}", timerData);

          // TODO(BEAM-6274): get the correct payload and payload coder
          StateNamespaces.WindowNamespace windowNamespace =
              (StateNamespaces.WindowNamespace) timerData.getNamespace();
          BoundedWindow window = windowNamespace.getWindow();

          WindowedValue<KV<Object, Timer>> timerValue =
              WindowedValue.of(
                  KV.of(
                      timerIdToKey.get(timerData.getTimerId()),
                      Timer.of(timerData.getTimestamp(), new byte[0])),
                  timerData.getTimestamp(),
                  Collections.singleton(window),
                  PaneInfo.NO_FIRING);

          String mainInputId =
              timerIdToTimerSpecMap.get(timerData.getTimerId()).inputCollectionId();

          bundle.getInputReceivers().get(mainInputId).accept(timerValue);

          // TODO(BEAM-6274): investigate if this is the correct window
          timerData = stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
        }
      }*/
    }
  }

  private void fireTimers() throws Exception {
    // TODO(BEAM-6274): Why do we need to namespace this to "user"?
    DataflowExecutionContext.DataflowStepContext stepContext =
        executionContext
            .getStepContext((DataflowOperationContext) this.context)
            .namespacedToUser();

    // TODO(BEAM-6274): investigate if this is the correct window
    TimerInternals.TimerData timerData =
        stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
    while (timerData != null) {
      LOG.error("[{}] Found fired timer in 'receive' {}", loggingName, timerData);

      // TODO(BEAM-6274): get the correct payload and payload coder
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

      String mainInputId =
          timerIdToTimerSpecMap.get(timerData.getTimerId()).inputCollectionId();

      timerRemoteBundle.getInputReceivers().get(mainInputId).accept(timerValue);

      // TODO(BEAM-6274): investigate if this is the correct window
      timerData = stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
    }
  }

  private void receive(String pCollectionId, Object receivedElement) throws Exception {
    LOG.error("[{}] Received element {} for pcollection {}", loggingName, receivedElement, pCollectionId);
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

        TimerInternals timerData = stepContext.namespacedToUser().timerInternals();
        timerData.setTimer(namespace, timerId, timer.getTimestamp(), timeDomain);

        timerIdToKey.put(timerId, windowedValue.getValue().getKey());
        timerIdToPayload.put(timerId, timer.getPayload());

        fireTimers();
      }
    } else {
      outputReceiverMap.get(pCollectionId).process((WindowedValue<?>) receivedElement);
    }
  }
}
