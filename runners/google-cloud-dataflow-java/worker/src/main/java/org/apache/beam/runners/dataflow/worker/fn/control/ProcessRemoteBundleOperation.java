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
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import java.io.IOException;
import java.util.*;

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
// TODO(srohde): Clean up logging
// TODO(srohde): Add unit tests
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

  // TODO(srohde): Do we need all these maps? Can we clean this up?
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToSpecMap;
  private final Map<String, Coder<BoundedWindow>> timerWindowCodersMap;
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap;
  private final Map<String, Object> timerIdToKey;
  private ExecutableStage executableStage;

  public ProcessRemoteBundleOperation(
      ExecutableStage executableStage,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext,
      StageBundleFactory stageBundleFactory,
    Map<String, OutputReceiver> outputReceiverMap) {
    super(receivers, operationContext);
    LOG.error("Creating ProcessRemoteBundleOperation for stage {}", operationContext.nameContext());
    this.stageBundleFactory = stageBundleFactory;
    this.stateRequestHandler = StateRequestHandler.unsupported();
    this.progressHandler = BundleProgressHandler.ignored();
    this.executionContext = executionContext;
    this.timerOutputIdToSpecMap = new HashMap<>();
    this.timerWindowCodersMap = new HashMap<>();
    this.executableStage = executableStage;
    this.timerIdToKey = new HashMap<>();
    this.outputReceiverMap = outputReceiverMap;
    timerIdToTimerSpecMap = new HashMap<>();

    for (Map<String, ProcessBundleDescriptors.TimerSpec> transformTimerMap :
        stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().values()) {
      for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
        timerIdToTimerSpecMap.put(timerSpec.timerId(), timerSpec);
        LOG.error("got timer spec {}", timerSpec);
      }
    }

    for (Map<String, ProcessBundleDescriptors.TimerSpec> transformTimerMap :
        stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().values()) {
      for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
        timerOutputIdToSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
      }
    }

    LOG.error("All OutputTargetCoders {}", stageBundleFactory.getProcessBundleDescriptor().getOutputTargetCoders());

    for (RunnerApi.PTransform pTransform :
        stageBundleFactory
            .getProcessBundleDescriptor()
            .getProcessBundleDescriptor()
            .getTransformsMap()
            .values()) {
      for (String timerId : timerIdToTimerSpecMap.keySet()) {

        if (!pTransform.getInputsMap().containsKey(timerId)) {
          continue;
        }

        String timerPCollectionId = pTransform.getInputsMap().get(timerId);
        RunnerApi.PCollection timerPCollection =
            stageBundleFactory
                .getProcessBundleDescriptor()
                .getProcessBundleDescriptor()
                .getPcollectionsMap()
                .get(timerPCollectionId);

        String windowingStrategyId = timerPCollection.getWindowingStrategyId();
        RunnerApi.WindowingStrategy windowingStrategy =
            stageBundleFactory
                .getProcessBundleDescriptor()
                .getProcessBundleDescriptor()
                .getWindowingStrategiesMap()
                .get(windowingStrategyId);

        String windowingCoderId = windowingStrategy.getWindowCoderId();
        RunnerApi.Coder windowingCoder =
            stageBundleFactory
                .getProcessBundleDescriptor()
                .getProcessBundleDescriptor()
                .getCodersMap()
                .get(windowingCoderId);

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

    LOG.error("timerIdToTimerSpecMap = {}", timerIdToTimerSpecMap.toString());
    LOG.error("timerOutputIdToSpecMap = {}", this.timerOutputIdToSpecMap.toString());
    LOG.error("timerWindowCodersMap = {}", this.timerWindowCodersMap.toString());
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
    LOG.error(String.format("Sending element: %s", inputElement));
    String mainInputPCollectionId = executableStage.getInputPCollection().getId();
    FnDataReceiver<WindowedValue<?>> mainInputReceiver = remoteBundle.getInputReceivers().get(mainInputPCollectionId);

    LOG.error("All InputReceivers {}", remoteBundle.getInputReceivers());
    LOG.error("Found main receiver {} for mainInput {}", mainInputReceiver, mainInputPCollectionId);

    // TODO(srohde): Is this always true? Do we always send the input element to the main input receiver?
    try (Closeable scope = context.enterProcess()) {
      mainInputReceiver.accept((WindowedValue<InputT>)inputElement);
    } catch (Exception e) {
      LOG.error("Could not process element {} to receiver {} for pcollection {} with error {}",
          inputElement, mainInputReceiver, mainInputPCollectionId, e.getMessage());
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

      // TODO(srohde): do we have to put this in the "start" method as well?
      try (RemoteBundle bundle = stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler)) {

        // TODO(srohde): Why do we need to namespace this to "user"?
        DataflowExecutionContext.DataflowStepContext stepContext =
            executionContext.getStepContext((DataflowOperationContext) this.context).namespacedToUser();

        // TODO(srohde): investigate if this is the correct window
        TimerInternals.TimerData timerData =
            stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
        while (timerData != null) {
          LOG.error("Found fired timer in start {}", timerData);

          // TODO(srohde): get the correct payload and payload coder
          StateNamespaces.WindowNamespace windowNamespace = (StateNamespaces.WindowNamespace)timerData.getNamespace();
          BoundedWindow window = windowNamespace.getWindow();

          WindowedValue<KV<Object, Timer>> timerValue =
              WindowedValue.of(
                  KV.of(timerIdToKey.get(timerData.getTimerId()), Timer.of(timerData.getTimestamp(), new byte[0])),
                  timerData.getTimestamp(),
                  Collections.singleton(window),
                  PaneInfo.NO_FIRING);

          String mainInputId = timerIdToTimerSpecMap.get(timerData.getTimerId()).inputCollectionId();

          bundle
              .getInputReceivers()
              .get(mainInputId)
              .accept(timerValue);

          // TODO(srohde): investigate if this is the correct window
          timerData = stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
        }
      }
    }
  }

  private void receive(String pCollectionId, Object receivedElement) throws Exception {
    LOG.error("Received element {} for pcollection {}", receivedElement, pCollectionId);
    LOG.error("All OutputReceivers: {}", new ArrayList<>(Arrays.asList(receivers)));

    // TODO(srohde): move this out into its own receiver class
    if (timerOutputIdToSpecMap.containsKey(pCollectionId)) {
      try {
        WindowedValue<KV<Object, Timer>> windowedValue = (WindowedValue<KV<Object, Timer>>) receivedElement;

        ProcessBundleDescriptors.TimerSpec timerSpec = timerOutputIdToSpecMap.get(pCollectionId);

        Timer timer = windowedValue.getValue().getValue();
        LOG.error("Received timer element {} for pcollection {}", timer, pCollectionId);
        // TODO(srohde): Which window should we set timers in?
        for (BoundedWindow window : windowedValue.getWindows()) {
          Coder<BoundedWindow> windowCoder = timerWindowCodersMap.get(timerSpec.timerId());
          StateNamespace namespace = StateNamespaces.window(windowCoder, window);

          TimeDomain timeDomain = timerSpec.getTimerSpec().getTimeDomain();
          String timerId = timerSpec.timerId();

          DataflowExecutionContext.DataflowStepContext stepContext = executionContext.getStepContext((DataflowOperationContext) this.context);

          TimerInternals timerData = stepContext.namespacedToUser().timerInternals();
          timerData.setTimer(namespace, timerId, timer.getTimestamp(), timeDomain);

          timerIdToKey.put(timerId, windowedValue.getValue().getKey());
        }

        /*
        StateNamespace namespace = StateNamespaces.global();

        String timerId = timerSpec.timerId();
        TimeDomain timeDomain = timerSpec.getTimerSpec().getTimeDomain();
        DataflowExecutionContext.DataflowStepContext stepContext = executionContext.getStepContext((DataflowOperationContext) this.context);
        TimerInternals timerData = stepContext.namespacedToUser().timerInternals();
        timerData.setTimer(namespace, timerId, timer.getTimestamp(), timeDomain);*/

        //timerIdToKey.put(timerId, windowedValue.getValue().getKey());
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    } else {
      outputReceiverMap.get(pCollectionId).process((WindowedValue<?>) receivedElement);
    }
  }

  private static class TimerReceiverFactory implements OutputReceiverFactory {

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
      return null;
    }
  }
}
