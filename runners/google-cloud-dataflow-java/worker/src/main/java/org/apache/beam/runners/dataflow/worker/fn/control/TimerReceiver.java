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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerReceiver {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessRemoteBundleOperation.class);
  private final StageBundleFactory stageBundleFactory;
  private final DataflowExecutionContext.DataflowStepContext stepContext;

  private final Map<String, Coder<BoundedWindow>> timerWindowCodersMap;
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToSpecMap;
  private final Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap;
  private final Map<String, Object> timerIdToKey;
  private final Map<String, Object> timerIdToPayload;

  private final StateRequestHandler stateRequestHandler = StateRequestHandler.unsupported();
  private final BundleProgressHandler progressHandler = BundleProgressHandler.ignored();
  private OutputReceiverFactory receiverFactory =
      new OutputReceiverFactory() {
        @Override
        public FnDataReceiver<?> create(String pCollectionId) {
          return receivedElement -> receive(pCollectionId, receivedElement);
        }
      };

  public TimerReceiver(
      RunnerApi.Components components,
      DataflowExecutionContext.DataflowStepContext stepContext,
      StageBundleFactory stageBundleFactory) {
    this.stageBundleFactory = stageBundleFactory;
    this.stepContext = stepContext;

    this.timerIdToKey = new HashMap<>();
    this.timerIdToPayload = new HashMap<>();

    ProcessBundleDescriptors.ExecutableProcessBundleDescriptor executableProcessBundleDescriptor =
        stageBundleFactory.getProcessBundleDescriptor();

    ProcessBundleDescriptor processBundleDescriptor =
        executableProcessBundleDescriptor.getProcessBundleDescriptor();

    // Create and cache lookups so that we don't have to dive into the ProcessBundleDescriptor
    // later.
    this.timerIdToTimerSpecMap = createTimerIdToSpecMap(executableProcessBundleDescriptor);
    this.timerOutputIdToSpecMap = createTimerOutputIdToSpecMap(executableProcessBundleDescriptor);
    this.timerWindowCodersMap =
        createTimerWindowCodersMap(processBundleDescriptor, this.timerIdToTimerSpecMap, components);
  }

  // Fires all timers until the SDK stops scheduling timers.
  public void finish() {
    try {
      // In Batch, the input watermark advances from -inf to +inf once all elements are processed.
      // For timers, this means we only fire them at the end of the bundle.
      fireTimers();
    } catch (Exception e) {
      throw new RuntimeException("Failed to finish firing all timers", e);
    }
  }

  // Collects timer elements and sets the timer to fire later. Returns true if receivedElement is a
  // timer element and was handled. Returns false, if receivedElement is not a timer element.
  public boolean receive(String pCollectionId, Object receivedElement) {
    LOG.debug("Received element {} for pcollection {}", receivedElement, pCollectionId);
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

        TimerInternals timerInternals = stepContext.namespacedToUser().timerInternals();
        timerInternals.setTimer(
            namespace, timerId, "", timer.getTimestamp(), windowedValue.getTimestamp(), timeDomain);

        timerIdToKey.put(timerId, windowedValue.getValue().getKey());
        timerIdToPayload.put(timerId, timer.getPayload());
      }
      return true;
    }
    return false;
  }

  // Continuously fires scheduled timers from the SDK Harness until the SDK Harness stops scheduling
  // timers.
  private void fireTimers() throws Exception {
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
              timerData.getOutputTimestamp(),
              Collections.singleton(window),
              PaneInfo.NO_FIRING);

      String timerInputPCollection =
          timerIdToTimerSpecMap.get(timerData.getTimerId()).inputCollectionId();

      fireTimer(timerInputPCollection, timerValue);

      timerData = stepContext.getNextFiredTimer(GlobalWindow.Coder.INSTANCE);
    }
  }

  @VisibleForTesting
  protected void fireTimer(String timerInputPCollection, WindowedValue<KV<Object, Timer>> timer)
      throws Exception {
    // TODO(BEAM-6274): check this for performance considerations.
    // Timers are allowed to A) fire other timers, and B) fire themselves. To implement this,
    // we send the timer to the SDK Harness, wait for any elements, then check if any additional
    // timers were fired.
    RemoteBundle timerRemoteBundle =
        stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
    timerRemoteBundle.getInputReceivers().get(timerInputPCollection).accept(timer);
    timerRemoteBundle.close();
  }

  // Fills the given maps from timer the TimerSpec definitions in the process bundle descriptor.
  private static Map<String, ProcessBundleDescriptors.TimerSpec> createTimerOutputIdToSpecMap(
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor) {
    Map<String, ProcessBundleDescriptors.TimerSpec> timerOutputIdToTimerSpecMap = new HashMap<>();
    processBundleDescriptor
        .getTimerSpecs()
        .values()
        .forEach(
            transformTimerMap -> {
              for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
                timerOutputIdToTimerSpecMap.put(timerSpec.outputCollectionId(), timerSpec);
              }
            });
    return timerOutputIdToTimerSpecMap;
  }

  // Fills the given maps from timer the TimerSpec definitions in the process bundle descriptor.
  private static Map<String, ProcessBundleDescriptors.TimerSpec> createTimerIdToSpecMap(
      ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor) {
    Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap = new HashMap<>();
    processBundleDescriptor
        .getTimerSpecs()
        .values()
        .forEach(
            transformTimerMap -> {
              for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerMap.values()) {
                timerIdToTimerSpecMap.put(timerSpec.timerId(), timerSpec);
              }
            });
    return timerIdToTimerSpecMap;
  }

  // Retrieves all window coders for all TimerSpecs.
  private static Map<String, Coder<BoundedWindow>> createTimerWindowCodersMap(
      ProcessBundleDescriptor processBundleDescriptor,
      Map<String, ProcessBundleDescriptors.TimerSpec> timerIdToTimerSpecMap,
      RunnerApi.Components components) {
    Map<String, Coder<BoundedWindow>> timerWindowCodersMap = new HashMap<>();

    RehydratedComponents rehydratedComponents = RehydratedComponents.forComponents(components);

    // Find the matching PTransform per timer. Then, rehydrate the PTransform window coder and
    // associate it with its timer.
    for (RunnerApi.PTransform pTransform : processBundleDescriptor.getTransformsMap().values()) {
      for (String timerId : timerIdToTimerSpecMap.keySet()) {
        if (!pTransform.getInputsMap().containsKey(timerId)) {
          continue;
        }

        RunnerApi.Coder windowingCoder =
            getTimerWindowingCoder(pTransform, timerId, processBundleDescriptor);
        try {
          timerWindowCodersMap.put(
              timerId,
              (Coder<BoundedWindow>)
                  CoderTranslation.fromProto(windowingCoder, rehydratedComponents));
        } catch (IOException e) {
          String err =
              String.format(
                  "Failed to retrieve window coder for timerId %s. Failed with error: %s",
                  timerId, e.getMessage());
          LOG.error(err);
          throw new RuntimeException(err, e);
        }
      }
    }
    return timerWindowCodersMap;
  }

  // Retrieves the window coder for the given timer.
  private static RunnerApi.Coder getTimerWindowingCoder(
      RunnerApi.PTransform pTransform,
      String timerId,
      ProcessBundleDescriptor processBundleDescriptor) {
    String timerPCollectionId = pTransform.getInputsMap().get(timerId);
    RunnerApi.PCollection timerPCollection =
        processBundleDescriptor.getPcollectionsMap().get(timerPCollectionId);

    String windowingStrategyId = timerPCollection.getWindowingStrategyId();
    RunnerApi.WindowingStrategy windowingStrategy =
        processBundleDescriptor.getWindowingStrategiesMap().get(windowingStrategyId);

    String windowingCoderId = windowingStrategy.getWindowCoderId();
    return processBundleDescriptor.getCodersMap().get(windowingCoderId);
  }
}
