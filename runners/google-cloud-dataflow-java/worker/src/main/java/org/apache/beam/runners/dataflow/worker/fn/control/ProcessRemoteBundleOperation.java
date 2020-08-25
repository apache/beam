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
import java.util.EnumMap;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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
  private ExecutableStage executableStage;

  public ProcessRemoteBundleOperation(
      ExecutableStage executableStage,
      DataflowOperationContext operationContext,
      StageBundleFactory stageBundleFactory,
      Map<String, OutputReceiver> outputReceiverMap,
      Map<String, SideInputReader> ptransformIdToSideInputReader,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
          sideInputIdToPCollectionViewMap) {
    super(EMPTY_RECEIVER_ARRAY, operationContext);

    this.stageBundleFactory = stageBundleFactory;
    this.progressHandler = BundleProgressHandler.ignored();
    this.executableStage = executableStage;
    this.outputReceiverMap = outputReceiverMap;

    StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
        DataflowSideInputHandlerFactory.of(
            ptransformIdToSideInputReader, sideInputIdToPCollectionViewMap);

    stateRequestHandler = getStateRequestHandler(executableStage, sideInputHandlerFactory);
  }

  private StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage,
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory) {
    final StateRequestHandler sideInputHandler;

    try {
      sideInputHandler =
          StateRequestHandlers.forSideInputHandlerFactory(
              ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup state handler", e);
    }

    EnumMap<BeamFnApi.StateKey.TypeCase, StateRequestHandler> handlerMap =
        new EnumMap<BeamFnApi.StateKey.TypeCase, StateRequestHandler>(
            BeamFnApi.StateKey.TypeCase.class);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.ITERABLE_SIDE_INPUT, sideInputHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT, sideInputHandler);
    handlerMap.put(BeamFnApi.StateKey.TypeCase.MULTIMAP_KEYS_SIDE_INPUT, sideInputHandler);

    return StateRequestHandlers.delegateBasedUponType(handlerMap);
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
    LOG.debug("Sending element: {}", inputElement);
    FnDataReceiver<WindowedValue<?>> mainInputReceiver =
        Iterables.getOnlyElement(remoteBundle.getInputReceivers().values());

    try (Closeable scope = context.enterProcess()) {
      mainInputReceiver.accept((WindowedValue<InputT>) inputElement);
    } catch (Exception e) {
      String err =
          String.format(
              "Could not process element %s to receiver %s for PTransform %s with error %s",
              inputElement,
              mainInputReceiver,
              Iterables.getOnlyElement(remoteBundle.getInputReceivers().keySet()),
              e);
      LOG.error(err);
      throw new RuntimeException(err, e);
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
    }
  }

  private void receive(String pCollectionId, Object receivedElement) throws Exception {
    LOG.debug("Received element {} for pcollection {}", receivedElement, pCollectionId);
    outputReceiverMap.get(pCollectionId).process((WindowedValue<?>) receivedElement);
  }
}
