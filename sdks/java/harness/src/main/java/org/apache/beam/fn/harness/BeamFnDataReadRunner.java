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
package org.apache.beam.fn.harness;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.HandlesSplits.SplitResult;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest.DesiredSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers as a consumer for data over the Beam Fn API. Multiplexes any received data to all
 * receivers in a specified output map.
 *
 * <p>Can be re-used serially across {@link BeamFnApi.ProcessBundleRequest}s. For each request, call
 * {@link #registerInputLocation()} to start and call {@link #blockTillReadFinishes()} to finish.
 */
public class BeamFnDataReadRunner<OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataReadRunner.class);

  /** A registrar which provides a factory to handle reading from the Fn Api Data Plane. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(RemoteGrpcPortRead.URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataReadRunner}s. */
  static class Factory<OutputT> implements PTransformRunnerFactory<BeamFnDataReadRunner<OutputT>> {

    @Override
    public BeamFnDataReadRunner<OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        Consumer<ThrowingRunnable> tearDownFunctions,
        BundleSplitListener splitListener,
        BundleFinalizer bundleFinalizer)
        throws IOException {

      FnDataReceiver<WindowedValue<OutputT>> consumer =
          (FnDataReceiver<WindowedValue<OutputT>>)
              (FnDataReceiver)
                  pCollectionConsumerRegistry.getMultiplexingConsumer(
                      getOnlyElement(pTransform.getOutputsMap().values()));

      BeamFnDataReadRunner<OutputT> runner =
          new BeamFnDataReadRunner<>(
              pTransformId,
              pTransform,
              processBundleInstructionId,
              coders,
              beamFnDataClient,
              consumer);
      startFunctionRegistry.register(pTransformId, runner::registerInputLocation);
      finishFunctionRegistry.register(pTransformId, runner::blockTillReadFinishes);
      return runner;
    }
  }

  private final String pTransformId;
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final FnDataReceiver<WindowedValue<OutputT>> consumer;
  private final Supplier<String> processBundleInstructionIdSupplier;
  private final BeamFnDataClient beamFnDataClient;
  private final Coder<WindowedValue<OutputT>> coder;

  private final Object splittingLock = new Object();
  // 0-based index of the current element being processed
  private long index = -1;
  // 0-based index of the first element to not process, aka the first element of the residual
  private long stopIndex = Long.MAX_VALUE;
  private InboundDataClient readFuture;

  BeamFnDataReadRunner(
      String pTransformId,
      RunnerApi.PTransform grpcReadNode,
      Supplier<String> processBundleInstructionIdSupplier,
      Map<String, RunnerApi.Coder> coders,
      BeamFnDataClient beamFnDataClient,
      FnDataReceiver<WindowedValue<OutputT>> consumer)
      throws IOException {
    this.pTransformId = pTransformId;
    RemoteGrpcPort port = RemoteGrpcPortRead.fromPTransform(grpcReadNode).getPort();
    this.apiServiceDescriptor = port.getApiServiceDescriptor();
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.beamFnDataClient = beamFnDataClient;
    this.consumer = consumer;

    RehydratedComponents components =
        RehydratedComponents.forComponents(Components.newBuilder().putAllCoders(coders).build());
    this.coder =
        (Coder<WindowedValue<OutputT>>)
            CoderTranslation.fromProto(coders.get(port.getCoderId()), components);
  }

  public void registerInputLocation() {
    this.readFuture =
        beamFnDataClient.receive(
            apiServiceDescriptor,
            LogicalEndpoint.of(processBundleInstructionIdSupplier.get(), pTransformId),
            coder,
            this::forwardElementToConsumer);
  }

  public void forwardElementToConsumer(WindowedValue<OutputT> element) throws Exception {
    synchronized (splittingLock) {
      if (index == stopIndex - 1) {
        return;
      }
      index += 1;
    }
    consumer.accept(element);
  }

  public void split(
      ProcessBundleSplitRequest request, ProcessBundleSplitResponse.Builder response) {
    DesiredSplit desiredSplit = request.getDesiredSplitsMap().get(pTransformId);
    if (desiredSplit == null) {
      return;
    }

    long totalBufferSize = desiredSplit.getEstimatedInputElements();

    HandlesSplits splittingConsumer = null;
    if (consumer instanceof HandlesSplits) {
      splittingConsumer = ((HandlesSplits) consumer);
    }

    synchronized (splittingLock) {
      // Since we hold the splittingLock, we guarantee that we will not pass the next element
      // to the downstream consumer. We still have a race where the downstream consumer may
      // have yet to see the element or has completed processing the element by the time
      // we ask it to split (even after we have asked for its progress).

      // If the split request we received was delayed and is less then the known number of elements
      // then use "index + 1" as the total size. Similarly, if we have already split and the
      // split request is bounded incorrectly, use the stop index as the upper bound.
      if (totalBufferSize < index + 1) {
        totalBufferSize = index + 1;
      } else if (totalBufferSize > stopIndex) {
        totalBufferSize = stopIndex;
      }

      // In the case where we have yet to process an element, set the current element progress to 1.
      double currentElementProgress = 1;

      // If we have started processing at least one element, attempt to get the downstream
      // progress defaulting to 0.5 if no progress was able to get fetched.
      if (index >= 0) {
        if (splittingConsumer != null) {
          currentElementProgress = splittingConsumer.getProgress();
        } else {
          currentElementProgress = 0.5;
        }
      }

      checkArgument(
          desiredSplit.getAllowedSplitPointsList().isEmpty(),
          "TODO: BEAM-3836, support split point restrictions.");

      // Now figure out where to split.
      //
      // The units here (except for keepOfElementRemainder) are all in terms of number or
      // (possibly fractional) elements.

      // Compute the amount of "remaining" work that we know of.
      double remainder = totalBufferSize - index - currentElementProgress;
      // Compute the number of elements (including fractional elements) that we should "keep".
      double keep = remainder * desiredSplit.getFractionOfRemainder();

      // If the downstream operator says the progress is less than 1 then the element could be
      // splittable.
      if (currentElementProgress < 1) {
        // See if the amount we need to keep falls within the current element's remainder and if
        // so, attempt to split it.
        double keepOfElementRemainder = keep / (1 - currentElementProgress);
        if (keepOfElementRemainder < 1) {
          SplitResult splitResult =
              splittingConsumer != null ? splittingConsumer.trySplit(keepOfElementRemainder) : null;
          if (splitResult != null) {
            stopIndex = index + 1;
            response
                .addPrimaryRoots(splitResult.getPrimaryRoot())
                .addResidualRoots(splitResult.getResidualRoot())
                .addChannelSplitsBuilder()
                .setLastPrimaryElement(index - 1)
                .setFirstResidualElement(stopIndex);
            return;
          }
        }
      }

      // Otherwise, split at the closest element boundary.
      int newStopIndex =
          Ints.checkedCast(index + Math.max(1, Math.round(currentElementProgress + keep)));
      if (newStopIndex < stopIndex) {
        stopIndex = newStopIndex;
        response
            .addChannelSplitsBuilder()
            .setLastPrimaryElement(stopIndex - 1)
            .setFirstResidualElement(stopIndex);
        return;
      }
    }
  }

  public void blockTillReadFinishes() throws Exception {
    LOG.debug(
        "Waiting for process bundle instruction {} and transform {} to close.",
        processBundleInstructionIdSupplier.get(),
        pTransformId);
    readFuture.awaitCompletion();
  }
}
