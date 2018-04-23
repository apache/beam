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

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.MultiplexingFnDataReceiver;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
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
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers as a consumer for data over the Beam Fn API. Multiplexes any received data
 * to all receivers in a specified output map.
 *
 * <p>Can be re-used serially across {@link BeamFnApi.ProcessBundleRequest}s.
 * For each request, call {@link #registerInputLocation()} to start and call
 * {@link #blockTillReadFinishes()} to finish.
 */
public class BeamFnDataReadRunner<OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataReadRunner.class);

  /**
   * A registrar which provides a factory to handle reading from the Fn Api Data
   * Plane.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(RemoteGrpcPortRead.URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataReadRunner}s. */
  static class Factory<OutputT>
      implements PTransformRunnerFactory<BeamFnDataReadRunner<OutputT>> {

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
        Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

      BeamFnApi.Target target = BeamFnApi.Target.newBuilder()
          .setPrimitiveTransformReference(pTransformId)
          .setName(getOnlyElement(pTransform.getOutputsMap().keySet()))
          .build();
      RunnerApi.Coder coderSpec =
          coders.get(
              pCollections.get(getOnlyElement(pTransform.getOutputsMap().values())).getCoderId());
      Collection<FnDataReceiver<WindowedValue<OutputT>>> consumers =
          (Collection) pCollectionIdsToConsumers.get(
              getOnlyElement(pTransform.getOutputsMap().values()));

      BeamFnDataReadRunner<OutputT> runner = new BeamFnDataReadRunner<>(
          pTransform,
          processBundleInstructionId,
          target,
          coderSpec,
          coders,
          beamFnDataClient,
          consumers);
      addStartFunction.accept(runner::registerInputLocation);
      addFinishFunction.accept(runner::blockTillReadFinishes);
      return runner;
    }
  }

  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final FnDataReceiver<WindowedValue<OutputT>> receiver;
  private final Supplier<String> processBundleInstructionIdSupplier;
  private final BeamFnDataClient beamFnDataClient;
  private final Coder<WindowedValue<OutputT>> coder;
  private final BeamFnApi.Target inputTarget;

  private InboundDataClient readFuture;

  BeamFnDataReadRunner(
      RunnerApi.PTransform grpcReadNode,
      Supplier<String> processBundleInstructionIdSupplier,
      BeamFnApi.Target inputTarget,
      RunnerApi.Coder coderSpec,
      Map<String, RunnerApi.Coder> coders,
      BeamFnDataClient beamFnDataClient,
      Collection<FnDataReceiver<WindowedValue<OutputT>>> consumers)
      throws IOException {
    RemoteGrpcPort port = RemoteGrpcPortRead.fromPTransform(grpcReadNode).getPort();
    this.apiServiceDescriptor = port.getApiServiceDescriptor();
    this.inputTarget = inputTarget;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.beamFnDataClient = beamFnDataClient;
    this.receiver = MultiplexingFnDataReceiver.forConsumers(consumers);

    RehydratedComponents components =
        RehydratedComponents.forComponents(Components.newBuilder().putAllCoders(coders).build());
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<OutputT>> coder;
    if (!port.getCoderId().isEmpty()) {
      coder =
          (Coder<WindowedValue<OutputT>>)
              CoderTranslation.fromProto(coders.get(port.getCoderId()), components);
    } else {
      // TODO: Remove this path once it is no longer used
      coder =
          (Coder<WindowedValue<OutputT>>)
              CoderTranslation.fromProto(
                  coderSpec,
                  components);
    }
    this.coder = coder;
  }

  public void registerInputLocation() {
    this.readFuture = beamFnDataClient.receive(
        apiServiceDescriptor,
        LogicalEndpoint.of(processBundleInstructionIdSupplier.get(), inputTarget),
        coder,
        receiver);
  }

  public void blockTillReadFinishes() throws Exception {
    LOG.debug("Waiting for process bundle instruction {} and target {} to close.",
        processBundleInstructionIdSupplier.get(), inputTarget);
    readFuture.awaitCompletion();
  }
}
