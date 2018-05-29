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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
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
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Registers as a consumer with the Beam Fn Data Api. Consumes elements and encodes them for
 * transmission.
 *
 * <p>Can be re-used serially across {@link BeamFnApi.ProcessBundleRequest}s.
 * For each request, call {@link #registerForOutput()} to start and call {@link #close()} to finish.
 */
public class BeamFnDataWriteRunner<InputT> {

  /** A registrar which provides a factory to handle writing to the Fn Api Data Plane. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(RemoteGrpcPortWrite.URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataWriteRunner}s. */
  static class Factory<InputT>
      implements PTransformRunnerFactory<BeamFnDataWriteRunner<InputT>> {

    @Override
    public BeamFnDataWriteRunner<InputT> createRunnerForPTransform(
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
          .setName(getOnlyElement(pTransform.getInputsMap().keySet()))
          .build();
      RunnerApi.Coder coderSpec = coders.get(
          pCollections.get(getOnlyElement(pTransform.getInputsMap().values())).getCoderId());
      BeamFnDataWriteRunner<InputT> runner =
          new BeamFnDataWriteRunner<>(
              pTransform,
              processBundleInstructionId,
              target,
              coderSpec,
              coders,
              beamFnDataClient);
      addStartFunction.accept(runner::registerForOutput);
      pCollectionIdsToConsumers.put(
          getOnlyElement(pTransform.getInputsMap().values()),
          (FnDataReceiver)
              (FnDataReceiver<WindowedValue<InputT>>) runner::consume);
      addFinishFunction.accept(runner::close);
      return runner;
    }
  }

  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final BeamFnApi.Target outputTarget;
  private final Coder<WindowedValue<InputT>> coder;
  private final BeamFnDataClient beamFnDataClientFactory;
  private final Supplier<String> processBundleInstructionIdSupplier;

  private CloseableFnDataReceiver<WindowedValue<InputT>> consumer;

  BeamFnDataWriteRunner(
      RunnerApi.PTransform remoteWriteNode,
      Supplier<String> processBundleInstructionIdSupplier,
      BeamFnApi.Target outputTarget,
      RunnerApi.Coder coderSpec,
      Map<String, RunnerApi.Coder> coders,
      BeamFnDataClient beamFnDataClientFactory)
          throws IOException {
    RemoteGrpcPort port = RemoteGrpcPortWrite.fromPTransform(remoteWriteNode).getPort();
    this.apiServiceDescriptor = port.getApiServiceDescriptor();
    this.beamFnDataClientFactory = beamFnDataClientFactory;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.outputTarget = outputTarget;

    RehydratedComponents components =
        RehydratedComponents.forComponents(Components.newBuilder().putAllCoders(coders).build());
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<InputT>> coder;
    if (!port.getCoderId().isEmpty()) {
      coder =
          (Coder<WindowedValue<InputT>>)
              CoderTranslation.fromProto(coders.get(port.getCoderId()), components);
    } else {
      // TODO: remove this path once it is no longer used
      coder = (Coder<WindowedValue<InputT>>) CoderTranslation.fromProto(coderSpec, components);
    }
    this.coder = coder;
  }

  public void registerForOutput() {
    consumer = beamFnDataClientFactory.send(
        apiServiceDescriptor,
        LogicalEndpoint.of(processBundleInstructionIdSupplier.get(), outputTarget),
        coder);
  }

  public void close() throws Exception {
    consumer.close();
  }

  public void consume(WindowedValue<InputT> value) throws Exception {
    consumer.accept(value);
  }
}
