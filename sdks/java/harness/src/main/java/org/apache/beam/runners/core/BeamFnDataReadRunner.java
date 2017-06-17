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

package org.apache.beam.runners.core;

import static com.google.common.collect.Iterables.getOnlyElement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers as a consumer for data over the Beam Fn API. Multiplexes any received data
 * to all consumers in the specified output map.
 *
 * <p>Can be re-used serially across {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleRequest}s.
 * For each request, call {@link #registerInputLocation()} to start and call
 * {@link #blockTillReadFinishes()} to finish.
 */
public class BeamFnDataReadRunner<OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataReadRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String URN = "urn:org.apache.beam:source:runner:0.1";

  /** A registrar which provides a factory to handle reading from the Fn Api Data Plane. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataReadRunner}s. */
  static class Factory<OutputT>
      implements PTransformRunnerFactory<BeamFnDataReadRunner<OutputT>> {

    @Override
    public BeamFnDataReadRunner<OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        String pTransformId,
        RunnerApi.PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, RunnerApi.PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

      BeamFnApi.Target target = BeamFnApi.Target.newBuilder()
          .setPrimitiveTransformReference(pTransformId)
          .setName(getOnlyElement(pTransform.getOutputsMap().keySet()))
          .build();
      RunnerApi.Coder coderSpec = coders.get(pCollections.get(
          getOnlyElement(pTransform.getOutputsMap().values())).getCoderId());
      Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers =
          (Collection) pCollectionIdsToConsumers.get(
              getOnlyElement(pTransform.getOutputsMap().values()));

      BeamFnDataReadRunner<OutputT> runner = new BeamFnDataReadRunner<>(
          pTransform.getSpec(),
          processBundleInstructionId,
          target,
          coderSpec,
          beamFnDataClient,
          consumers);
      addStartFunction.accept(runner::registerInputLocation);
      addFinishFunction.accept(runner::blockTillReadFinishes);
      return runner;
    }
  }

  private final BeamFnApi.ApiServiceDescriptor apiServiceDescriptor;
  private final Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers;
  private final Supplier<String> processBundleInstructionIdSupplier;
  private final BeamFnDataClient beamFnDataClientFactory;
  private final Coder<WindowedValue<OutputT>> coder;
  private final BeamFnApi.Target inputTarget;

  private CompletableFuture<Void> readFuture;

  BeamFnDataReadRunner(
      RunnerApi.FunctionSpec functionSpec,
      Supplier<String> processBundleInstructionIdSupplier,
      BeamFnApi.Target inputTarget,
      RunnerApi.Coder coderSpec,
      BeamFnDataClient beamFnDataClientFactory,
      Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers)
          throws IOException {
    this.apiServiceDescriptor = functionSpec.getParameter().unpack(BeamFnApi.RemoteGrpcPort.class)
        .getApiServiceDescriptor();
    this.inputTarget = inputTarget;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.beamFnDataClientFactory = beamFnDataClientFactory;
    this.consumers = consumers;

    @SuppressWarnings("unchecked")
    Coder<WindowedValue<OutputT>> coder =
        (Coder<WindowedValue<OutputT>>)
            CloudObjects.coderFromCloudObject(
                CloudObject.fromSpec(
                    OBJECT_MAPPER.readValue(
                        coderSpec
                            .getSpec()
                            .getSpec()
                            .getParameter()
                            .unpack(BytesValue.class)
                            .getValue()
                            .newInput(),
                        Map.class)));
    this.coder = coder;
  }

  public void registerInputLocation() {
    this.readFuture = beamFnDataClientFactory.forInboundConsumer(
        apiServiceDescriptor,
        KV.of(processBundleInstructionIdSupplier.get(), inputTarget),
        coder,
        this::multiplexToConsumers);
  }

  public void blockTillReadFinishes() throws Exception {
    LOG.debug("Waiting for process bundle instruction {} and target {} to close.",
        processBundleInstructionIdSupplier.get(), inputTarget);
    readFuture.get();
  }

  private void multiplexToConsumers(WindowedValue<OutputT> value) throws Exception {
    for (ThrowingConsumer<WindowedValue<OutputT>> consumer : consumers) {
      consumer.accept(value);
    }
  }
}
