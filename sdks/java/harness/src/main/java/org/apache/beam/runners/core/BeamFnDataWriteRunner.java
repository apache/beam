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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
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

/**
 * Registers as a consumer with the Beam Fn Data Api. Consumes elements and encodes them for
 * transmission.
 *
 * <p>Can be re-used serially across {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleRequest}s.
 * For each request, call {@link #registerForOutput()} to start and call {@link #close()} to finish.
 */
public class BeamFnDataWriteRunner<InputT> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String URN = "urn:org.apache.beam:sink:runner:0.1";

  /** A registrar which provides a factory to handle writing to the Fn Api Data Plane. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataWriteRunner}s. */
  static class Factory<InputT>
      implements PTransformRunnerFactory<BeamFnDataWriteRunner<InputT>> {

    @Override
    public BeamFnDataWriteRunner<InputT> createRunnerForPTransform(
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
          .setName(getOnlyElement(pTransform.getInputsMap().keySet()))
          .build();
      RunnerApi.Coder coderSpec = coders.get(
          pCollections.get(getOnlyElement(pTransform.getInputsMap().values())).getCoderId());
      BeamFnDataWriteRunner<InputT> runner =
          new BeamFnDataWriteRunner<>(
              pTransform.getSpec(),
              processBundleInstructionId,
              target,
              coderSpec,
              beamFnDataClient);
      addStartFunction.accept(runner::registerForOutput);
      pCollectionIdsToConsumers.put(
          getOnlyElement(pTransform.getInputsMap().values()),
          (ThrowingConsumer)
              (ThrowingConsumer<WindowedValue<InputT>>) runner::consume);
      addFinishFunction.accept(runner::close);
      return runner;
    }
  }

  private final BeamFnApi.ApiServiceDescriptor apiServiceDescriptor;
  private final BeamFnApi.Target outputTarget;
  private final Coder<WindowedValue<InputT>> coder;
  private final BeamFnDataClient beamFnDataClientFactory;
  private final Supplier<String> processBundleInstructionIdSupplier;

  private CloseableThrowingConsumer<WindowedValue<InputT>> consumer;

  BeamFnDataWriteRunner(
      RunnerApi.FunctionSpec functionSpec,
      Supplier<String> processBundleInstructionIdSupplier,
      BeamFnApi.Target outputTarget,
      RunnerApi.Coder coderSpec,
      BeamFnDataClient beamFnDataClientFactory)
          throws IOException {
    this.apiServiceDescriptor = functionSpec.getParameter().unpack(BeamFnApi.RemoteGrpcPort.class)
        .getApiServiceDescriptor();
    this.beamFnDataClientFactory = beamFnDataClientFactory;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.outputTarget = outputTarget;

    @SuppressWarnings("unchecked")
    Coder<WindowedValue<InputT>> coder =
        (Coder<WindowedValue<InputT>>)
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

  public void registerForOutput() {
    consumer = beamFnDataClientFactory.forOutboundConsumer(
        apiServiceDescriptor,
        KV.of(processBundleInstructionIdSupplier.get(), outputTarget),
        coder);
  }

  public void close() throws Exception {
    consumer.close();
  }

  public void consume(WindowedValue<InputT> value) throws Exception {
    consumer.accept(value);
  }
}
