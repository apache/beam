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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.StateBackedIterable.StateBackedIterableTranslationContext;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Registers as a consumer with the Beam Fn Data Api. Consumes elements and encodes them for
 * transmission.
 *
 * <p>Can be re-used serially across {@link BeamFnApi.ProcessBundleRequest}s. For each request, call
 * {@link #registerForOutput()} to start and call {@link #close()} to finish.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamFnDataWriteRunner<InputT> {

  /** A registrar which provides a factory to handle writing to the Fn Api Data Plane. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(RemoteGrpcPortWrite.URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataWriteRunner}s. */
  static class Factory<InputT> implements PTransformRunnerFactory<BeamFnDataWriteRunner<InputT>> {

    @Override
    public BeamFnDataWriteRunner<InputT> createRunnerForPTransform(Context context)
        throws IOException {

      BeamFnDataWriteRunner<InputT> runner =
          new BeamFnDataWriteRunner<>(
              context.getBundleCacheSupplier(),
              context.getPTransformId(),
              context.getPTransform(),
              context.getProcessBundleInstructionIdSupplier(),
              context.getCoders(),
              context.getBeamFnDataClient(),
              context.getBeamFnStateClient());
      context.addStartBundleFunction(runner::registerForOutput);
      context.addPCollectionConsumer(
          getOnlyElement(context.getPTransform().getInputsMap().values()),
          (FnDataReceiver) (FnDataReceiver<WindowedValue<InputT>>) runner::consume,
          ((WindowedValueCoder<InputT>) runner.coder).getValueCoder());

      context.addFinishBundleFunction(runner::close);
      return runner;
    }
  }

  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final String pTransformId;
  private final Coder<WindowedValue<InputT>> coder;
  private final BeamFnDataClient beamFnDataClientFactory;
  private final Supplier<String> processBundleInstructionIdSupplier;

  private CloseableFnDataReceiver<WindowedValue<InputT>> consumer;

  BeamFnDataWriteRunner(
      Supplier<Cache<?, ?>> cache,
      String pTransformId,
      RunnerApi.PTransform remoteWriteNode,
      Supplier<String> processBundleInstructionIdSupplier,
      Map<String, RunnerApi.Coder> coders,
      BeamFnDataClient beamFnDataClientFactory,
      BeamFnStateClient beamFnStateClient)
      throws IOException {
    this.pTransformId = pTransformId;
    RemoteGrpcPort port = RemoteGrpcPortWrite.fromPTransform(remoteWriteNode).getPort();
    this.apiServiceDescriptor = port.getApiServiceDescriptor();
    this.beamFnDataClientFactory = beamFnDataClientFactory;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;

    RehydratedComponents components =
        RehydratedComponents.forComponents(Components.newBuilder().putAllCoders(coders).build());
    this.coder =
        (Coder<WindowedValue<InputT>>)
            CoderTranslation.fromProto(
                coders.get(port.getCoderId()),
                components,
                new StateBackedIterableTranslationContext() {
                  @Override
                  public Supplier<Cache<?, ?>> getCache() {
                    return cache;
                  }

                  @Override
                  public BeamFnStateClient getStateClient() {
                    return beamFnStateClient;
                  }

                  @Override
                  public Supplier<String> getCurrentInstructionId() {
                    return processBundleInstructionIdSupplier;
                  }
                });
  }

  public void registerForOutput() {
    consumer =
        beamFnDataClientFactory.send(
            apiServiceDescriptor,
            LogicalEndpoint.data(processBundleInstructionIdSupplier.get(), pTransformId),
            coder);
  }

  public void close() throws Exception {
    consumer.close();
  }

  public void consume(WindowedValue<InputT> value) throws Exception {
    consumer.accept(value);
  }
}
