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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.StateBackedIterable.StateBackedIterableTranslationContext;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Registers as a consumer with the Beam Fn Data Api. Consumes elements and encodes them for
 * transmission.
 *
 * <p>Can be re-used serially across {@link BeamFnApi.ProcessBundleRequest}s.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
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
  static class Factory<InputT> implements PTransformRunnerFactory<BeamFnDataWriteRunner> {

    @Override
    public BeamFnDataWriteRunner createRunnerForPTransform(Context context) throws IOException {

      RemoteGrpcPort port = RemoteGrpcPortWrite.fromPTransform(context.getPTransform()).getPort();
      RehydratedComponents components =
          RehydratedComponents.forComponents(
              Components.newBuilder().putAllCoders(context.getCoders()).build());
      Coder<WindowedValue<InputT>> coder =
          (Coder<WindowedValue<InputT>>)
              CoderTranslation.fromProto(
                  context.getCoders().get(port.getCoderId()),
                  components,
                  new StateBackedIterableTranslationContext() {
                    @Override
                    public Supplier<Cache<?, ?>> getCache() {
                      return context.getBundleCacheSupplier();
                    }

                    @Override
                    public BeamFnStateClient getStateClient() {
                      return context.getBeamFnStateClient();
                    }

                    @Override
                    public Supplier<String> getCurrentInstructionId() {
                      return context.getProcessBundleInstructionIdSupplier();
                    }
                  });
      context.addPCollectionConsumer(
          getOnlyElement(context.getPTransform().getInputsMap().values()),
          context.addOutgoingDataEndpoint(port.getApiServiceDescriptor(), coder));

      return new BeamFnDataWriteRunner();
    }
  }
}
