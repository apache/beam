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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;

public class FlattenRunner<InputT, OutputT>{
  /**
   * A registrar which provides a factory to handle Java {@link DoFn}s.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          PTransformTranslation.FLATTEN_TRANSFORM_URN, new Factory());
    }

    /** A factory for {@link FlattenRunner}s. */
    static class Factory<InputT> implements
        PTransformRunnerFactory<FlattenRunner<InputT>> {
      @Override
      public FlattenRunner<InputT> createRunnerForPTransform(
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
          Consumer<ThrowingRunnable> addFinishFunction) {

      }
    }
  }

}
