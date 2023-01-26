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
package org.apache.beam.fn.harness.debug;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.beam.fn.harness.FlattenRunner;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
        "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class DataSamplingFnRunner {
  public static final String URN = "beam:internal:sampling:v1";

  private static final Logger LOG = LoggerFactory.getLogger(DataSamplingFnRunner.class);

  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, new Factory());
    }
  }

  public static class Factory<ElementT>
      implements PTransformRunnerFactory<DataSamplingFnRunner> {
    @Override
    public DataSamplingFnRunner createRunnerForPTransform(Context context)
        throws IOException {
      DataSamplingFnRunner runner = new DataSamplingFnRunner();

      Optional<DataSampler> maybeDataSampler = context.getDataSampler();
      if (!maybeDataSampler.isPresent()) {
        LOG.warn("Trying to sample output but DataSampler is not present. Is " +
                 "\"enable_data_sampling\" set?");
        return runner;
      }

      DataSampler dataSampler = maybeDataSampler.get();

      String inputPCollectionId =
          Iterables.getOnlyElement(context.getPTransform().getInputsMap().values());

      RunnerApi.PCollection inputPCollection = context
              .getPCollections()
              .get(inputPCollectionId);

      if (inputPCollection == null) {
        LOG.warn("Expected input PCollection \"" + inputPCollectionId + "\" does not exist in " +
                 "PCollections map.");
        return runner;
      }

      String inputCoderId = inputPCollection.getCoderId();

      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(
                  RunnerApi.Components.newBuilder()
                      .putAllCoders(context.getCoders())
                      .putAllPcollections(context.getPCollections())
                      .putAllWindowingStrategies(context.getWindowingStrategies())
                      .build())
              .withPipeline(Pipeline.create());
      Coder<ElementT> inputCoder = (Coder<ElementT>)rehydratedComponents.getCoder(inputCoderId);

      DataSampler.OutputSampler<ElementT> outputSampler = dataSampler.sampleOutput(
              inputPCollectionId,
              inputCoder);

      String pCollectionId =
          Iterables.getOnlyElement(context.getPTransform().getInputsMap().values());
      context.addPCollectionConsumer(pCollectionId,
              (FnDataReceiver<WindowedValue<ElementT>>) input -> outputSampler.sample(input.getValue()));
      return runner;
    }
  }
}
