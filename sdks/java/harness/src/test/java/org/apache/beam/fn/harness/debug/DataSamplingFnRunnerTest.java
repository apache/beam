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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.ByteArrayOutputStream;
import java.util.*;
import org.apache.beam.fn.harness.PTransformRunnerFactoryTestContext;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataSamplingFnRunnerTest {

  @Test
  public void testCreatingAndProcessingWithSampling() throws Exception {
    // Create the DataSampling PTransform.
    String pTransformId = "pTransformId";

    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN).build();
    RunnerApi.PTransform pTransform =
        RunnerApi.PTransform.newBuilder()
            .setSpec(functionSpec)
            .putInputs("input", "inputTarget")
            .build();

    // Populate fake input PCollections.
    Map<String, PCollection> pCollectionMap = new HashMap<>();
    pCollectionMap.put(
        "inputTarget",
        RunnerApi.PCollection.newBuilder()
            .setUniqueName("inputTarget")
            .setCoderId("coder-id")
            .build());

    // Populate the PTransform context that includes the DataSampler.
    DataSampler dataSampler = new DataSampler();
    RunnerApi.Coder coder = CoderTranslation.toProto(StringUtf8Coder.of()).getCoder();
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(pTransformId, pTransform)
            .processBundleInstructionId("instruction-id")
            .pCollections(pCollectionMap)
            .coders(Collections.singletonMap("coder-id", coder))
            .dataSampler(dataSampler)
            .build();

    // Create the runner which samples the input PCollection.
    new DataSamplingFnRunner.Factory<>().createRunnerForPTransform(context);
    assertThat(context.getPCollectionConsumers().keySet(), contains("inputTarget"));

    // Send in a test value that should be sampled.
    context.getPCollectionConsumer("inputTarget").accept(valueInGlobalWindow("Hello, World!"));

    // Rehydrate the given utf-8 string coder.
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(
                RunnerApi.Components.newBuilder()
                    .putAllCoders(context.getCoders())
                    .putAllPcollections(context.getPCollections())
                    .putAllWindowingStrategies(context.getWindowingStrategies())
                    .build())
            .withPipeline(Pipeline.create());
    Coder<String> rehydratedCoder = (Coder<String>) rehydratedComponents.getCoder("coder-id");

    Map<String, List<byte[]>> samples = dataSampler.allSamples();
    assertThat(samples.keySet(), contains("inputTarget"));

    // Ensure that the value was sampled.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    rehydratedCoder.encode("Hello, World!", outputStream);
    byte[] encodedValue = outputStream.toByteArray();
    assertThat(samples.get("inputTarget"), contains(encodedValue));
  }
}
