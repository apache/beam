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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlattenRunner}. */
@RunWith(JUnit4.class)
public class FlattenRunnerTest {

  /**
   * Create a Flatten that has 4 inputs (inputATarget1, inputATarget2, inputBTarget, inputCTarget)
   * and one output (mainOutput). Validate that inputs are flattened together and directed to the
   * output.
   */
  @Test
  public void testCreatingAndProcessingDoFlatten() throws Exception {
    String pTransformId = "pTransformId";
    String mainOutputId = "101";

    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN)
            .build();
    RunnerApi.PTransform pTransform =
        RunnerApi.PTransform.newBuilder()
            .setSpec(functionSpec)
            .putInputs("inputA", "inputATarget")
            .putInputs("inputB", "inputBTarget")
            .putInputs("inputC", "inputCTarget")
            .putOutputs(mainOutputId, "mainOutputTarget")
            .build();

    Map<String, PCollection> pCollectionMap = new HashMap<>();
    pCollectionMap.put(
        "inputATarget",
        RunnerApi.PCollection.newBuilder()
            .setUniqueName("inputATarget")
            .setCoderId("coder-id")
            .build());
    pCollectionMap.put(
        "inputBTarget",
        RunnerApi.PCollection.newBuilder()
            .setUniqueName("inputBTarget")
            .setCoderId("coder-id")
            .build());
    pCollectionMap.put(
        "inputCTarget",
        RunnerApi.PCollection.newBuilder()
            .setUniqueName("inputCTarget")
            .setCoderId("coder-id")
            .build());
    RunnerApi.Coder coder = CoderTranslation.toProto(StringUtf8Coder.of()).getCoder();

    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(pTransformId, pTransform)
            .processBundleInstructionId("57")
            .pCollections(pCollectionMap)
            .coders(Collections.singletonMap("coder-id", coder))
            .build();
    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    context.addPCollectionConsumer(
        "mainOutputTarget",
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

    new FlattenRunner.Factory<>().createRunnerForPTransform(context);

    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder("inputATarget", "inputBTarget", "inputCTarget", "mainOutputTarget"));

    context.getPCollectionConsumer("inputATarget").accept(valueInGlobalWindow("A1"));
    context.getPCollectionConsumer("inputATarget").accept(valueInGlobalWindow("A2"));
    context.getPCollectionConsumer("inputBTarget").accept(valueInGlobalWindow("B"));
    context.getPCollectionConsumer("inputCTarget").accept(valueInGlobalWindow("C"));
    assertThat(
        mainOutputValues,
        contains(
            valueInGlobalWindow("A1"),
            valueInGlobalWindow("A2"),
            valueInGlobalWindow("B"),
            valueInGlobalWindow("C")));

    mainOutputValues.clear();
  }

  /**
   * Create a Flatten that consumes data from the same PCollection duplicated through two outputs
   * and validates that inputs are flattened together and directed to the output.
   */
  @Test
  public void testFlattenWithDuplicateInputCollectionProducesMultipleOutputs() throws Exception {
    String pTransformId = "pTransformId";
    String mainOutputId = "101";
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN)
            .build();
    RunnerApi.PTransform pTransform =
        RunnerApi.PTransform.newBuilder()
            .setSpec(functionSpec)
            .putInputs("inputA", "inputATarget")
            .putInputs("inputAAgain", "inputATarget")
            .putOutputs(mainOutputId, "mainOutputTarget")
            .build();

    RunnerApi.PCollection pCollection =
        RunnerApi.PCollection.newBuilder()
            .setUniqueName("inputATarget")
            .setCoderId("coder-id")
            .build();
    RunnerApi.Coder coder = CoderTranslation.toProto(StringUtf8Coder.of()).getCoder();

    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(pTransformId, pTransform)
            .processBundleInstructionId("57")
            .pCollections(Collections.singletonMap("inputATarget", pCollection))
            .coders(Collections.singletonMap("coder-id", coder))
            .build();
    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    context.addPCollectionConsumer(
        "mainOutputTarget",
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

    new FlattenRunner.Factory<>().createRunnerForPTransform(context);

    mainOutputValues.clear();
    assertThat(
        context.getPCollectionConsumers().keySet(),
        containsInAnyOrder("inputATarget", "mainOutputTarget"));

    assertThat(context.getPCollectionConsumers().get("inputATarget"), hasSize(2));

    FnDataReceiver<WindowedValue<?>> input = context.getPCollectionConsumer("inputATarget");

    input.accept(WindowedValue.valueInGlobalWindow("A1"));
    input.accept(WindowedValue.valueInGlobalWindow("A2"));

    assertThat(
        mainOutputValues,
        containsInAnyOrder(
            valueInGlobalWindow("A1"),
            valueInGlobalWindow("A1"),
            valueInGlobalWindow("A2"),
            valueInGlobalWindow("A2")));
  }
}
