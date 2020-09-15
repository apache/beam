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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
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

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        "mainOutputTarget",
        pTransformId,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

    new FlattenRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            pTransformId,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            consumers,
            null /* startFunctionRegistry */,
            null /* finishFunctionRegistry */,
            null /* addResetFunction */,
            null /* tearDownRegistry */,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    mainOutputValues.clear();
    assertThat(
        consumers.keySet(),
        containsInAnyOrder("inputATarget", "inputBTarget", "inputCTarget", "mainOutputTarget"));

    consumers.getMultiplexingConsumer("inputATarget").accept(valueInGlobalWindow("A1"));
    consumers.getMultiplexingConsumer("inputATarget").accept(valueInGlobalWindow("A2"));
    consumers.getMultiplexingConsumer("inputBTarget").accept(valueInGlobalWindow("B"));
    consumers.getMultiplexingConsumer("inputCTarget").accept(valueInGlobalWindow("C"));
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

    List<WindowedValue<String>> mainOutputValues = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register(
        "mainOutputTarget",
        pTransformId,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) mainOutputValues::add);

    new FlattenRunner.Factory<>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            pTransformId,
            pTransform,
            Suppliers.ofInstance("57L")::get,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            consumers,
            null /* startFunctionRegistry */,
            null /* finishFunctionRegistry */,
            null /* addResetFunction */,
            null /* tearDownRegistry */,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    mainOutputValues.clear();
    assertThat(consumers.keySet(), containsInAnyOrder("inputATarget", "mainOutputTarget"));

    assertThat(consumers.getUnderlyingConsumers("inputATarget"), hasSize(2));

    FnDataReceiver<WindowedValue<?>> input = consumers.getMultiplexingConsumer("inputATarget");

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
