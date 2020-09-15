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
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.fn.harness.MapFnRunners.ValueMapFnFactory;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MapFnRunners}. */
@RunWith(JUnit4.class)
public class MapFnRunnersTest {
  private static final String EXPECTED_ID = "pTransformId";
  private static final RunnerApi.PTransform EXPECTED_PTRANSFORM =
      RunnerApi.PTransform.newBuilder()
          .putInputs("input", "inputPC")
          .putOutputs("output", "outputPC")
          .build();

  @Test
  public void testValueOnlyMapping() throws Exception {
    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register("outputPC", EXPECTED_ID, outputConsumer::add);

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    ValueMapFnFactory<String, String> factory = (ptId, pt) -> String::toUpperCase;
    MapFnRunners.forValueMapFnFactory(factory)
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            EXPECTED_ID,
            EXPECTED_PTRANSFORM,
            Suppliers.ofInstance("57L")::get,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* addResetFunction */,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    assertThat(startFunctionRegistry.getFunctions(), empty());
    assertThat(finishFunctionRegistry.getFunctions(), empty());
    assertThat(teardownFunctions, empty());

    assertThat(consumers.keySet(), containsInAnyOrder("inputPC", "outputPC"));

    consumers.getMultiplexingConsumer("inputPC").accept(valueInGlobalWindow("abc"));

    assertThat(outputConsumer, contains(valueInGlobalWindow("ABC")));
  }

  @Test
  public void testFullWindowedValueMapping() throws Exception {
    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    consumers.register("outputPC", EXPECTED_ID, outputConsumer::add);

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    MapFnRunners.forWindowedValueMapFnFactory(this::createMapFunctionForPTransform)
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            EXPECTED_ID,
            EXPECTED_PTRANSFORM,
            Suppliers.ofInstance("57L")::get,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* addResetFunction */,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    assertThat(startFunctionRegistry.getFunctions(), empty());
    assertThat(finishFunctionRegistry.getFunctions(), empty());
    assertThat(teardownFunctions, empty());

    assertThat(consumers.keySet(), containsInAnyOrder("inputPC", "outputPC"));

    consumers.getMultiplexingConsumer("inputPC").accept(valueInGlobalWindow("abc"));

    assertThat(outputConsumer, contains(valueInGlobalWindow("ABC")));
  }

  @Test
  public void testFullWindowedValueMappingWithCompressedWindow() throws Exception {
    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class));
    consumers.register("outputPC", "pTransformId", outputConsumer::add);

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    MapFnRunners.forWindowedValueMapFnFactory(this::createMapFunctionForPTransform)
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            EXPECTED_ID,
            EXPECTED_PTRANSFORM,
            Suppliers.ofInstance("57L")::get,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            null /* addResetFunction */,
            teardownFunctions::add,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    assertThat(startFunctionRegistry.getFunctions(), empty());
    assertThat(finishFunctionRegistry.getFunctions(), empty());
    assertThat(teardownFunctions, empty());

    assertThat(consumers.keySet(), containsInAnyOrder("inputPC", "outputPC"));

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0L), Duration.standardMinutes(10L));
    IntervalWindow secondWindow =
        new IntervalWindow(new Instant(-10L), Duration.standardSeconds(22L));
    consumers
        .getMultiplexingConsumer("inputPC")
        .accept(
            WindowedValue.of(
                "abc",
                new Instant(12),
                ImmutableSet.of(firstWindow, GlobalWindow.INSTANCE, secondWindow),
                PaneInfo.NO_FIRING));

    assertThat(
        outputConsumer,
        containsInAnyOrder(
            WindowedValue.timestampedValueInGlobalWindow("ABC", new Instant(12)),
            WindowedValue.of("ABC", new Instant(12), secondWindow, PaneInfo.NO_FIRING),
            WindowedValue.of("ABC", new Instant(12), firstWindow, PaneInfo.NO_FIRING)));
  }

  public ThrowingFunction<WindowedValue<String>, WindowedValue<String>>
      createMapFunctionForPTransform(String ptransformId, PTransform pTransform) {
    assertEquals(EXPECTED_ID, ptransformId);
    assertEquals(EXPECTED_PTRANSFORM, pTransform);
    return (WindowedValue<String> str) -> str.withValue(str.getValue().toUpperCase());
  }
}
