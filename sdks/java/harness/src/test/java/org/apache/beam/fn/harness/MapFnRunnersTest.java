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
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.fn.harness.MapFnRunners.ValueMapFnFactory;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
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
  private static final RunnerApi.PCollection INPUT_PCOLLECTION =
      RunnerApi.PCollection.newBuilder().setUniqueName("inputPC").setCoderId("coder-id").build();
  private static RunnerApi.Coder valueCoder;

  static {
    try {
      valueCoder = CoderTranslation.toProto(StringUtf8Coder.of()).getCoder();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  public void testValueOnlyMapping() throws Exception {
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(EXPECTED_ID, EXPECTED_PTRANSFORM)
            .processBundleInstructionId("57")
            .pCollections(Collections.singletonMap("inputPC", INPUT_PCOLLECTION))
            .coders(Collections.singletonMap("coder-id", valueCoder))
            .build();
    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    context.addPCollectionConsumer("outputPC", outputConsumer::add);

    ValueMapFnFactory<String, String> factory = (ptId, pt) -> String::toUpperCase;
    MapFnRunners.forValueMapFnFactory(factory).createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());
    assertThat(context.getTearDownFunctions(), empty());

    assertThat(
        context.getPCollectionConsumers().keySet(), containsInAnyOrder("inputPC", "outputPC"));

    context.getPCollectionConsumer("inputPC").accept(valueInGlobalWindow("abc"));

    assertThat(outputConsumer, contains(valueInGlobalWindow("ABC")));
  }

  @Test
  public void testFullWindowedValueMapping() throws Exception {
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(EXPECTED_ID, EXPECTED_PTRANSFORM)
            .processBundleInstructionId("57")
            .pCollections(Collections.singletonMap("inputPC", INPUT_PCOLLECTION))
            .coders(Collections.singletonMap("coder-id", valueCoder))
            .build();
    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    context.addPCollectionConsumer("outputPC", outputConsumer::add);
    MapFnRunners.forWindowedValueMapFnFactory(this::createMapFunctionForPTransform)
        .createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());
    assertThat(context.getTearDownFunctions(), empty());

    assertThat(
        context.getPCollectionConsumers().keySet(), containsInAnyOrder("inputPC", "outputPC"));

    context.getPCollectionConsumer("inputPC").accept(valueInGlobalWindow("abc"));

    assertThat(outputConsumer, contains(valueInGlobalWindow("ABC")));
  }

  @Test
  public void testFullWindowedValueMappingWithCompressedWindow() throws Exception {
    PTransformRunnerFactoryTestContext context =
        PTransformRunnerFactoryTestContext.builder(EXPECTED_ID, EXPECTED_PTRANSFORM)
            .processBundleInstructionId("57")
            .pCollections(Collections.singletonMap("inputPC", INPUT_PCOLLECTION))
            .coders(Collections.singletonMap("coder-id", valueCoder))
            .build();
    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    context.addPCollectionConsumer("outputPC", outputConsumer::add);

    MapFnRunners.forWindowedValueMapFnFactory(this::createMapFunctionForPTransform)
        .createRunnerForPTransform(context);

    assertThat(context.getStartBundleFunctions(), empty());
    assertThat(context.getFinishBundleFunctions(), empty());
    assertThat(context.getTearDownFunctions(), empty());

    assertThat(
        context.getPCollectionConsumers().keySet(), containsInAnyOrder("inputPC", "outputPC"));

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0L), Duration.standardMinutes(10L));
    IntervalWindow secondWindow =
        new IntervalWindow(new Instant(-10L), Duration.standardSeconds(22L));
    context
        .getPCollectionConsumer("inputPC")
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
