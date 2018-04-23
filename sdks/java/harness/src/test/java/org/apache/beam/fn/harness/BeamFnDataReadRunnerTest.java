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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.CompletableFutureInboundDataClient;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BeamFnDataReadRunner}. */
@RunWith(JUnit4.class)
public class BeamFnDataReadRunnerTest {

  private static final Coder<String> ELEMENT_CODER = StringUtf8Coder.of();
  private static final String ELEMENT_CODER_SPEC_ID = "string-coder-id";
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(ELEMENT_CODER, GlobalWindow.Coder.INSTANCE);
  private static final String CODER_SPEC_ID = "windowed-string-coder-id";
  private static final RunnerApi.Coder CODER_SPEC;
  private static final RunnerApi.Components COMPONENTS;
  private static final BeamFnApi.RemoteGrpcPort PORT_SPEC =
      BeamFnApi.RemoteGrpcPort.newBuilder()
          .setApiServiceDescriptor(Endpoints.ApiServiceDescriptor.getDefaultInstance())
          .setCoderId(CODER_SPEC_ID)
          .build();

  static {
    try {
      MessageWithComponents coderAndComponents = CoderTranslation.toProto(CODER);
      CODER_SPEC = coderAndComponents.getCoder();
      COMPONENTS =
          coderAndComponents
              .getComponents()
              .toBuilder()
              .putCoders(CODER_SPEC_ID, CODER_SPEC)
              .putCoders(ELEMENT_CODER_SPEC_ID, CoderTranslation.toProto(ELEMENT_CODER).getCoder())
              .build();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final BeamFnApi.Target INPUT_TARGET = BeamFnApi.Target.newBuilder()
      .setPrimitiveTransformReference("1")
      .setName("out")
      .build();

  @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);
  @Mock private BeamFnDataClient mockBeamFnDataClient;
  @Captor private ArgumentCaptor<FnDataReceiver<WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreatingAndProcessingBeamFnDataReadRunner() throws Exception {
    String bundleId = "57";

    List<WindowedValue<String>> outputValues = new ArrayList<>();

    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    String localOutputId = "outputPC";
    consumers.put(localOutputId,
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) outputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.PTransform pTransform =
        RemoteGrpcPortRead.readFromPort(PORT_SPEC, localOutputId).toPTransform();

    new BeamFnDataReadRunner.Factory<String>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        mockBeamFnDataClient,
        null /* beamFnStateClient */,
        "pTransformId",
        pTransform,
        Suppliers.ofInstance(bundleId)::get,
        ImmutableMap.of(localOutputId,
            RunnerApi.PCollection.newBuilder().setCoderId(ELEMENT_CODER_SPEC_ID).build()),
        COMPONENTS.getCodersMap(),
        COMPONENTS.getWindowingStrategiesMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    verifyZeroInteractions(mockBeamFnDataClient);

    InboundDataClient completionFuture = CompletableFutureInboundDataClient.create();
    when(mockBeamFnDataClient.receive(any(), any(), any(), any()))
        .thenReturn(completionFuture);
    Iterables.getOnlyElement(startFunctions).run();
    verify(mockBeamFnDataClient).receive(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(LogicalEndpoint.of(bundleId, BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference("pTransformId")
            .setName(Iterables.getOnlyElement(pTransform.getOutputsMap().keySet()))
            .build())),
        eq(CODER),
        consumerCaptor.capture());

    consumerCaptor.getValue().accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder(localOutputId));

    completionFuture.complete();
    Iterables.getOnlyElement(finishFunctions).run();

    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  @Test
  public void testReuseForMultipleBundles() throws Exception {
    InboundDataClient bundle1Future = CompletableFutureInboundDataClient.create();
    InboundDataClient bundle2Future = CompletableFutureInboundDataClient.create();
    when(mockBeamFnDataClient.receive(
        any(),
        any(),
        any(),
        any())).thenReturn(bundle1Future).thenReturn(bundle2Future);
    List<WindowedValue<String>> valuesA = new ArrayList<>();
    List<WindowedValue<String>> valuesB = new ArrayList<>();

    AtomicReference<String> bundleId = new AtomicReference<>("0");
    BeamFnDataReadRunner<String> readRunner = new BeamFnDataReadRunner<>(
        RemoteGrpcPortRead.readFromPort(PORT_SPEC, "localOutput").toPTransform(),
        bundleId::get,
        INPUT_TARGET,
        CODER_SPEC,
        COMPONENTS.getCodersMap(),
        mockBeamFnDataClient,
        ImmutableList.of(valuesA::add, valuesB::add));

    // Process for bundle id 0
    readRunner.registerInputLocation();

    verify(mockBeamFnDataClient).receive(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(LogicalEndpoint.of(bundleId.get(), INPUT_TARGET)),
        eq(CODER),
        consumerCaptor.capture());

    executor.submit(
        () -> {
          // Sleep for some small amount of time simulating the parent blocking
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          try {
            consumerCaptor.getValue().accept(valueInGlobalWindow("ABC"));
            consumerCaptor.getValue().accept(valueInGlobalWindow("DEF"));
          } catch (Exception e) {
            bundle1Future.fail(e);
          } finally {
            bundle1Future.complete();
          }
        });

    readRunner.blockTillReadFinishes();
    assertThat(valuesA, contains(valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF")));
    assertThat(valuesB, contains(valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF")));

    // Process for bundle id 1
    bundleId.set("1");
    valuesA.clear();
    valuesB.clear();
    readRunner.registerInputLocation();

    verify(mockBeamFnDataClient).receive(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(LogicalEndpoint.of(bundleId.get(), INPUT_TARGET)),
        eq(CODER),
        consumerCaptor.capture());

    executor.submit(
        () -> {
          // Sleep for some small amount of time simulating the parent blocking
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          try {
            consumerCaptor.getValue().accept(valueInGlobalWindow("GHI"));
            consumerCaptor.getValue().accept(valueInGlobalWindow("JKL"));
          } catch (Exception e) {
            bundle2Future.fail(e);
          } finally {
            bundle2Future.complete();
          }
        });

    readRunner.blockTillReadFinishes();
    assertThat(valuesA, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));
    assertThat(valuesB, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));

    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  @Test
  public void testRegistration() {
    for (Registrar registrar :
        ServiceLoader.load(Registrar.class)) {
      if (registrar instanceof BeamFnDataReadRunner.Registrar) {
        assertThat(
            registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(RemoteGrpcPortRead.URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
