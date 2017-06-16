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

package org.apache.beam.runners.core;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.test.TestExecutors;
import org.apache.beam.fn.harness.test.TestExecutors.TestExecutorService;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.core.PTransformRunnerFactory.Registrar;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
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

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final BeamFnApi.RemoteGrpcPort PORT_SPEC = BeamFnApi.RemoteGrpcPort.newBuilder()
      .setApiServiceDescriptor(BeamFnApi.ApiServiceDescriptor.getDefaultInstance()).build();
  private static final RunnerApi.FunctionSpec FUNCTION_SPEC = RunnerApi.FunctionSpec.newBuilder()
      .setParameter(Any.pack(PORT_SPEC)).build();
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final String CODER_SPEC_ID = "string-coder-id";
  private static final RunnerApi.Coder CODER_SPEC;
  private static final String URN = "urn:org.apache.beam:source:runner:0.1";

  static {
    try {
      CODER_SPEC = RunnerApi.Coder.newBuilder().setSpec(
          RunnerApi.SdkFunctionSpec.newBuilder().setSpec(
              RunnerApi.FunctionSpec.newBuilder().setParameter(
                  Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(
                      OBJECT_MAPPER.writeValueAsBytes(CloudObjects.asCloudObject(CODER))))
                      .build()))
                  .build())
              .build())
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
  @Captor private ArgumentCaptor<ThrowingConsumer<WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreatingAndProcessingBeamFnDataReadRunner() throws Exception {
    String bundleId = "57";
    String outputId = "101";

    List<WindowedValue<String>> outputValues = new ArrayList<>();

    Multimap<String, ThrowingConsumer<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("outputPC",
        (ThrowingConsumer) (ThrowingConsumer<WindowedValue<String>>) outputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn("urn:org.apache.beam:source:runner:0.1")
        .setParameter(Any.pack(PORT_SPEC))
        .build();

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putOutputs(outputId, "outputPC")
        .build();

    new BeamFnDataReadRunner.Factory<String>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        mockBeamFnDataClient,
        "pTransformId",
        pTransform,
        Suppliers.ofInstance(bundleId)::get,
        ImmutableMap.of("outputPC",
            RunnerApi.PCollection.newBuilder().setCoderId(CODER_SPEC_ID).build()),
        ImmutableMap.of(CODER_SPEC_ID, CODER_SPEC),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    verifyZeroInteractions(mockBeamFnDataClient);

    CompletableFuture<Void> completionFuture = new CompletableFuture<>();
    when(mockBeamFnDataClient.forInboundConsumer(any(), any(), any(), any()))
        .thenReturn(completionFuture);
    Iterables.getOnlyElement(startFunctions).run();
    verify(mockBeamFnDataClient).forInboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId, BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference("pTransformId")
            .setName(outputId)
            .build())),
        eq(CODER),
        consumerCaptor.capture());

    consumerCaptor.getValue().accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertThat(consumers.keySet(), containsInAnyOrder("outputPC"));

    completionFuture.complete(null);
    Iterables.getOnlyElement(finishFunctions).run();

    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  @Test
  public void testReuseForMultipleBundles() throws Exception {
    CompletableFuture<Void> bundle1Future = new CompletableFuture<>();
    CompletableFuture<Void> bundle2Future = new CompletableFuture<>();
    when(mockBeamFnDataClient.forInboundConsumer(
        any(),
        any(),
        any(),
        any())).thenReturn(bundle1Future).thenReturn(bundle2Future);
    List<WindowedValue<String>> valuesA = new ArrayList<>();
    List<WindowedValue<String>> valuesB = new ArrayList<>();

    AtomicReference<String> bundleId = new AtomicReference<>("0");
    BeamFnDataReadRunner<String> readRunner = new BeamFnDataReadRunner<>(
        FUNCTION_SPEC,
        bundleId::get,
        INPUT_TARGET,
        CODER_SPEC,
        mockBeamFnDataClient,
        ImmutableList.of(valuesA::add, valuesB::add));

    // Process for bundle id 0
    readRunner.registerInputLocation();

    verify(mockBeamFnDataClient).forInboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId.get(), INPUT_TARGET)),
        eq(CODER),
        consumerCaptor.capture());

    executor.submit(new Runnable() {
      @Override
      public void run() {
        // Sleep for some small amount of time simulating the parent blocking
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        try {
          consumerCaptor.getValue().accept(valueInGlobalWindow("ABC"));
          consumerCaptor.getValue().accept(valueInGlobalWindow("DEF"));
        } catch (Exception e) {
          bundle1Future.completeExceptionally(e);
        } finally {
          bundle1Future.complete(null);
        }
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

    verify(mockBeamFnDataClient).forInboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId.get(), INPUT_TARGET)),
        eq(CODER),
        consumerCaptor.capture());

    executor.submit(new Runnable() {
      @Override
      public void run() {
        // Sleep for some small amount of time simulating the parent blocking
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        try {
          consumerCaptor.getValue().accept(valueInGlobalWindow("GHI"));
          consumerCaptor.getValue().accept(valueInGlobalWindow("JKL"));
        } catch (Exception e) {
          bundle2Future.completeExceptionally(e);
        } finally {
          bundle2Future.complete(null);
        }
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
        assertThat(registrar.getPTransformRunnerFactories(), IsMapContaining.hasKey(URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
