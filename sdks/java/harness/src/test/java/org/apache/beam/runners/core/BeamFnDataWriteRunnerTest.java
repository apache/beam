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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BeamFnDataWriteRunner}. */
@RunWith(JUnit4.class)
public class BeamFnDataWriteRunnerTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final BeamFnApi.RemoteGrpcPort PORT_SPEC = BeamFnApi.RemoteGrpcPort.newBuilder()
      .setApiServiceDescriptor(BeamFnApi.ApiServiceDescriptor.getDefaultInstance()).build();
  private static final RunnerApi.FunctionSpec FUNCTION_SPEC = RunnerApi.FunctionSpec.newBuilder()
      .setParameter(Any.pack(PORT_SPEC)).build();
  private static final String CODER_ID = "string-coder-id";
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final RunnerApi.Coder CODER_SPEC;
  private static final String URN = "urn:org.apache.beam:sink:runner:0.1";

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
  private static final BeamFnApi.Target OUTPUT_TARGET = BeamFnApi.Target.newBuilder()
      .setPrimitiveTransformReference("1")
      .setName("out")
      .build();

  @Mock private BeamFnDataClient mockBeamFnDataClient;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void testCreatingAndProcessingBeamFnDataWriteRunner() throws Exception {
    String bundleId = "57L";
    String inputId = "100L";

    Multimap<String, ThrowingConsumer<WindowedValue<?>>> consumers = HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.FunctionSpec functionSpec = RunnerApi.FunctionSpec.newBuilder()
        .setUrn("urn:org.apache.beam:sink:runner:0.1")
        .setParameter(Any.pack(PORT_SPEC))
        .build();

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs(inputId, "inputPC")
        .build();

    new BeamFnDataWriteRunner.Factory<String>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        mockBeamFnDataClient,
        "ptransformId",
        pTransform,
        Suppliers.ofInstance(bundleId)::get,
        ImmutableMap.of("inputPC",
            RunnerApi.PCollection.newBuilder().setCoderId(CODER_ID).build()),
        ImmutableMap.of(CODER_ID, CODER_SPEC),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    verifyZeroInteractions(mockBeamFnDataClient);

    List<WindowedValue<String>> outputValues = new ArrayList<>();
    AtomicBoolean wasCloseCalled = new AtomicBoolean();
    CloseableThrowingConsumer<WindowedValue<String>> outputConsumer =
        new CloseableThrowingConsumer<WindowedValue<String>>(){
          @Override
          public void close() throws Exception {
            wasCloseCalled.set(true);
          }

          @Override
          public void accept(WindowedValue<String> t) throws Exception {
            outputValues.add(t);
          }
        };

    when(mockBeamFnDataClient.forOutboundConsumer(
        any(),
        any(),
        Matchers.<Coder<WindowedValue<String>>>any())).thenReturn(outputConsumer);
    Iterables.getOnlyElement(startFunctions).run();
    verify(mockBeamFnDataClient).forOutboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId, BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference("ptransformId")
            .setName(inputId)
            .build())),
        eq(CODER));

    assertThat(consumers.keySet(), containsInAnyOrder("inputPC"));
    Iterables.getOnlyElement(consumers.get("inputPC")).accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertFalse(wasCloseCalled.get());
    Iterables.getOnlyElement(finishFunctions).run();
    assertTrue(wasCloseCalled.get());

    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  @Test
  public void testReuseForMultipleBundles() throws Exception {
    RecordingConsumer<WindowedValue<String>> valuesA = new RecordingConsumer<>();
    RecordingConsumer<WindowedValue<String>> valuesB = new RecordingConsumer<>();
    when(mockBeamFnDataClient.forOutboundConsumer(
        any(),
        any(),
        Matchers.<Coder<WindowedValue<String>>>any())).thenReturn(valuesA).thenReturn(valuesB);
    AtomicReference<String> bundleId = new AtomicReference<>("0");
    BeamFnDataWriteRunner<String> writeRunner = new BeamFnDataWriteRunner<>(
        FUNCTION_SPEC,
        bundleId::get,
        OUTPUT_TARGET,
        CODER_SPEC,
        mockBeamFnDataClient);

    // Process for bundle id 0
    writeRunner.registerForOutput();

    verify(mockBeamFnDataClient).forOutboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId.get(), OUTPUT_TARGET)),
        eq(CODER));

    writeRunner.consume(valueInGlobalWindow("ABC"));
    writeRunner.consume(valueInGlobalWindow("DEF"));
    writeRunner.close();

    assertTrue(valuesA.closed);
    assertThat(valuesA, contains(valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF")));

    // Process for bundle id 1
    bundleId.set("1");
    valuesA.clear();
    valuesB.clear();
    writeRunner.registerForOutput();

    verify(mockBeamFnDataClient).forOutboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId.get(), OUTPUT_TARGET)),
        eq(CODER));

    writeRunner.consume(valueInGlobalWindow("GHI"));
    writeRunner.consume(valueInGlobalWindow("JKL"));
    writeRunner.close();

    assertTrue(valuesB.closed);
    assertThat(valuesB, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));
    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  private static class RecordingConsumer<T> extends ArrayList<T>
      implements CloseableThrowingConsumer<T> {
    private boolean closed;
    @Override
    public void close() throws Exception {
      closed = true;
    }

    @Override
    public void accept(T t) throws Exception {
      if (closed) {
        throw new IllegalStateException("Consumer is closed but attempting to consume " + t);
      }
      add(t);
    }
  }

  @Test
  public void testRegistration() {
    for (Registrar registrar :
        ServiceLoader.load(Registrar.class)) {
      if (registrar instanceof BeamFnDataWriteRunner.Registrar) {
        assertThat(registrar.getPTransformRunnerFactories(), IsMapContaining.hasKey(URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
