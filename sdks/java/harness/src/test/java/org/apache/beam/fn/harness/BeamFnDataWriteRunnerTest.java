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

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
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

  private static final String ELEM_CODER_ID = "string-coder-id";
  private static final Coder<String> ELEM_CODER = StringUtf8Coder.of();
  private static final String WIRE_CODER_ID = "windowed-string-coder-id";
  private static final Coder<WindowedValue<String>> WIRE_CODER =
      WindowedValue.getFullCoder(ELEM_CODER, GlobalWindow.Coder.INSTANCE);
  private static final RunnerApi.Coder WIRE_CODER_SPEC;
  private static final RunnerApi.Components COMPONENTS;

  private static final BeamFnApi.RemoteGrpcPort PORT_SPEC =
      BeamFnApi.RemoteGrpcPort.newBuilder()
          .setApiServiceDescriptor(Endpoints.ApiServiceDescriptor.getDefaultInstance())
          .setCoderId(WIRE_CODER_ID)
          .build();

  static {
    try {
      MessageWithComponents coderAndComponents = CoderTranslation.toProto(WIRE_CODER);
      WIRE_CODER_SPEC = coderAndComponents.getCoder();
      COMPONENTS =
          coderAndComponents
              .getComponents()
              .toBuilder()
              .putCoders(WIRE_CODER_ID, WIRE_CODER_SPEC)
              .putCoders(ELEM_CODER_ID, CoderTranslation.toProto(ELEM_CODER).getCoder())
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

    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    String localInputId = "inputPC";
    RunnerApi.PTransform pTransform =
        RemoteGrpcPortWrite.writeToPort(localInputId, PORT_SPEC).toPTransform();

    new BeamFnDataWriteRunner.Factory<String>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        mockBeamFnDataClient,
        null /* beamFnStateClient */,
        "ptransformId",
        pTransform,
        Suppliers.ofInstance(bundleId)::get,
        ImmutableMap.of(localInputId,
            RunnerApi.PCollection.newBuilder().setCoderId(ELEM_CODER_ID).build()),
        COMPONENTS.getCodersMap(),
        COMPONENTS.getWindowingStrategiesMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    verifyZeroInteractions(mockBeamFnDataClient);

    List<WindowedValue<String>> outputValues = new ArrayList<>();
    AtomicBoolean wasCloseCalled = new AtomicBoolean();
    CloseableFnDataReceiver<WindowedValue<String>> outputConsumer =
        new CloseableFnDataReceiver<WindowedValue<String>>() {
          @Override
          public void close() throws Exception {
            wasCloseCalled.set(true);
          }

          @Override
          public void accept(WindowedValue<String> t) throws Exception {
            outputValues.add(t);
          }
        };

    when(mockBeamFnDataClient.send(
        any(),
        any(),
        Matchers.<Coder<WindowedValue<String>>>any())).thenReturn(outputConsumer);
    Iterables.getOnlyElement(startFunctions).run();
    verify(mockBeamFnDataClient).send(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(
            LogicalEndpoint.of(
                bundleId,
                BeamFnApi.Target.newBuilder()
                    .setPrimitiveTransformReference("ptransformId")
                    // The local input name is arbitrary, so use whatever the
                    // RemoteGrpcPortWrite uses
                    .setName(Iterables.getOnlyElement(pTransform.getInputsMap().keySet()))
                    .build())),
        eq(WIRE_CODER));

    assertThat(consumers.keySet(), containsInAnyOrder(localInputId));
    Iterables.getOnlyElement(consumers.get(localInputId)).accept(valueInGlobalWindow("TestValue"));
    assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
    outputValues.clear();

    assertFalse(wasCloseCalled.get());
    Iterables.getOnlyElement(finishFunctions).run();
    assertTrue(wasCloseCalled.get());

    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  @Test
  public void testReuseForMultipleBundles() throws Exception {
    RecordingReceiver<WindowedValue<String>> valuesA = new RecordingReceiver<>();
    RecordingReceiver<WindowedValue<String>> valuesB = new RecordingReceiver<>();
    when(mockBeamFnDataClient.send(
        any(),
        any(),
        Matchers.<Coder<WindowedValue<String>>>any())).thenReturn(valuesA).thenReturn(valuesB);
    AtomicReference<String> bundleId = new AtomicReference<>("0");
    BeamFnDataWriteRunner<String> writeRunner = new BeamFnDataWriteRunner<>(
        RemoteGrpcPortWrite.writeToPort("myWrite", PORT_SPEC).toPTransform(),
        bundleId::get,
        OUTPUT_TARGET, WIRE_CODER_SPEC,
        COMPONENTS.getCodersMap(),
        mockBeamFnDataClient);

    // Process for bundle id 0
    writeRunner.registerForOutput();

    verify(mockBeamFnDataClient).send(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(LogicalEndpoint.of(bundleId.get(), OUTPUT_TARGET)),
        eq(WIRE_CODER));

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

    verify(mockBeamFnDataClient).send(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(LogicalEndpoint.of(bundleId.get(), OUTPUT_TARGET)),
        eq(WIRE_CODER));

    writeRunner.consume(valueInGlobalWindow("GHI"));
    writeRunner.consume(valueInGlobalWindow("JKL"));
    writeRunner.close();

    assertTrue(valuesB.closed);
    assertThat(valuesB, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));
    verifyNoMoreInteractions(mockBeamFnDataClient);
  }

  private static class RecordingReceiver<T> extends ArrayList<T>
      implements CloseableFnDataReceiver<T> {
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
        assertThat(
            registrar.getPTransformRunnerFactories(),
            IsMapContaining.hasKey(RemoteGrpcPortWrite.URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
