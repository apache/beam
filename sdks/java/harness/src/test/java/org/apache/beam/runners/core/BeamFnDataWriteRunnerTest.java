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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
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
  private static final BeamFnApi.FunctionSpec FUNCTION_SPEC = BeamFnApi.FunctionSpec.newBuilder()
      .setData(Any.pack(PORT_SPEC)).build();
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final BeamFnApi.Coder CODER_SPEC;
  static {
    try {
      CODER_SPEC = BeamFnApi.Coder.newBuilder().setFunctionSpec(BeamFnApi.FunctionSpec.newBuilder()
      .setData(Any.pack(BytesValue.newBuilder().setValue(ByteString.copyFrom(
          OBJECT_MAPPER.writeValueAsBytes(CODER.asCloudObject()))).build())))
      .build();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
  private static final BeamFnApi.Target OUTPUT_TARGET = BeamFnApi.Target.newBuilder()
      .setPrimitiveTransformReference("1")
      .setName("out")
      .build();

  @Mock private BeamFnDataClient mockBeamFnDataClientFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testReuseForMultipleBundles() throws Exception {
    RecordingConsumer<WindowedValue<String>> valuesA = new RecordingConsumer<>();
    RecordingConsumer<WindowedValue<String>> valuesB = new RecordingConsumer<>();
    when(mockBeamFnDataClientFactory.forOutboundConsumer(
        any(),
        any(),
        Matchers.<Coder<WindowedValue<String>>>any())).thenReturn(valuesA).thenReturn(valuesB);
    AtomicReference<String> bundleId = new AtomicReference<>("0");
    BeamFnDataWriteRunner<String> writeRunner = new BeamFnDataWriteRunner<>(
        FUNCTION_SPEC,
        bundleId::get,
        OUTPUT_TARGET,
        CODER_SPEC,
        mockBeamFnDataClientFactory);

    // Process for bundle id 0
    writeRunner.registerForOutput();

    verify(mockBeamFnDataClientFactory).forOutboundConsumer(
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

    verify(mockBeamFnDataClientFactory).forOutboundConsumer(
        eq(PORT_SPEC.getApiServiceDescriptor()),
        eq(KV.of(bundleId.get(), OUTPUT_TARGET)),
        eq(CODER));

    writeRunner.consume(valueInGlobalWindow("GHI"));
    writeRunner.consume(valueInGlobalWindow("JKL"));
    writeRunner.close();

    assertTrue(valuesB.closed);
    assertThat(valuesB, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));
    verifyNoMoreInteractions(mockBeamFnDataClientFactory);
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
}
