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
package org.apache.beam.fn.harness.data;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataBufferingOutboundObserver}. */
@RunWith(JUnit4.class)
public class BeamFnDataBufferingOutboundObserverTest {
  private static final int DEFAULT_BUFFER_LIMIT = 1_000_000;
  private static final LogicalEndpoint OUTPUT_LOCATION =
      LogicalEndpoint.of(
          "777L",
          Target.newBuilder()
              .setPrimitiveTransformReference("555L")
              .setName("Test")
              .build());
  private static final Coder<WindowedValue<byte[]>> CODER =
      LengthPrefixCoder.of(WindowedValue.getValueOnlyCoder(ByteArrayCoder.of()));

  @Test
  public void testWithDefaultBuffer() throws Exception {
    Collection<BeamFnApi.Elements> values = new ArrayList<>();
    AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    CloseableFnDataReceiver<WindowedValue<byte[]>> consumer =
        new BeamFnDataBufferingOutboundObserver<>(
        PipelineOptionsFactory.create(),
        OUTPUT_LOCATION,
        CODER,
        TestStreams.withOnNext(values::add)
            .withOnCompleted(() -> onCompletedWasCalled.set(true))
            .build());

    // Test that nothing is emitted till the default buffer size is surpassed.
    consumer.accept(valueInGlobalWindow(new byte[DEFAULT_BUFFER_LIMIT - 50]));
    assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    consumer.accept(valueInGlobalWindow(new byte[50]));
    assertEquals(
        messageWithData(new byte[DEFAULT_BUFFER_LIMIT - 50], new byte[50]),
        Iterables.get(values, 0));

    // Test that nothing is emitted till the default buffer size is surpassed after a reset
    consumer.accept(valueInGlobalWindow(new byte[DEFAULT_BUFFER_LIMIT - 50]));
    assertEquals(1, values.size());

    // Test that when we cross the buffer, we emit.
    consumer.accept(valueInGlobalWindow(new byte[50]));
    assertEquals(
        messageWithData(new byte[DEFAULT_BUFFER_LIMIT - 50], new byte[50]),
        Iterables.get(values, 1));

    // Test that when we close with an empty buffer we only have one end of stream
    consumer.close();
    assertEquals(messageWithData(),
        Iterables.get(values, 2));
  }

  @Test
  public void testExperimentConfiguresBufferLimit() throws Exception {
    Collection<BeamFnApi.Elements> values = new ArrayList<>();
    AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    CloseableFnDataReceiver<WindowedValue<byte[]>> consumer =
        new BeamFnDataBufferingOutboundObserver<>(
        PipelineOptionsFactory.fromArgs(
            new String[] { "--experiments=beam_fn_api_data_buffer_limit=100" }).create(),
        OUTPUT_LOCATION,
        CODER,
        TestStreams.withOnNext(values::add)
            .withOnCompleted(() -> onCompletedWasCalled.set(true))
            .build());

    // Test that nothing is emitted till the default buffer size is surpassed.
    consumer.accept(valueInGlobalWindow(new byte[51]));
    assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    consumer.accept(valueInGlobalWindow(new byte[49]));
    assertEquals(
        messageWithData(new byte[51], new byte[49]),
        Iterables.get(values, 0));

    // Test that when we close we empty the value, and then the stream terminator as part
    // of the same message
    consumer.accept(valueInGlobalWindow(new byte[1]));
    consumer.close();
    assertEquals(
        BeamFnApi.Elements.newBuilder(messageWithData(new byte[1]))
            .addData(BeamFnApi.Elements.Data.newBuilder()
                .setInstructionReference(OUTPUT_LOCATION.getInstructionId())
                .setTarget(OUTPUT_LOCATION.getTarget()))
            .build(),
        Iterables.get(values, 1));
  }

  private static BeamFnApi.Elements messageWithData(byte[] ... datum) throws IOException {
    ByteString.Output output = ByteString.newOutput();
    for (byte[] data : datum) {
      CODER.encode(valueInGlobalWindow(data), output);
    }
    return BeamFnApi.Elements.newBuilder()
        .addData(BeamFnApi.Elements.Data.newBuilder()
            .setInstructionReference(OUTPUT_LOCATION.getInstructionId())
            .setTarget(OUTPUT_LOCATION.getTarget())
            .setData(output.toByteString()))
        .build();
  }
}
