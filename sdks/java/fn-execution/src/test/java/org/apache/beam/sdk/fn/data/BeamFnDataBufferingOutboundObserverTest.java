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
package org.apache.beam.sdk.fn.data;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataBufferingOutboundObserver}. */
@RunWith(JUnit4.class)
public class BeamFnDataBufferingOutboundObserverTest {
  private static final LogicalEndpoint OUTPUT_LOCATION = LogicalEndpoint.of("777L", "555L");
  private static final Coder<WindowedValue<byte[]>> CODER =
      LengthPrefixCoder.of(WindowedValue.getValueOnlyCoder(ByteArrayCoder.of()));

  @Test
  public void testWithDefaultBuffer() throws Exception {
    final Collection<BeamFnApi.Elements> values = new ArrayList<>();
    final AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    CloseableFnDataReceiver<WindowedValue<byte[]>> consumer =
        BeamFnDataBufferingOutboundObserver.forLocation(
            OUTPUT_LOCATION,
            CODER,
            TestStreams.withOnNext(addToValuesConsumer(values))
                .withOnCompleted(setBooleanToTrue(onCompletedWasCalled))
                .build());

    // Test that nothing is emitted till the default buffer size is surpassed.
    consumer.accept(
        valueInGlobalWindow(
            new byte[BeamFnDataBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50]));
    assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    consumer.accept(valueInGlobalWindow(new byte[50]));
    assertEquals(
        messageWithData(
            new byte[BeamFnDataBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50],
            new byte[50]),
        Iterables.get(values, 0));

    // Test that nothing is emitted till the default buffer size is surpassed after a reset
    consumer.accept(
        valueInGlobalWindow(
            new byte[BeamFnDataBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50]));
    assertEquals(1, values.size());

    // Test that when we cross the buffer, we emit.
    consumer.accept(valueInGlobalWindow(new byte[50]));
    assertEquals(
        messageWithData(
            new byte[BeamFnDataBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50],
            new byte[50]),
        Iterables.get(values, 1));

    // Test that when we close with an empty buffer we only have one end of stream
    consumer.close();
    assertEquals(messageWithData(), Iterables.get(values, 2));

    // Test that we can't write to a closed stream.
    try {
      consumer.accept(
          valueInGlobalWindow(
              new byte[BeamFnDataBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50]));
      fail("Writing after close should be prohibited.");
    } catch (IllegalStateException exn) {
      // expected
    }

    // Test that we can't close a stream twice.
    try {
      consumer.close();
      fail("Closing twice should be prohibited.");
    } catch (IllegalStateException exn) {
      // expected
    }
  }

  @Test
  public void testConfiguredBufferLimit() throws Exception {
    Collection<BeamFnApi.Elements> values = new ArrayList<>();
    AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    CloseableFnDataReceiver<WindowedValue<byte[]>> consumer =
        BeamFnDataBufferingOutboundObserver.forLocationWithBufferLimit(
            100,
            OUTPUT_LOCATION,
            CODER,
            TestStreams.withOnNext(addToValuesConsumer(values))
                .withOnCompleted(setBooleanToTrue(onCompletedWasCalled))
                .build());

    // Test that nothing is emitted till the default buffer size is surpassed.
    consumer.accept(valueInGlobalWindow(new byte[51]));
    assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    consumer.accept(valueInGlobalWindow(new byte[49]));
    assertEquals(messageWithData(new byte[51], new byte[49]), Iterables.get(values, 0));

    // Test that when we close we empty the value, and then the stream terminator as part
    // of the same message
    consumer.accept(valueInGlobalWindow(new byte[1]));
    consumer.close();
    assertEquals(
        BeamFnApi.Elements.newBuilder(messageWithData(new byte[1]))
            .addData(
                BeamFnApi.Elements.Data.newBuilder()
                    .setInstructionId(OUTPUT_LOCATION.getInstructionId())
                    .setTransformId(OUTPUT_LOCATION.getTransformId()))
            .build(),
        Iterables.get(values, 1));
  }

  private static BeamFnApi.Elements messageWithData(byte[]... datum) throws IOException {
    ByteString.Output output = ByteString.newOutput();
    for (byte[] data : datum) {
      CODER.encode(valueInGlobalWindow(data), output);
    }
    return BeamFnApi.Elements.newBuilder()
        .addData(
            BeamFnApi.Elements.Data.newBuilder()
                .setInstructionId(OUTPUT_LOCATION.getInstructionId())
                .setTransformId(OUTPUT_LOCATION.getTransformId())
                .setData(output.toByteString()))
        .build();
  }

  private Consumer<Elements> addToValuesConsumer(final Collection<Elements> values) {
    return values::add;
  }

  private Runnable setBooleanToTrue(final AtomicBoolean onCompletedWasCalled) {
    return () -> onCompletedWasCalled.set(true);
  }
}
