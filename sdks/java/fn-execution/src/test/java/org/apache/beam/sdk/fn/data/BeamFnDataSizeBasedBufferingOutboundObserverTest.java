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

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link BeamFnDataSizeBasedBufferingOutboundObserver}. */
@RunWith(Parameterized.class)
public class BeamFnDataSizeBasedBufferingOutboundObserverTest {
  private static final LogicalEndpoint DATA_OUTPUT_LOCATION = LogicalEndpoint.data("777L", "555L");
  private static final LogicalEndpoint TIMER_OUTPUT_LOCATION =
      LogicalEndpoint.timer("999L", "333L", "111L");
  private static final Coder<byte[]> CODER = LengthPrefixCoder.of(ByteArrayCoder.of());

  @Parameters
  public static Collection<LogicalEndpoint> data() {
    return Arrays.asList(DATA_OUTPUT_LOCATION, TIMER_OUTPUT_LOCATION);
  }

  private final LogicalEndpoint endpoint;

  public BeamFnDataSizeBasedBufferingOutboundObserverTest(LogicalEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Test
  public void testWithDefaultBuffer() throws Exception {
    final List<Elements> values = new ArrayList<>();
    final AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    CloseableFnDataReceiver<byte[]> consumer =
        BeamFnDataBufferingOutboundObserver.forLocation(
            PipelineOptionsFactory.create(),
            endpoint,
            CODER,
            TestStreams.<Elements>withOnNext(values::add)
                .withOnCompleted(() -> onCompletedWasCalled.set(true))
                .build());

    // Test that the time-based flush is disabled by default.
    assertFalse(consumer instanceof BeamFnDataTimeBasedBufferingOutboundObserver);

    // Test that nothing is emitted till the default buffer size is surpassed.
    consumer.accept(
        new byte[BeamFnDataSizeBasedBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50]);
    assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    consumer.accept(new byte[50]);
    assertEquals(
        messageWithData(
            new byte[BeamFnDataSizeBasedBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50],
            new byte[50]),
        values.get(0));

    // Test that nothing is emitted till the default buffer size is surpassed after a reset
    consumer.accept(
        new byte[BeamFnDataSizeBasedBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50]);
    assertEquals(1, values.size());

    // Test that when we cross the buffer, we emit.
    consumer.accept(new byte[50]);
    assertEquals(
        messageWithData(
            new byte[BeamFnDataSizeBasedBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50],
            new byte[50]),
        values.get(1));

    // Test that when we close with an empty buffer we only have one end of stream
    consumer.close();

    assertEquals(endMessageWithData(), values.get(2));

    // Test that we can't write to a closed stream.
    try {
      consumer.accept(
          new byte[BeamFnDataSizeBasedBufferingOutboundObserver.DEFAULT_BUFFER_LIMIT_BYTES - 50]);
      fail("Writing after close should be prohibited.");
    } catch (IllegalStateException exn) {
      // expected
    }

    // Test that we can close a stream twice.
    consumer.close();
  }

  @Test
  public void testConfiguredBufferLimit() throws Exception {
    List<BeamFnApi.Elements> values = new ArrayList<>();
    AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_size_limit=100"));
    CloseableFnDataReceiver<byte[]> consumer =
        BeamFnDataBufferingOutboundObserver.forLocation(
            options,
            endpoint,
            CODER,
            TestStreams.<Elements>withOnNext(values::add)
                .withOnCompleted(() -> onCompletedWasCalled.set(true))
                .build());

    // Test that nothing is emitted till the default buffer size is surpassed.
    consumer.accept(new byte[51]);
    assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    consumer.accept(new byte[49]);
    assertEquals(messageWithData(new byte[51], new byte[49]), values.get(0));

    // Test that when we close we empty the value, and then the stream terminator as part
    // of the same message
    consumer.accept(new byte[1]);
    consumer.close();

    BeamFnApi.Elements.Builder builder = messageWithDataBuilder(new byte[1]);
    if (endpoint.isTimer()) {
      builder.addTimers(
          BeamFnApi.Elements.Timers.newBuilder()
              .setInstructionId(endpoint.getInstructionId())
              .setTransformId(endpoint.getTransformId())
              .setTimerFamilyId(endpoint.getTimerFamilyId())
              .setIsLast(true));
    } else {
      builder.addData(
          BeamFnApi.Elements.Data.newBuilder()
              .setInstructionId(endpoint.getInstructionId())
              .setTransformId(endpoint.getTransformId())
              .setIsLast(true));
    }
    assertEquals(builder.build(), values.get(1));
  }

  BeamFnApi.Elements.Builder messageWithDataBuilder(byte[]... datum) throws IOException {
    ByteString.Output output = ByteString.newOutput();
    for (byte[] data : datum) {
      CODER.encode(data, output);
    }
    if (endpoint.isTimer()) {
      return BeamFnApi.Elements.newBuilder()
          .addTimers(
              BeamFnApi.Elements.Timers.newBuilder()
                  .setInstructionId(endpoint.getInstructionId())
                  .setTransformId(endpoint.getTransformId())
                  .setTimerFamilyId(endpoint.getTimerFamilyId())
                  .setTimers(output.toByteString()));
    } else {
      return BeamFnApi.Elements.newBuilder()
          .addData(
              BeamFnApi.Elements.Data.newBuilder()
                  .setInstructionId(endpoint.getInstructionId())
                  .setTransformId(endpoint.getTransformId())
                  .setData(output.toByteString()));
    }
  }

  BeamFnApi.Elements messageWithData(byte[]... datum) throws IOException {
    return messageWithDataBuilder(datum).build();
  }

  BeamFnApi.Elements endMessageWithData() throws IOException {
    BeamFnApi.Elements.Builder builder = messageWithDataBuilder();
    if (endpoint.isTimer()) {
      builder.getTimersBuilder(0).setIsLast(true);
    } else {
      builder.getDataBuilder(0).setIsLast(true);
    }
    return builder.build();
  }
}
