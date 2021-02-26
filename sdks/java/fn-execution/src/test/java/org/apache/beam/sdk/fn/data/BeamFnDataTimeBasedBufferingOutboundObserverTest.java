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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
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

/** Tests for {@link BeamFnDataTimeBasedBufferingOutboundObserver}. */
@RunWith(Parameterized.class)
public class BeamFnDataTimeBasedBufferingOutboundObserverTest {
  private static final LogicalEndpoint DATA_OUTPUT_LOCATION = LogicalEndpoint.data("777L", "555L");
  private static final LogicalEndpoint TIMER_OUTPUT_LOCATION =
      LogicalEndpoint.timer("999L", "333L", "111L");
  private static final Coder<byte[]> CODER = LengthPrefixCoder.of(ByteArrayCoder.of());

  @Parameters
  public static Collection<LogicalEndpoint> data() {
    return Arrays.asList(DATA_OUTPUT_LOCATION, TIMER_OUTPUT_LOCATION);
  }

  private final LogicalEndpoint endpoint;

  public BeamFnDataTimeBasedBufferingOutboundObserverTest(LogicalEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Test
  public void testConfiguredTimeLimit() throws Exception {
    List<Elements> values = new ArrayList<>();
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_time_limit_ms=1"));
    final CountDownLatch waitForFlush = new CountDownLatch(1);
    CloseableFnDataReceiver<byte[]> consumer =
        BeamFnDataBufferingOutboundObserver.forLocation(
            options,
            endpoint,
            CODER,
            TestStreams.withOnNext(
                    (Consumer<Elements>)
                        e -> {
                          values.add(e);
                          waitForFlush.countDown();
                        })
                .build());

    // Test that it emits when time passed the time limit
    consumer.accept(new byte[1]);
    waitForFlush.await(); // wait the flush thread to flush the buffer
    assertEquals(messageWithData(new byte[1]), values.get(0));
  }

  @Test
  public void testConfiguredTimeLimitExceptionPropagation() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_time_limit_ms=1"));
    BeamFnDataTimeBasedBufferingOutboundObserver<byte[]> consumer =
        (BeamFnDataTimeBasedBufferingOutboundObserver<byte[]>)
            BeamFnDataBufferingOutboundObserver.forLocation(
                options,
                endpoint,
                CODER,
                TestStreams.withOnNext(
                        (Consumer<Elements>)
                            e -> {
                              throw new RuntimeException("");
                            })
                    .build());

    // Test that it emits when time passed the time limit
    consumer.accept(new byte[1]);
    // wait the flush thread to flush the buffer
    while (!consumer.flushFuture.isDone()) {
      Thread.sleep(1);
    }
    try {
      // Test that the exception caught in the flush thread is propagate to
      // the main thread when processing the next element
      consumer.accept(new byte[1]);
      fail();
    } catch (Exception e) {
      // expected
    }

    consumer =
        (BeamFnDataTimeBasedBufferingOutboundObserver<byte[]>)
            BeamFnDataBufferingOutboundObserver.forLocation(
                options,
                endpoint,
                CODER,
                TestStreams.withOnNext(
                        (Consumer<Elements>)
                            e -> {
                              throw new RuntimeException("");
                            })
                    .build());
    consumer.accept(new byte[1]);
    // wait the flush thread to flush the buffer
    while (!consumer.flushFuture.isDone()) {
      Thread.sleep(1);
    }
    try {
      // Test that the exception caught in the flush thread is propagate to
      // the main thread when closing
      consumer.close();
      fail();
    } catch (Exception e) {
      // expected
    }
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
}
