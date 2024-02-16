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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator.Receiver;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link BeamFnDataOutboundAggregator}. */
@RunWith(Parameterized.class)
public class BeamFnDataOutboundAggregatorTest {

  private static final LogicalEndpoint DATA_OUTPUT_LOCATION = LogicalEndpoint.data("777L", "555L");
  private static final LogicalEndpoint TIMER_OUTPUT_LOCATION =
      LogicalEndpoint.timer("999L", "333L", "111L");
  private static final Coder<byte[]> CODER = LengthPrefixCoder.of(ByteArrayCoder.of());

  @Parameters
  public static Collection<LogicalEndpoint> data() {
    return Arrays.asList(DATA_OUTPUT_LOCATION, TIMER_OUTPUT_LOCATION);
  }

  private final LogicalEndpoint endpoint;

  public BeamFnDataOutboundAggregatorTest(LogicalEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Test
  public void testWithDefaultBuffer() throws Exception {
    final List<Elements> values = new ArrayList<>();
    final AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    BeamFnDataOutboundAggregator aggregator =
        new BeamFnDataOutboundAggregator(
            PipelineOptionsFactory.create(),
            endpoint::getInstructionId,
            TestStreams.<Elements>withOnNext(values::add)
                .withOnCompleted(() -> onCompletedWasCalled.set(true))
                .build(),
            false);

    // Test that nothing is emitted till the default buffer size is surpassed.
    FnDataReceiver<byte[]> dataReceiver = registerOutputLocation(aggregator, endpoint, CODER);
    aggregator.start();
    dataReceiver.accept(new byte[BeamFnDataOutboundAggregator.DEFAULT_BUFFER_LIMIT_BYTES - 50]);
    MatcherAssert.assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    dataReceiver.accept(new byte[50]);
    Assert.assertEquals(
        messageWithData(
            new byte[BeamFnDataOutboundAggregator.DEFAULT_BUFFER_LIMIT_BYTES - 50], new byte[50]),
        values.get(0));

    // Test that nothing is emitted till the default buffer size is surpassed after a reset
    dataReceiver.accept(new byte[BeamFnDataOutboundAggregator.DEFAULT_BUFFER_LIMIT_BYTES - 50]);
    assertEquals(1, values.size());

    // Test that when we cross the buffer, we emit.
    dataReceiver.accept(new byte[50]);
    Assert.assertEquals(
        messageWithData(
            new byte[BeamFnDataOutboundAggregator.DEFAULT_BUFFER_LIMIT_BYTES - 50], new byte[50]),
        values.get(1));

    // Test that when we close with an empty buffer we only have one end of stream
    aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
    Assert.assertEquals(endMessage(), values.get(2));

    // Test that we can close twice.
    aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
    Assert.assertEquals(endMessage(), values.get(2));
  }

  @Test
  public void testConfiguredBufferLimit() throws Exception {
    List<BeamFnApi.Elements> values = new ArrayList<>();
    AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_size_limit=100"));
    BeamFnDataOutboundAggregator aggregator =
        new BeamFnDataOutboundAggregator(
            options,
            endpoint::getInstructionId,
            TestStreams.<Elements>withOnNext(values::add)
                .withOnCompleted(() -> onCompletedWasCalled.set(true))
                .build(),
            false);
    // Test that nothing is emitted till the default buffer size is surpassed.
    FnDataReceiver<byte[]> dataReceiver = registerOutputLocation(aggregator, endpoint, CODER);
    aggregator.start();
    dataReceiver.accept(new byte[51]);
    MatcherAssert.assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    dataReceiver.accept(new byte[49]);
    Assert.assertEquals(messageWithData(new byte[51], new byte[49]), values.get(0));
    Receiver<?> receiver;
    if (endpoint.isTimer()) {
      receiver = Iterables.getOnlyElement(aggregator.outputTimersReceivers.values());
    } else {
      receiver = Iterables.getOnlyElement(aggregator.outputDataReceivers.values());
    }
    assertEquals(0L, receiver.bufferedSize());
    assertEquals(102L, receiver.getByteCount());
    assertEquals(2L, receiver.getElementCount());

    // Test that when we close we empty the value, and then send the stream terminator as part
    // of the same message
    dataReceiver.accept(new byte[1]);
    aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
    // Test that receiver stats have been reset after
    // sendOrCollectBufferedDataAndFinishOutboundStreams.
    assertEquals(0L, receiver.bufferedSize());
    assertEquals(0L, receiver.getByteCount());
    assertEquals(0L, receiver.getElementCount());

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
    Assert.assertEquals(builder.build(), values.get(1));
  }

  @Test
  public void testConfiguredTimeLimit() throws Exception {
    List<Elements> values = new ArrayList<>();
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_time_limit_ms=1"));
    final CountDownLatch waitForFlush = new CountDownLatch(1);
    BeamFnDataOutboundAggregator aggregator =
        new BeamFnDataOutboundAggregator(
            options,
            endpoint::getInstructionId,
            TestStreams.withOnNext(
                    (Consumer<Elements>)
                        e -> {
                          values.add(e);
                          waitForFlush.countDown();
                        })
                .build(),
            false);

    // Test that it emits when time passed the time limit
    FnDataReceiver<byte[]> dataReceiver = registerOutputLocation(aggregator, endpoint, CODER);
    aggregator.start();
    dataReceiver.accept(new byte[1]);
    waitForFlush.await(); // wait the flush thread to flush the buffer
    Assert.assertEquals(messageWithData(new byte[1]), values.get(0));
  }

  @Test
  public void testConfiguredTimeLimitExceptionPropagation() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_time_limit_ms=1"));
    BeamFnDataOutboundAggregator aggregator =
        new BeamFnDataOutboundAggregator(
            options,
            endpoint::getInstructionId,
            TestStreams.withOnNext(
                    (Consumer<Elements>)
                        e -> {
                          throw new RuntimeException("");
                        })
                .build(),
            false);

    // Test that it emits when time passed the time limit
    FnDataReceiver<byte[]> dataReceiver = registerOutputLocation(aggregator, endpoint, CODER);
    aggregator.start();
    dataReceiver.accept(new byte[1]);
    // wait the flush thread to flush the buffer
    while (!aggregator.flushFuture.isDone()) {
      Thread.sleep(1);
    }
    try {
      // Test that the exception caught in the flush thread is propagated to
      // the main thread when processing the next element
      dataReceiver.accept(new byte[1]);
      fail();
    } catch (Exception e) {
      // expected
    }

    aggregator =
        new BeamFnDataOutboundAggregator(
            options,
            endpoint::getInstructionId,
            TestStreams.withOnNext(
                    (Consumer<Elements>)
                        e -> {
                          throw new RuntimeException("");
                        })
                .build(),
            false);
    dataReceiver = registerOutputLocation(aggregator, endpoint, CODER);
    aggregator.start();
    dataReceiver.accept(new byte[1]);
    // wait the flush thread to flush the buffer
    while (!aggregator.flushFuture.isDone()) {
      Thread.sleep(1);
    }
    try {
      // Test that the exception caught in the flush thread is propagated to
      // the main thread when closing
      aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
      fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testConfiguredBufferLimitMultipleEndpoints() throws Exception {
    List<BeamFnApi.Elements> values = new ArrayList<>();
    AtomicBoolean onCompletedWasCalled = new AtomicBoolean();
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_size_limit=100"));
    BeamFnDataOutboundAggregator aggregator =
        new BeamFnDataOutboundAggregator(
            options,
            endpoint::getInstructionId,
            TestStreams.<Elements>withOnNext(values::add)
                .withOnCompleted(() -> onCompletedWasCalled.set(true))
                .build(),
            false);
    // Test that nothing is emitted till the default buffer size is surpassed.
    LogicalEndpoint additionalEndpoint =
        LogicalEndpoint.data(
            endpoint.getInstructionId(), "additional:" + endpoint.getTransformId());
    FnDataReceiver<byte[]> dataReceiver = registerOutputLocation(aggregator, endpoint, CODER);
    FnDataReceiver<byte[]> additionalDataReceiver =
        registerOutputLocation(aggregator, additionalEndpoint, CODER);
    aggregator.start();
    dataReceiver.accept(new byte[51]);
    MatcherAssert.assertThat(values, empty());

    // Test that when we cross the buffer, we emit.
    additionalDataReceiver.accept(new byte[49]);
    checkEqualInAnyOrder(
        messageWithDataBuilder(new byte[51])
            .mergeFrom(messageWithDataBuilder(additionalEndpoint, new byte[49]).build())
            .build(),
        values.get(0));

    // Test that when we close we empty the value, and then the stream terminator as part
    // of the same message
    dataReceiver.accept(new byte[1]);
    aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();

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
    builder.addData(
        BeamFnApi.Elements.Data.newBuilder()
            .setInstructionId(additionalEndpoint.getInstructionId())
            .setTransformId(additionalEndpoint.getTransformId())
            .setIsLast(true));
    checkEqualInAnyOrder(builder.build(), values.get(1));
  }

  private void checkEqualInAnyOrder(Elements first, Elements second) {
    MatcherAssert.assertThat(
        first.getDataList(), Matchers.containsInAnyOrder(second.getDataList().toArray()));
    MatcherAssert.assertThat(
        first.getTimersList(), Matchers.containsInAnyOrder(second.getTimersList().toArray()));
  }

  BeamFnApi.Elements.Builder messageWithDataBuilder(byte[]... datum) throws IOException {
    return messageWithDataBuilder(endpoint, datum);
  }

  BeamFnApi.Elements.Builder messageWithDataBuilder(LogicalEndpoint endpoint, byte[]... datum)
      throws IOException {
    ByteStringOutputStream output = new ByteStringOutputStream();
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

  BeamFnApi.Elements endMessage() throws IOException {
    BeamFnApi.Elements.Builder builder = messageWithDataBuilder();
    if (endpoint.isTimer()) {
      builder.getTimersBuilder(0).setIsLast(true);
    } else {
      builder.getDataBuilder(0).setIsLast(true);
    }
    return builder.build();
  }

  // Convenience method for unit tests.
  <T> FnDataReceiver<T> registerOutputLocation(
      BeamFnDataOutboundAggregator aggregator, LogicalEndpoint endpoint, Coder<T> coder) {
    if (endpoint.isTimer()) {
      return aggregator.registerOutputTimersLocation(
          endpoint.getTransformId(), endpoint.getTimerFamilyId(), coder);
    } else {
      return aggregator.registerOutputDataLocation(endpoint.getTransformId(), coder);
    }
  }
}
