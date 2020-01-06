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

import static org.apache.beam.sdk.fn.data.BeamFnDataSizeBasedBufferingOutboundObserverTest.messageWithData;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataTimeBasedBufferingOutboundObserver}. */
@RunWith(JUnit4.class)
public class BeamFnDataTimeBasedBufferingOutboundObserverTest {
  private static final LogicalEndpoint OUTPUT_LOCATION = LogicalEndpoint.of("777L", "555L");
  private static final Coder<WindowedValue<byte[]>> CODER =
      LengthPrefixCoder.of(WindowedValue.getValueOnlyCoder(ByteArrayCoder.of()));

  @Test
  public void testConfiguredTimeLimit() throws Exception {
    Collection<Elements> values = new ArrayList<>();
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_time_limit_ms=1"));
    final CountDownLatch waitForFlush = new CountDownLatch(1);
    CloseableFnDataReceiver<WindowedValue<byte[]>> consumer =
        BeamFnDataBufferingOutboundObserver.forLocation(
            options,
            OUTPUT_LOCATION,
            CODER,
            TestStreams.withOnNext(
                    (Consumer<Elements>)
                        e -> {
                          values.add(e);
                          waitForFlush.countDown();
                        })
                .build());

    // Test that it emits when time passed the time limit
    consumer.accept(valueInGlobalWindow(new byte[1]));
    waitForFlush.await(); // wait the flush thread to flush the buffer
    assertEquals(messageWithData(new byte[1]), Iterables.get(values, 0));
  }

  @Test
  public void testConfiguredTimeLimitExceptionPropagation() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(ExperimentalOptions.class)
        .setExperiments(Arrays.asList("data_buffer_time_limit_ms=1"));
    BeamFnDataTimeBasedBufferingOutboundObserver<WindowedValue<byte[]>> consumer =
        (BeamFnDataTimeBasedBufferingOutboundObserver<WindowedValue<byte[]>>)
            BeamFnDataBufferingOutboundObserver.forLocation(
                options,
                OUTPUT_LOCATION,
                CODER,
                TestStreams.withOnNext(
                        (Consumer<Elements>)
                            e -> {
                              throw new RuntimeException("");
                            })
                    .build());

    // Test that it emits when time passed the time limit
    consumer.accept(valueInGlobalWindow(new byte[1]));
    // wait the flush thread to flush the buffer
    while (!consumer.flushFuture.isDone()) {
      Thread.sleep(1);
    }
    try {
      // Test that the exception caught in the flush thread is propagate to
      // the main thread when processing the next element
      consumer.accept(valueInGlobalWindow(new byte[1]));
      fail();
    } catch (Exception e) {
      // expected
    }

    consumer =
        (BeamFnDataTimeBasedBufferingOutboundObserver<WindowedValue<byte[]>>)
            BeamFnDataBufferingOutboundObserver.forLocation(
                options,
                OUTPUT_LOCATION,
                CODER,
                TestStreams.withOnNext(
                        (Consumer<Elements>)
                            e -> {
                              throw new RuntimeException("");
                            })
                    .build());
    consumer.accept(valueInGlobalWindow(new byte[1]));
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
}
