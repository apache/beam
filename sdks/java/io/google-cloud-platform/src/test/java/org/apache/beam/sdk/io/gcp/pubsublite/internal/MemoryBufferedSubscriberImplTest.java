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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Spy;

@RunWith(JUnit4.class)
public class MemoryBufferedSubscriberImplTest {
  private static final long MAX_MEMORY = 1024;

  @Rule public Timeout globalTimeout = Timeout.seconds(30);

  abstract static class FakeSubscriber extends FakeApiService implements Subscriber {}

  @Spy FakeSubscriber subscriber;
  @Mock Function<Consumer<List<SequencedMessage>>, Subscriber> subscriberFactory;
  @Mock MemoryLimiter limiter;
  @Mock MemoryLimiter.Block block;

  MemoryBufferedSubscriber bufferedSubscriber;

  Consumer<List<SequencedMessage>> consumer;

  private static SequencedMessage messageWithSize(long size) {
    return SequencedMessage.newBuilder().setSizeBytes(size).build();
  }

  @Before
  public void setUp() {
    initMocks(this);
    doAnswer(
            args -> {
              consumer = args.getArgument(0);
              return subscriber;
            })
        .when(subscriberFactory)
        .apply(any());
    doReturn(1L).when(limiter).minBlockSize();
    doReturn(MAX_MEMORY).when(limiter).maxBlockSize();
    checkNotNull(block);
    checkNotNull(limiter);
    doReturn(block).when(limiter).claim(anyLong());
    doReturn(MAX_MEMORY).when(block).claimed();
    bufferedSubscriber =
        new MemoryBufferedSubscriberImpl(
            example(Partition.class), example(Offset.class), limiter, subscriberFactory);
    checkNotNull(consumer);
    bufferedSubscriber.startAsync().awaitRunning();
    verify(subscriber).startAsync();
    assertTrue(subscriber.isRunning());
  }

  @Test
  public void underlyingFailureFails() {
    subscriber.fail(new RuntimeException("bad"));
    assertThrows(Exception.class, subscriber::awaitTerminated);
  }

  @Test
  public void rebufferReducesToOutstandingWhenLittleData() {
    consumer.accept(ImmutableList.of(messageWithSize(MAX_MEMORY / 4)));
    bufferedSubscriber.pop();
    bufferedSubscriber.rebuffer();
    verify(block).close();
    verify(limiter).claim(3 * MAX_MEMORY / 4);
  }

  @Test
  public void rebufferCannotGoBelowMin() {
    long minBlock = MAX_MEMORY * 4 / 5;
    doReturn(minBlock).when(limiter).minBlockSize();
    for (int i = 0; i < 1000; ++i) {
      // Rebuffer many times with no data to bring down the target value
      bufferedSubscriber.rebuffer();
    }
    reset(limiter);
    doReturn(minBlock).when(limiter).minBlockSize();
    doReturn(block).when(limiter).claim(anyLong());
    // Deliver enough data that 3 * minBlock / 4 is outstanding, buffer is allowed to and will
    // shrink except it is limited by min block size.
    consumer.accept(ImmutableList.of(messageWithSize(2 * MAX_MEMORY / 5)));
    bufferedSubscriber.pop();
    bufferedSubscriber.rebuffer();
    verify(limiter).claim(minBlock);
  }

  @Test
  public void rebufferStaysSameOnHalfDelivered() {
    consumer.accept(ImmutableList.of(messageWithSize(MAX_MEMORY / 4)));
    bufferedSubscriber.pop();
    bufferedSubscriber.rebuffer();
    verify(limiter).claim(3 * MAX_MEMORY / 4);
    consumer.accept(ImmutableList.of(messageWithSize(3 * MAX_MEMORY / 8)));
    bufferedSubscriber.pop();
    bufferedSubscriber.rebuffer();
    verify(limiter).claim(3 * MAX_MEMORY / 4);
  }

  @Test
  public void rebufferGrowsOnMoreDelivered() {
    consumer.accept(ImmutableList.of(messageWithSize(MAX_MEMORY / 4)));
    bufferedSubscriber.pop();
    bufferedSubscriber.rebuffer();
    verify(limiter).claim(3 * MAX_MEMORY / 4);
    consumer.accept(
        ImmutableList.of(messageWithSize(MAX_MEMORY / 2), messageWithSize(MAX_MEMORY / 8)));
    bufferedSubscriber.rebuffer();
    verify(limiter, times(2)).claim(MAX_MEMORY); // once in setup
  }

  @Test
  public void dataAvailableToCaller() {
    SequencedMessage message1 =
        SequencedMessage.newBuilder()
            .setCursor(Cursor.newBuilder().setOffset(example(Offset.class).value() + 10))
            .setSizeBytes(1)
            .build();
    SequencedMessage message2 =
        SequencedMessage.newBuilder()
            .setCursor(Cursor.newBuilder().setOffset(example(Offset.class).value() + 20))
            .setSizeBytes(1)
            .build();
    assertFalse(bufferedSubscriber.peek().isPresent());
    consumer.accept(ImmutableList.of(message1, message2));
    assertEquals(bufferedSubscriber.fetchOffset(), example(Offset.class));
    assertEquals(bufferedSubscriber.peek().get(), message1);
    bufferedSubscriber.pop();
    assertEquals(bufferedSubscriber.fetchOffset(), Offset.of(message1.getCursor().getOffset() + 1));
    assertEquals(bufferedSubscriber.peek().get(), message2);
    bufferedSubscriber.pop();
    assertEquals(bufferedSubscriber.fetchOffset(), Offset.of(message2.getCursor().getOffset() + 1));
  }
}
