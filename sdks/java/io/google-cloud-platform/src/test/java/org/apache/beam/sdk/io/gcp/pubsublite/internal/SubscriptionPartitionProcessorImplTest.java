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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class SubscriptionPartitionProcessorImplTest {
  @Spy RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker;
  @Mock OutputReceiver<SequencedMessage> receiver;

  @Rule public Timeout globalTimeout = Timeout.seconds(30);

  abstract static class FakeSubscriber extends FakeApiService implements MemoryBufferedSubscriber {}

  @Spy FakeSubscriber subscriber;

  private static SequencedMessage messageWithOffset(long offset) {
    return SequencedMessage.newBuilder()
        .setCursor(Cursor.newBuilder().setOffset(offset))
        .setPublishTime(Timestamps.fromMillis(10000 + offset))
        .setSizeBytes(1024)
        .build();
  }

  private OffsetByteRange initialRange() {
    return OffsetByteRange.of(new OffsetRange(example(Offset.class).value(), Long.MAX_VALUE));
  }

  @Before
  public void setUp() {
    initMocks(this);
    subscriber.startAsync().awaitRunning();
    when(tracker.currentRestriction()).thenReturn(initialRange());
    doReturn(example(Offset.class)).when(subscriber).fetchOffset();
  }

  private SubscriptionPartitionProcessor newProcessor() {
    return new SubscriptionPartitionProcessorImpl(tracker, receiver, subscriber);
  }

  @Test
  public void create() {
    SubscriptionPartitionProcessor processor = newProcessor();
    assertEquals(ProcessContinuation.resume(), processor.run());
    InOrder order = inOrder(subscriber);
    order.verify(subscriber).fetchOffset();
    order.verify(subscriber).rebuffer();
  }

  @Test
  public void createRebufferThrows() throws Exception {
    doThrow(new CheckedApiException(Code.OUT_OF_RANGE).underlying).when(subscriber).rebuffer();
    assertThrows(ApiException.class, this::newProcessor);
  }

  @Test
  public void failedClaimCausesStop() {
    SubscriptionPartitionProcessor processor = newProcessor();

    when(tracker.tryClaim(any())).thenReturn(false);
    doReturn(Optional.of(messageWithOffset(1))).when(subscriber).peek();

    assertEquals(ProcessContinuation.stop(), processor.run());

    verify(tracker, times(1)).tryClaim(any());
    verify(subscriber, times(0)).pop();
    assertFalse(processor.lastClaimed().isPresent());
  }

  @Test
  public void successfulClaimsThenNoMoreMessagesFromSubscriber() {
    doReturn(true).when(tracker).tryClaim(any());

    SequencedMessage message1 = messageWithOffset(1);
    SequencedMessage message3 = messageWithOffset(3);
    doReturn(Optional.of(message1), Optional.of(message3), Optional.empty())
        .when(subscriber)
        .peek();

    SubscriptionPartitionProcessor processor = newProcessor();
    assertEquals(ProcessContinuation.resume(), processor.run());

    InOrder order = inOrder(tracker, receiver);
    order.verify(tracker).tryClaim(OffsetByteProgress.of(Offset.of(1), message1.getSizeBytes()));
    order
        .verify(receiver)
        .outputWithTimestamp(message1, new Instant(Timestamps.toMillis(message1.getPublishTime())));
    order.verify(tracker).tryClaim(OffsetByteProgress.of(Offset.of(3), message3.getSizeBytes()));
    order
        .verify(receiver)
        .outputWithTimestamp(message3, new Instant(Timestamps.toMillis(message3.getPublishTime())));
    assertEquals(processor.lastClaimed().get(), Offset.of(3));
  }
}
