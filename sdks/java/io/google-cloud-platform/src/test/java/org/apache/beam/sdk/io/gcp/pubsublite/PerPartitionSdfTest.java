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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class PerPartitionSdfTest {
  private final Duration MAX_SLEEP_TIME = Duration.standardMinutes(10).plus(Duration.millis(10));
  private final Duration SLEEP_CYCLE_TIME = Duration.millis(50);
  private final OffsetRange RESTRICTION = new OffsetRange(1, Long.MAX_VALUE);

  @Mock
  SerializableBiFunction<Partition, Offset, PullSubscriber<SequencedMessage>> subscriberFactory;

  @Mock SerializableFunction<Partition, Committer> committerFactory;
  @Mock SerializableFunction<Partition, InitialOffsetReader> offsetReaderFactory;

  @Mock
  SerializableBiFunction<
          Partition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
      trackerFactory;

  @Mock PullSubscriber<SequencedMessage> subscriber;

  abstract static class FakeCommitter extends FakeApiService implements Committer {}

  @Spy FakeCommitter committer;
  @Mock Sleeper sleeper;
  @Mock InitialOffsetReader initialOffsetReader;
  @Spy RestrictionTracker<OffsetRange, OffsetByteProgress> tracker;
  @Mock OutputReceiver<SequencedMessage> output;

  PerPartitionSdf sdf;

  private static SequencedMessage messageWithOffset(long offset) {
    return SequencedMessage.newBuilder()
        .setCursor(Cursor.newBuilder().setOffset(offset))
        .setPublishTime(Timestamps.fromMillis(10000 + offset))
        .setSizeBytes(1024)
        .build();
  }

  @Before
  public void setUp() {
    initMocks(this);
    when(subscriberFactory.apply(any(), any())).thenReturn(subscriber);
    when(committerFactory.apply(any())).thenReturn(committer);
    when(offsetReaderFactory.apply(any())).thenReturn(initialOffsetReader);
    when(trackerFactory.apply(any(), any())).thenReturn(tracker);
    when(tracker.currentRestriction()).thenReturn(RESTRICTION);
    sdf =
        new PerPartitionSdf(
            MAX_SLEEP_TIME,
            subscriberFactory,
            committerFactory,
            () -> sleeper,
            offsetReaderFactory,
            trackerFactory);
  }

  @Test
  public void getInitialRestrictionReadSuccess() {
    when(initialOffsetReader.read()).thenReturn(example(Offset.class));
    OffsetRange range = sdf.getInitialRestriction(example(Partition.class));
    assertEquals(example(Offset.class).value(), range.getFrom());
    assertEquals(Long.MAX_VALUE, range.getTo());
    verify(offsetReaderFactory).apply(example(Partition.class));
  }

  @Test
  public void getInitialRestrictionReadFailure() {
    when(initialOffsetReader.read()).thenThrow(new CheckedApiException(Code.INTERNAL).underlying);
    assertThrows(ApiException.class, () -> sdf.getInitialRestriction(example(Partition.class)));
  }

  @Test
  public void newTrackerCallsFactory() {
    assertSame(tracker, sdf.newTracker(example(Partition.class), RESTRICTION));
    verify(trackerFactory).apply(example(Partition.class), RESTRICTION);
  }

  @Test
  public void processThrowDuringPoll() throws Exception {
    when(subscriber.pull()).thenThrow(new CheckedApiException(Code.INTERNAL));
    assertThrows(
        CheckedApiException.class,
        () -> sdf.processElement(tracker, example(Partition.class), output));
    verify(subscriber).pull();
    verify(subscriber).close();
    verify(committer).startAsync();
    verify(committer).stopAsync();
  }

  @Test
  public void processReceivesDataThenTimeout() throws Exception {
    int numSleeps =
        (int) Math.ceil(MAX_SLEEP_TIME.getMillis() * 1.0 / SLEEP_CYCLE_TIME.getMillis());
    int numFullSleeps = numSleeps - 1;
    Duration leftoverSleep = MAX_SLEEP_TIME.minus(SLEEP_CYCLE_TIME.multipliedBy(numFullSleeps));
    SequencedMessage message3 = messageWithOffset(3);
    SequencedMessage message4 = messageWithOffset(4);
    when(subscriber.pull())
        .thenReturn(ImmutableList.of(message3, message4))
        .thenReturn(ImmutableList.of()); // Repeated
    when(committer.commitOffset(any())).thenReturn(ApiFutures.immediateFuture(null));
    when(tracker.tryClaim(any())).thenReturn(true);
    assertTrue(sdf.processElement(tracker, example(Partition.class), output).shouldResume());
    verify(subscriber, times(numSleeps + 1)).pull();
    verify(sleeper, times(numFullSleeps)).sleep(SLEEP_CYCLE_TIME.getMillis());
    verify(sleeper).sleep(leftoverSleep.getMillis());
    InOrder order = inOrder(output, tracker, committer);
    order
        .verify(tracker)
        .tryClaim(
            OffsetByteProgress.of(Offset.of(4), message3.getSizeBytes() + message4.getSizeBytes()));
    order
        .verify(output)
        .outputWithTimestamp(message3, new Instant(Timestamps.toMillis(message3.getPublishTime())));
    order
        .verify(output)
        .outputWithTimestamp(message4, new Instant(Timestamps.toMillis(message4.getPublishTime())));
    order.verify(committer).commitOffset(Offset.of(5)); // Commit last received + 1
    verify(subscriber).close();
    verify(committer).startAsync();
    verify(committer).stopAsync();
  }

  @Test
  public void processRejectedClaim() throws Exception {
    when(subscriber.pull())
        .thenReturn(ImmutableList.of(messageWithOffset(3), messageWithOffset(4)));
    when(committer.commitOffset(any())).thenReturn(ApiFutures.immediateFuture(null));
    when(tracker.tryClaim(any())).thenReturn(false);
    assertFalse(sdf.processElement(tracker, example(Partition.class), output).shouldResume());
    verify(committer, times(0)).commitOffset(any());
    verify(subscriber).close();
    verify(committer).startAsync();
    verify(committer).stopAsync();
  }

  @Test
  @SuppressWarnings("return.type.incompatible")
  public void dofnIsSerializable() throws Exception {
    ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream());
    output.writeObject(
        new PerPartitionSdf(
            MAX_SLEEP_TIME, (x, y) -> null, x -> null, () -> null, x -> null, (x, y) -> null));
  }
}
