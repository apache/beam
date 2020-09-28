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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.FakeApiService;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteUnboundedReader.SubscriberState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

@RunWith(JUnit4.class)
public class PubsubLiteUnboundedReaderTest {
  @Mock private PullSubscriber<SequencedMessage> subscriber5;

  @Mock private PullSubscriber<SequencedMessage> subscriber8;

  abstract static class CommitterFakeService extends FakeApiService implements Committer {}

  @Spy private CommitterFakeService committer5;
  @Spy private CommitterFakeService committer8;

  @SuppressWarnings("unchecked")
  private final UnboundedSource<SequencedMessage, ?> source = mock(UnboundedSource.class);

  private final PubsubLiteUnboundedReader reader;

  private static SequencedMessage exampleMessage(Offset offset, Timestamp publishTime) {
    return SequencedMessage.newBuilder()
        .setPublishTime(publishTime)
        .setCursor(Cursor.newBuilder().setOffset(offset.value()))
        .setSizeBytes(100)
        .build();
  }

  private static Timestamp randomMilliAllignedTimestamp() {
    return Timestamps.fromMillis(new Random().nextInt(Integer.MAX_VALUE));
  }

  private static Instant toInstant(Timestamp timestamp) {
    return new Instant(Timestamps.toMillis(timestamp));
  }

  public PubsubLiteUnboundedReaderTest() throws StatusException {
    MockitoAnnotations.initMocks(this);
    SubscriberState state5 = new SubscriberState();
    state5.subscriber = subscriber5;
    state5.committer = committer5;
    SubscriberState state8 = new SubscriberState();
    state8.subscriber = subscriber8;
    state8.committer = committer8;
    reader =
        new PubsubLiteUnboundedReader(
            source, ImmutableMap.of(Partition.of(5), state5, Partition.of(8), state8));
  }

  @Test
  public void sourceReturnsSource() {
    assertThat(reader.getCurrentSource(), sameInstance(source));
  }

  @Test
  public void startPullsFromAllSubscribers() throws Exception {
    when(subscriber5.pull()).thenReturn(ImmutableList.of());
    when(subscriber8.pull()).thenReturn(ImmutableList.of());
    assertFalse(reader.start());
    verify(subscriber5).pull();
    verify(subscriber8).pull();
    assertThat(reader.getWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
    verifyNoMoreInteractions(subscriber5, subscriber8);
  }

  @Test
  public void startReturnsTrueIfMessagesExist() throws Exception {
    Timestamp ts = randomMilliAllignedTimestamp();
    SequencedMessage message = exampleMessage(Offset.of(10), ts);
    when(subscriber5.pull()).thenReturn(ImmutableList.of(message));
    when(subscriber8.pull()).thenReturn(ImmutableList.of());
    assertTrue(reader.start());
    verify(subscriber5).pull();
    verify(subscriber8).pull();
    assertThat(reader.getCurrent(), equalTo(message));
    assertThat(reader.getWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
    assertThat(reader.getCurrentTimestamp(), equalTo(toInstant(ts)));
    verifyNoMoreInteractions(subscriber5, subscriber8);
  }

  @Test
  public void advanceSetsWatermarkAfterAllSubscribersPopulated() throws Exception {
    Timestamp ts1 = randomMilliAllignedTimestamp();
    Timestamp ts2 = randomMilliAllignedTimestamp();
    SequencedMessage message1 = exampleMessage(Offset.of(10), ts1);
    SequencedMessage message2 = exampleMessage(Offset.of(888), ts2);
    when(subscriber5.pull()).thenReturn(ImmutableList.of(message1));
    when(subscriber8.pull()).thenReturn(ImmutableList.of(message2));
    assertTrue(reader.start());
    verify(subscriber5).pull();
    verify(subscriber8).pull();
    verifyNoMoreInteractions(subscriber5, subscriber8);
    reset(subscriber5, subscriber8);
    List<SequencedMessage> messages = new ArrayList<>();
    messages.add(reader.getCurrent());
    assertThat(reader.getWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
    // This could be either original message, but is the current message from the reader.
    assertThat(reader.getCurrentTimestamp(), equalTo(toInstant(messages.get(0).getPublishTime())));
    assertTrue(reader.advance());
    messages.add(reader.getCurrent());
    assertThat(
        reader.getWatermark(),
        equalTo(Collections.min(Arrays.asList(toInstant(ts1), toInstant(ts2)))));
    assertThat(reader.getCurrentTimestamp(), equalTo(toInstant(messages.get(1).getPublishTime())));
    // Second pull yields no more messages.
    when(subscriber5.pull()).thenReturn(ImmutableList.of());
    when(subscriber8.pull()).thenReturn(ImmutableList.of());
    assertFalse(reader.advance());
    verify(subscriber5).pull();
    verify(subscriber8).pull();
    verifyNoMoreInteractions(subscriber5, subscriber8);
  }

  @Test
  public void multipleMessagesInPullReadsAllBeforeNextPull() throws Exception {
    SequencedMessage message1 = exampleMessage(Offset.of(10), randomMilliAllignedTimestamp());
    SequencedMessage message2 = exampleMessage(Offset.of(888), randomMilliAllignedTimestamp());
    SequencedMessage message3 = exampleMessage(Offset.of(999), randomMilliAllignedTimestamp());
    when(subscriber5.pull())
        .thenReturn(ImmutableList.of(message1, message2, message3))
        .thenReturn(ImmutableList.of());
    when(subscriber8.pull()).thenReturn(ImmutableList.of()).thenReturn(ImmutableList.of());
    assertTrue(reader.start());
    assertTrue(reader.advance());
    assertTrue(reader.advance());
    assertFalse(reader.advance());
    verify(subscriber5, times(2)).pull();
    verify(subscriber8, times(2)).pull();
    verifyNoMoreInteractions(subscriber5, subscriber8);
  }

  @Test
  public void messagesOnSubsequentPullsProcessed() throws Exception {
    SequencedMessage message1 = exampleMessage(Offset.of(10), randomMilliAllignedTimestamp());
    SequencedMessage message2 = exampleMessage(Offset.of(888), randomMilliAllignedTimestamp());
    SequencedMessage message3 = exampleMessage(Offset.of(999), randomMilliAllignedTimestamp());
    when(subscriber5.pull())
        .thenReturn(ImmutableList.of(message1))
        .thenReturn(ImmutableList.of(message2))
        .thenReturn(ImmutableList.of());
    when(subscriber8.pull())
        .thenReturn(ImmutableList.of())
        .thenReturn(ImmutableList.of(message3))
        .thenReturn(ImmutableList.of());
    assertTrue(reader.start());
    assertTrue(reader.advance());
    assertTrue(reader.advance());
    assertFalse(reader.advance());
    verify(subscriber5, times(3)).pull();
    verify(subscriber8, times(3)).pull();
    verifyNoMoreInteractions(subscriber5, subscriber8);
  }

  @Test
  public void checkpointMarkFinalizeCommits() throws Exception {
    Timestamp ts = randomMilliAllignedTimestamp();
    SequencedMessage message = exampleMessage(Offset.of(10), ts);
    when(subscriber5.pull()).thenReturn(ImmutableList.of(message));
    when(subscriber8.pull()).thenReturn(ImmutableList.of());
    assertTrue(reader.start());
    verify(subscriber5).pull();
    verify(subscriber8).pull();
    verifyNoMoreInteractions(subscriber5, subscriber8);

    CheckpointMark mark = reader.getCheckpointMark();

    when(committer5.commitOffset(Offset.of(10))).thenReturn(ApiFutures.immediateFuture(null));
    mark.finalizeCheckpoint();
    verify(committer5).commitOffset(Offset.of(10));
  }
}
