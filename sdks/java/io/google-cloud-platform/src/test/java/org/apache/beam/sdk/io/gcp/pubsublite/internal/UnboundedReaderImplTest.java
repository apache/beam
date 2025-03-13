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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class UnboundedReaderImplTest {

  private static final Offset INITIAL_OFFSET = Offset.of(1);

  @Rule public MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Spy private UnboundedSource<SequencedMessage, CheckpointMarkImpl> source;

  abstract static class FakeSubscriber extends FakeApiService implements MemoryBufferedSubscriber {}

  @Spy private FakeSubscriber subscriber;
  @Mock private TopicBacklogReader backlogReader;
  @Mock private BlockingCommitter committer;

  private UnboundedReaderImpl reader;

  private static SequencedMessage messageWith(String data, long offset, Instant timestamp) {
    return com.google.cloud.pubsublite.SequencedMessage.of(
            Message.builder().setData(ByteString.copyFromUtf8(data)).build(),
            Timestamps.fromMillis(timestamp.getMillis()),
            Offset.of(offset),
            21)
        .toProto();
  }

  private void startSubscriber() throws Exception {
    doReturn(Optional.empty()).when(subscriber).peek();
    assertFalse(reader.start());
    verify(subscriber).startAsync();
    verify(subscriber).awaitRunning(1, TimeUnit.MINUTES);
  }

  private void advancePastMessage(long offset) throws Exception {
    SequencedMessage message = messageWith("abc", offset, Instant.now());
    doReturn(Optional.of(message)).when(subscriber).peek();
    assertTrue(reader.advance());
    doAnswer(
            (Answer<Void>)
                args -> {
                  doReturn(Optional.empty()).when(subscriber).peek();
                  return null;
                })
        .when(subscriber)
        .pop();
    assertFalse(reader.advance());
  }

  @Before
  public void setUp() {
    doReturn(INITIAL_OFFSET).when(subscriber).fetchOffset();
    reader =
        new UnboundedReaderImpl(source, subscriber, backlogReader, () -> committer, Offset.of(1));
  }

  @Test
  public void startAdvances() throws Exception {
    Instant ts = Instant.now();
    SequencedMessage message = messageWith("abc", 2, ts);
    doReturn(Optional.of(message)).when(subscriber).peek();
    assertTrue(reader.start());
    verify(subscriber).startAsync();
    verify(subscriber).awaitRunning(1, TimeUnit.MINUTES);
    assertEquals(reader.getCurrent(), message);
    assertEquals(reader.getWatermark(), ts);
  }

  @Test
  public void startAdvancesNoMessage() throws Exception {
    doReturn(Optional.empty()).when(subscriber).peek();
    assertFalse(reader.start());
    verify(subscriber).startAsync();
    verify(subscriber).awaitRunning(1, TimeUnit.MINUTES);
    assertThrows(NoSuchElementException.class, reader::getCurrent);
    assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, reader.getWatermark());
  }

  @Test
  public void advanceNoPreviousValue() throws Exception {
    startSubscriber();
    SequencedMessage message = messageWith("abc", 2, Instant.now());
    doReturn(Optional.of(message)).when(subscriber).peek();
    assertTrue(reader.advance());
    verify(subscriber, times(0)).pop();
  }

  @Test
  public void advanceWithPreviousValue() throws Exception {
    startSubscriber();
    Instant ts1 = Instant.now();
    Instant ts2 = Instant.now();
    SequencedMessage message1 = messageWith("abc", 2, ts1);
    SequencedMessage message2 = messageWith("def", 3, ts2);
    doReturn(Optional.of(message1)).when(subscriber).peek();
    assertTrue(reader.advance());
    assertEquals(reader.getCurrent(), message1);
    assertEquals(reader.getCurrentTimestamp(), ts1);
    assertEquals(reader.getWatermark(), ts1);
    doAnswer(
            (Answer<Void>)
                args -> {
                  doReturn(Optional.of(message2)).when(subscriber).peek();
                  return null;
                })
        .when(subscriber)
        .pop();
    assertTrue(reader.advance());
    verify(subscriber).pop();
    assertEquals(reader.getCurrent(), message2);
    assertEquals(reader.getCurrentTimestamp(), ts2);
    assertEquals(reader.getWatermark(), ts1);
  }

  @Test
  public void advanceSubscriberNotRunningThrows() throws Exception {
    startSubscriber();
    subscriber.fail(new RuntimeException("I failed"));
    assertThrows(IOException.class, reader::advance);
  }

  @Test
  public void getCheckpointMark() throws Exception {
    startSubscriber();
    advancePastMessage(2);
    CheckpointMarkImpl mark = reader.getCheckpointMark();
    verify(subscriber).rebuffer();
    assertEquals(3, mark.offset.value());
  }

  @Test
  public void getSplitBacklogBytes() throws Exception {
    startSubscriber();
    advancePastMessage(2);
    doReturn(ComputeMessageStatsResponse.newBuilder().setMessageBytes(42).build())
        .when(backlogReader)
        .computeMessageStats(Offset.of(3));
    assertEquals(42, reader.getSplitBacklogBytes());
  }

  @Test
  public void closeClosesAll() throws Exception {
    startSubscriber();
    doThrow(new IllegalStateException("abc")).when(subscriber).awaitTerminated(1, TimeUnit.MINUTES);
    doThrow(new IllegalStateException("def")).when(backlogReader).close();
    assertThrows(IOException.class, reader::close);
    verify(subscriber).stopAsync();
    verify(subscriber).awaitTerminated(1, TimeUnit.MINUTES);
    verify(backlogReader).close();
  }
}
