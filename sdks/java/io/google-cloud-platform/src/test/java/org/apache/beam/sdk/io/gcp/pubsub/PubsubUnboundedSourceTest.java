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
package org.apache.beam.sdk.io.gcp.pubsub;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.client.util.Clock;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubCheckpoint;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubReader;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test PubsubUnboundedSource. */
@RunWith(JUnit4.class)
public class PubsubUnboundedSourceTest {
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final String DATA = "testData";
  private static final long TIMESTAMP = 1234L;
  private static final long REQ_TIME = 6373L;
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final String ACK_ID = "testAckId";
  private static final String RECORD_ID = "testRecordId";
  private static final int ACK_TIMEOUT_S = 60;

  private AtomicLong now;
  private Clock clock;
  private PubsubTestClientFactory factory;
  private PubsubSource primSource;

  @Rule public TestPipeline p = TestPipeline.create();

  private void setupOneMessage(Iterable<IncomingMessage> incoming) {
    now = new AtomicLong(REQ_TIME);
    clock = () -> now.get();
    factory = PubsubTestClient.createFactoryForPull(clock, SUBSCRIPTION, ACK_TIMEOUT_S, incoming);
    PubsubUnboundedSource source =
        new PubsubUnboundedSource(
            clock,
            factory,
            null,
            null,
            StaticValueProvider.of(SUBSCRIPTION),
            TIMESTAMP_ATTRIBUTE,
            ID_ATTRIBUTE,
            true /* needsAttributes */);
    primSource = new PubsubSource(source);
  }

  private void setupOneMessage() {
    setupOneMessage(
        ImmutableList.of(
            IncomingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA))
                    .build(),
                TIMESTAMP,
                0,
                ACK_ID,
                RECORD_ID)));
  }

  @After
  public void after() throws IOException {
    factory.close();
    now = null;
    clock = null;
    primSource = null;
    factory = null;
  }

  private static String data(byte[] message, boolean payloadOnly) throws Exception {
    if (payloadOnly) {
      return new String(message, StandardCharsets.UTF_8);
    }
    PubsubMessage data = PubsubMessage.parseFrom(message);
    return new String(data.getData().toByteArray(), StandardCharsets.UTF_8);
  }

  @Test
  public void checkpointCoderIsSane() {
    setupOneMessage(ImmutableList.of());
    CoderProperties.coderSerializable(primSource.getCheckpointMarkCoder());
    // Since we only serialize/deserialize the 'notYetReadIds', and we don't want to make
    // equals on checkpoints ignore those fields, we'll test serialization and deserialization
    // of checkpoints in multipleReaders below.
  }

  @Test
  public void readOneMessage() throws Exception {
    setupOneMessage();
    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    // Read one message.
    assertTrue(reader.start());
    assertEquals(
        DATA,
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));
    assertFalse(reader.advance());
    // ACK the message.
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void timeoutAckAndRereadOneMessage() throws Exception {
    setupOneMessage();
    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    PubsubTestClient pubsubClient = (PubsubTestClient) reader.getPubsubClient();
    assertTrue(reader.start());
    assertEquals(
        DATA,
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));
    // Let the ACK deadline for the above expire.
    now.addAndGet(65 * 1000);
    pubsubClient.advance();
    // We'll now receive the same message again.
    assertTrue(reader.advance());
    assertEquals(
        DATA,
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));
    assertFalse(reader.advance());
    // Now ACK the message.
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void extendAck() throws Exception {
    setupOneMessage();
    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    PubsubTestClient pubsubClient = (PubsubTestClient) reader.getPubsubClient();
    // Pull the first message but don't take a checkpoint for it.
    assertTrue(reader.start());
    assertEquals(
        DATA,
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));
    // Extend the ack
    now.addAndGet(55 * 1000);
    pubsubClient.advance();
    assertFalse(reader.advance());
    // Extend the ack again
    now.addAndGet(25 * 1000);
    pubsubClient.advance();
    assertFalse(reader.advance());
    // Now ACK the message.
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void timeoutAckExtensions() throws Exception {
    setupOneMessage();
    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    PubsubTestClient pubsubClient = (PubsubTestClient) reader.getPubsubClient();
    // Pull the first message but don't take a checkpoint for it.
    assertTrue(reader.start());
    assertEquals(
        DATA,
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));
    // Extend the ack.
    now.addAndGet(55 * 1000);
    pubsubClient.advance();
    assertFalse(reader.advance());
    // Let the ack expire.
    for (int i = 0; i < 3; i++) {
      now.addAndGet(25 * 1000);
      pubsubClient.advance();
      assertFalse(reader.advance());
    }
    // Wait for resend.
    now.addAndGet(25 * 1000);
    pubsubClient.advance();
    // Reread the same message.
    assertTrue(reader.advance());
    assertEquals(
        DATA,
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));
    // Now ACK the message.
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void multipleReaders() throws Exception {
    List<IncomingMessage> incoming = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String data = String.format("data_%d", i);
      String ackid = String.format("ackid_%d", i);
      incoming.add(
          IncomingMessage.of(
              com.google.pubsub.v1.PubsubMessage.newBuilder()
                  .setData(ByteString.copyFromUtf8(data))
                  .build(),
              TIMESTAMP,
              0,
              ackid,
              RECORD_ID));
    }
    setupOneMessage(incoming);
    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    // Consume two messages, only read one.
    assertTrue(reader.start());
    assertEquals(
        "data_0",
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));

    // Grab checkpoint.
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    assertEquals(1, checkpoint.notYetReadIds.size());
    assertEquals("ackid_1", checkpoint.notYetReadIds.get(0));

    // Read second message.
    assertTrue(reader.advance());
    assertEquals(
        "data_1",
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));

    // Restore from checkpoint.
    byte[] checkpointBytes =
        CoderUtils.encodeToByteArray(primSource.getCheckpointMarkCoder(), checkpoint);
    checkpoint =
        CoderUtils.decodeFromByteArray(primSource.getCheckpointMarkCoder(), checkpointBytes);
    assertEquals(1, checkpoint.notYetReadIds.size());
    assertEquals("ackid_1", checkpoint.notYetReadIds.get(0));

    // Re-read second message.
    reader = primSource.createReader(p.getOptions(), checkpoint);
    assertTrue(reader.start());
    assertEquals(
        "data_1",
        data(
            reader.getCurrent(),
            !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId())));

    // We are done.
    assertFalse(reader.advance());

    // ACK final message.
    checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  private long messageNumToTimestamp(int messageNum) {
    return TIMESTAMP + messageNum * 100;
  }

  @Test
  public void readManyMessages() throws Exception {
    Map<String, Integer> dataToMessageNum = new HashMap<>();

    final int m = 97;
    final int n = 10000;
    List<IncomingMessage> incoming = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      // Make the messages timestamps slightly out of order.
      int messageNum = ((i / m) * m) + (m - 1) - (i % m);
      String data = String.format("data_%d", messageNum);
      dataToMessageNum.put(data, messageNum);
      String recid = String.format("recordid_%d", messageNum);
      String ackId = String.format("ackid_%d", messageNum);
      incoming.add(
          IncomingMessage.of(
              com.google.pubsub.v1.PubsubMessage.newBuilder()
                  .setData(ByteString.copyFromUtf8(data))
                  .build(),
              messageNumToTimestamp(messageNum),
              0,
              ackId,
              recid));
    }
    setupOneMessage(incoming);

    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    PubsubTestClient pubsubClient = (PubsubTestClient) reader.getPubsubClient();

    for (int i = 0; i < n; i++) {
      if (i == 0) {
        assertTrue(reader.start());
      } else {
        assertTrue(reader.advance());
      }
      // We'll checkpoint and ack within the 2min limit.
      now.addAndGet(30);
      pubsubClient.advance();
      String data =
          data(
              reader.getCurrent(),
              !(primSource.outer.getNeedsAttributes() || primSource.outer.getNeedsMessageId()));
      Integer messageNum = dataToMessageNum.remove(data);
      // No duplicate messages.
      assertNotNull(messageNum);
      // Preserve timestamp.
      assertEquals(new Instant(messageNumToTimestamp(messageNum)), reader.getCurrentTimestamp());
      // Preserve record id.
      String recid = String.format("recordid_%d", messageNum);
      assertArrayEquals(recid.getBytes(StandardCharsets.UTF_8), reader.getCurrentRecordId());

      if (i % 1000 == 999) {
        // Estimated watermark can never get ahead of actual outstanding messages.
        long watermark = reader.getWatermark().getMillis();
        long minOutstandingTimestamp = Long.MAX_VALUE;
        for (Integer outstandingMessageNum : dataToMessageNum.values()) {
          minOutstandingTimestamp =
              Math.min(minOutstandingTimestamp, messageNumToTimestamp(outstandingMessageNum));
        }
        assertThat(watermark, lessThanOrEqualTo(minOutstandingTimestamp));
        // Ack messages, but only every other finalization.
        PubsubCheckpoint checkpoint = reader.getCheckpointMark();
        if (i % 2000 == 1999) {
          checkpoint.finalizeCheckpoint();
        }
      }
    }
    // We are done.
    assertFalse(reader.advance());
    // We saw each message exactly once.
    assertTrue(dataToMessageNum.isEmpty());
    reader.close();
  }

  @Test
  public void noSubscriptionSplitGeneratesSubscription() throws Exception {
    TopicPath topicPath = PubsubClient.topicPathFromName("my_project", "my_topic");
    factory = PubsubTestClient.createFactoryForCreateSubscription();
    PubsubUnboundedSource source =
        new PubsubUnboundedSource(
            factory,
            StaticValueProvider.of(PubsubClient.projectPathFromId("my_project")),
            StaticValueProvider.of(topicPath),
            null /* subscription */,
            null /* timestampLabel */,
            null /* idLabel */,
            false /* needsAttributes */);
    assertThat(source.getSubscription(), nullValue());

    assertThat(source.getSubscription(), nullValue());

    PipelineOptions options = PipelineOptionsFactory.create();
    List<PubsubSource> splits = new PubsubSource(source).split(3, options);
    // We have at least one returned split
    assertThat(splits, hasSize(greaterThan(0)));
    for (PubsubSource split : splits) {
      // Each split is equal
      assertThat(split, equalTo(splits.get(0)));
    }

    assertThat(splits.get(0).subscriptionPath, not(nullValue()));
  }

  @Test
  public void noSubscriptionNoSplitGeneratesSubscription() throws Exception {
    TopicPath topicPath = PubsubClient.topicPathFromName("my_project", "my_topic");
    factory = PubsubTestClient.createFactoryForCreateSubscription();
    PubsubUnboundedSource source =
        new PubsubUnboundedSource(
            factory,
            StaticValueProvider.of(PubsubClient.projectPathFromId("my_project")),
            StaticValueProvider.of(topicPath),
            null /* subscription */,
            null /* timestampLabel */,
            null /* idLabel */,
            false /* needsAttributes */);
    assertThat(source.getSubscription(), nullValue());

    assertThat(source.getSubscription(), nullValue());

    PipelineOptions options = PipelineOptionsFactory.create();
    PubsubSource actualSource = new PubsubSource(source);
    PubsubReader reader = actualSource.createReader(options, null);
    SubscriptionPath createdSubscription = reader.subscription;
    assertThat(createdSubscription, not(nullValue()));

    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    assertThat(checkpoint.subscriptionPath, equalTo(createdSubscription.getPath()));

    checkpoint.finalizeCheckpoint();
    PubsubCheckpoint deserCheckpoint =
        CoderUtils.clone(actualSource.getCheckpointMarkCoder(), checkpoint);
    assertThat(checkpoint.subscriptionPath, not(nullValue()));
    assertThat(checkpoint.subscriptionPath, equalTo(deserCheckpoint.subscriptionPath));

    PubsubReader readerFromOriginal = actualSource.createReader(options, checkpoint);
    PubsubReader readerFromDeser = actualSource.createReader(options, deserCheckpoint);

    assertThat(readerFromOriginal.subscription, equalTo(createdSubscription));
    assertThat(readerFromDeser.subscription, equalTo(createdSubscription));
  }

  /** Tests that checkpoints finalized after the reader is closed succeed. */
  @Test
  public void closeWithActiveCheckpoints() throws Exception {
    setupOneMessage();
    PubsubReader reader = primSource.createReader(p.getOptions(), null);
    reader.start();
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    reader.close();
    checkpoint.finalizeCheckpoint();
  }
}
