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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.naming.SizeLimitExceededException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that {@link PubsubIO.Write.PubsubBoundedWriter} cuts batches by the size of the publish
 * request that its client really sends.
 *
 * <p>Every test publishes through a real {@link PubsubJsonClient} whose HTTP transport records the
 * body of each request. The expected batches come from the sizes of those bodies, never from the
 * writer's own accounting.
 */
@RunWith(JUnit4.class)
public class PubsubBoundedWriterTest {
  private static final String TOPIC = "projects/project/topics/topic";
  private static final Instant TIMESTAMP = Instant.ofEpochMilli(1_700_000_000_000L);

  /** Pub/Sub rejects a publish request of more than 10 MB. */
  private static final int PUBLISH_REQUEST_LIMIT = 10 * 1000 * 1000;

  /** The number of messages that PubsubIO puts into one request unless told otherwise. */
  private static final int DEFAULT_MAX_BATCH_SIZE = 100;

  private static final int MAX_ATTRIBUTES = 100;
  private static final int MAX_ATTRIBUTE_KEY_BYTES = 256;
  private static final int MAX_ATTRIBUTE_VALUE_BYTES = 1024;

  private static class RecordingJsonClientFactory implements PubsubClient.PubsubClientFactory {
    private final transient RecordingPubsub recording;

    RecordingJsonClientFactory(RecordingPubsub recording) {
      this.recording = recording;
    }

    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute,
        @Nullable String idAttribute,
        PubsubOptions options,
        @Nullable String rootUrlOverride) {
      return new PubsubJsonClient(timestampAttribute, idAttribute, recording.newPubsub());
    }

    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options) {
      return newClient(timestampAttribute, idAttribute, options, null);
    }

    @Override
    public String getKind() {
      return "RecordingJson";
    }
  }

  /** Runs one bundle of {@code messages} through the writer and returns what Pub/Sub received. */
  private static RecordingPubsub write(
      PubsubIO.Write<PubsubMessage> configuredWrite,
      RecordingPubsub recording,
      List<PubsubMessage> messages)
      throws Exception {
    PubsubIO.Write<PubsubMessage> write =
        configuredWrite.withClientFactory(new RecordingJsonClientFactory(recording));
    // Built from the configured sizes the way PubsubIO.Write.expand builds the writer.
    @Nullable Integer maxBatchBytes = write.getMaxBatchBytesSize();
    PubsubIO.Write<PubsubMessage>.PubsubBoundedWriter writer =
        maxBatchBytes == null
            ? write.new PubsubBoundedWriter()
            : write.new PubsubBoundedWriter(DEFAULT_MAX_BATCH_SIZE, maxBatchBytes);
    try (DoFnTester<PubsubMessage, Void> tester = DoFnTester.of(writer)) {
      tester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);
      tester.startBundle();
      for (PubsubMessage message : messages) {
        tester.processTimestampedElement(TimestampedValue.of(message, TIMESTAMP));
      }
      tester.finishBundle();
    }
    return recording;
  }

  private static RecordingPubsub write(
      PubsubIO.Write<PubsubMessage> configuredWrite, List<PubsubMessage> messages)
      throws Exception {
    return write(configuredWrite, new RecordingPubsub(), messages);
  }

  /** The size of the body of the one request that publishes {@code messages} together. */
  private static int bodyBytesOfOneRequest(
      @Nullable String timestampAttribute, List<PubsubMessage> messages) throws Exception {
    RecordingPubsub oracle = new RecordingPubsub();
    PubsubJsonClient client = new PubsubJsonClient(timestampAttribute, null, oracle.newPubsub());
    List<OutgoingMessage> outgoing =
        messages.stream()
            .map(m -> OutgoingMessage.of(m, TIMESTAMP.getMillis(), null, null))
            .collect(Collectors.toList());
    client.publish(PubsubClient.topicPathFromName("project", "topic"), outgoing);
    return oracle.bodyBytes.get(0);
  }

  /** The most identical messages that fit into one request, found from two measured requests. */
  private static int capacityOfIdenticalMessages(PubsubMessage message, int limit)
      throws Exception {
    int one = bodyBytesOfOneRequest(null, Collections.nCopies(1, message));
    int two = bodyBytesOfOneRequest(null, Collections.nCopies(2, message));
    int perMessage = two - one;
    return (limit - one) / perMessage + 1;
  }

  private static Map<String, String> attributes(int count, char value) {
    Map<String, String> attributes = new HashMap<>();
    for (int i = 0; i < count; i++) {
      attributes.put(
          Strings.padStart(Integer.toString(i), MAX_ATTRIBUTE_KEY_BYTES, '0'),
          String.valueOf(value).repeat(MAX_ATTRIBUTE_VALUE_BYTES));
    }
    return attributes;
  }

  private static List<Integer> expectedBatches(int total, int capacity) {
    List<Integer> batches = new ArrayList<>();
    for (int remaining = total; remaining > 0; remaining -= capacity) {
      batches.add(Math.min(remaining, capacity));
    }
    return batches;
  }

  @Test
  public void fillsTheRequestLimitWithMessagesThatCarryMaxSizeAttributes() throws Exception {
    // 100 messages of about 128 KB each do not fit into one 10 MB request.
    PubsubMessage message = new PubsubMessage(new byte[10], attributes(MAX_ATTRIBUTES, 'v'));
    List<PubsubMessage> messages = Collections.nCopies(100, message);
    int capacity = capacityOfIdenticalMessages(message, PUBLISH_REQUEST_LIMIT);

    RecordingPubsub received = write(PubsubIO.writeMessages().to(TOPIC), messages);

    assertTrue(
        "Requests of " + received.bodyBytes + " bytes exceed " + PUBLISH_REQUEST_LIMIT,
        received.bodyBytes.stream().allMatch(bytes -> bytes <= PUBLISH_REQUEST_LIMIT));
    assertEquals(expectedBatches(100, capacity), received.messageCounts);
  }

  @Test
  public void countsTheEscapeSequencesThatJsonAddsToAttributes() throws Exception {
    // Each U+0001 in an attribute value takes 6 bytes (\u0001) in a JSON request, so a message
    // with 100 such attributes is five times as large in the request as its raw size.
    PubsubMessage message = new PubsubMessage(new byte[10], attributes(MAX_ATTRIBUTES, '\u0001'));
    List<PubsubMessage> messages = Collections.nCopies(100, message);
    int capacity = capacityOfIdenticalMessages(message, PUBLISH_REQUEST_LIMIT);

    RecordingPubsub received = write(PubsubIO.writeMessages().to(TOPIC), messages);

    assertTrue(
        "Requests of " + received.bodyBytes + " bytes exceed " + PUBLISH_REQUEST_LIMIT,
        received.bodyBytes.stream().allMatch(bytes -> bytes <= PUBLISH_REQUEST_LIMIT));
    assertEquals(expectedBatches(100, capacity), received.messageCounts);
  }

  @Test
  public void cutsEveryBatchAsLateAsPossibleWithOrderingKeysAndATimestampAttribute()
      throws Exception {
    int limit = 2_000;
    List<PubsubMessage> messages =
        IntStream.range(0, 60)
            .mapToObj(
                i -> {
                  Map<String, String> attributes = new HashMap<>();
                  for (int a = 0; a < i % 4; a++) {
                    attributes.put("attribute-" + a, "\"v\\".repeat(1 + (i * 7) % 23));
                  }
                  return new PubsubMessage(new byte[1 + (i * 13) % 61], attributes)
                      .withOrderingKey("order-key");
                })
            .collect(Collectors.toList());

    RecordingPubsub received =
        write(
            PubsubIO.writeMessages()
                .to(TOPIC)
                .withOrderingKey()
                .withTimestampAttribute("ts")
                .withMaxBatchBytesSize(limit),
            messages);

    assertTrue(
        "Requests of " + received.bodyBytes + " bytes exceed " + limit,
        received.bodyBytes.stream().allMatch(bytes -> bytes <= limit));
    int start = 0;
    for (int batch = 0; batch < received.messageCounts.size() - 1; batch++) {
      int end = start + received.messageCounts.get(batch);
      List<PubsubMessage> withNext = messages.subList(start, end + 1);
      assertTrue(
          "Batch "
              + batch
              + " of "
              + (end - start)
              + " messages had room for the next one: "
              + bodyBytesOfOneRequest("ts", withNext)
              + " <= "
              + limit,
          bodyBytesOfOneRequest("ts", withNext) > limit);
      start = end;
    }
    assertEquals(
        messages.size(), received.messageCounts.stream().mapToInt(Integer::intValue).sum());
  }

  @Test
  public void rejectsAMessageThatCannotFitIntoARequestBeforePublishingAnything() throws Exception {
    int limit = 1_000;
    // The raw message is far below the limit, but its base64 encoding in a JSON request is not.
    PubsubMessage message = new PubsubMessage(new byte[800], Collections.emptyMap());
    RecordingPubsub received = new RecordingPubsub();

    assertThrows(
        SizeLimitExceededException.class,
        () ->
            write(
                PubsubIO.writeMessages().to(TOPIC).withMaxBatchBytesSize(limit),
                received,
                Collections.singletonList(message)));

    assertTrue(received.bodyBytes.isEmpty());
  }

  @Test
  public void treatsTheLimitAsInclusiveDownToTheByte() throws Exception {
    PubsubMessage small = new PubsubMessage(new byte[7], Collections.singletonMap("k", "v"));
    PubsubMessage large = new PubsubMessage(new byte[300], attributes(3, 'v'));
    List<PubsubMessage> both = Arrays.asList(small, large);
    int bothBytes = bodyBytesOfOneRequest(null, both);
    int largeBytes = bodyBytesOfOneRequest(null, Collections.singletonList(large));

    assertEquals(
        Collections.singletonList(2),
        write(PubsubIO.writeMessages().to(TOPIC).withMaxBatchBytesSize(bothBytes), both)
            .messageCounts);
    assertEquals(
        Arrays.asList(1, 1),
        write(PubsubIO.writeMessages().to(TOPIC).withMaxBatchBytesSize(bothBytes - 1), both)
            .messageCounts);
    assertEquals(
        Collections.singletonList(1),
        write(
                PubsubIO.writeMessages().to(TOPIC).withMaxBatchBytesSize(largeBytes),
                Collections.singletonList(large))
            .messageCounts);
    assertThrows(
        SizeLimitExceededException.class,
        () ->
            write(
                PubsubIO.writeMessages().to(TOPIC).withMaxBatchBytesSize(largeBytes - 1),
                Collections.singletonList(large)));
  }
}
