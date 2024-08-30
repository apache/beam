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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink.RecordIdMethod;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test PubsubUnboundedSink. */
@RunWith(JUnit4.class)
public class PubsubUnboundedSinkTest implements Serializable {
  private static final TopicPath TOPIC = PubsubClient.topicPathFromName("testProject", "testTopic");
  private static final String DATA = "testData";
  private static final ImmutableMap<String, String> ATTRIBUTES =
      ImmutableMap.<String, String>builder().put("a", "b").put("c", "d").build();
  private static final long TIMESTAMP = 1234L;
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final int NUM_SHARDS = 10;

  private static class Stamp extends DoFn<String, PubsubMessage> {
    private final Map<String, String> attributes;

    private Stamp() {
      this(ImmutableMap.of());
    }

    private Stamp(Map<String, String> attributes) {
      this.attributes = attributes;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.outputWithTimestamp(
          new PubsubMessage(c.element().getBytes(StandardCharsets.UTF_8), attributes),
          new Instant(TIMESTAMP));
    }
  }

  private String getRecordId(String data) {
    return Hashing.murmur3_128().hashBytes(data.getBytes(StandardCharsets.UTF_8)).toString();
  }

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void saneCoder() throws Exception {
    OutgoingMessage message =
        OutgoingMessage.of(
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(DATA))
                .build(),
            TIMESTAMP,
            getRecordId(DATA),
            null);
    CoderProperties.coderDecodeEncodeEqual(PubsubUnboundedSink.CODER, message);
    CoderProperties.coderSerializable(PubsubUnboundedSink.CODER);
  }

  @Test
  public void sendOneMessage() throws IOException {
    List<OutgoingMessage> outgoing =
        ImmutableList.of(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA))
                    .putAllAttributes(ATTRIBUTES)
                    .build(),
                TIMESTAMP,
                getRecordId(DATA),
                null));
    int batchSize = 1;
    int batchBytes = 1;
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(TOPIC, outgoing, ImmutableList.of())) {
      PubsubUnboundedSink sink =
          new PubsubUnboundedSink(
              factory,
              StaticValueProvider.of(TOPIC),
              TIMESTAMP_ATTRIBUTE,
              ID_ATTRIBUTE,
              NUM_SHARDS,
              batchSize,
              batchBytes,
              Duration.standardSeconds(2),
              RecordIdMethod.DETERMINISTIC,
              null);
      p.apply(Create.of(ImmutableList.of(DATA))).apply(ParDo.of(new Stamp(ATTRIBUTES))).apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  @Test
  public void sendOneMessageWithoutAttributes() throws IOException {
    List<OutgoingMessage> outgoing =
        ImmutableList.of(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA))
                    .build(),
                TIMESTAMP,
                getRecordId(DATA),
                null));
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(TOPIC, outgoing, ImmutableList.of())) {
      PubsubUnboundedSink sink =
          new PubsubUnboundedSink(
              factory,
              StaticValueProvider.of(TOPIC),
              TIMESTAMP_ATTRIBUTE,
              ID_ATTRIBUTE,
              NUM_SHARDS,
              1 /* batchSize */,
              1 /* batchBytes */,
              Duration.standardSeconds(2),
              RecordIdMethod.DETERMINISTIC,
              null);
      p.apply(Create.of(ImmutableList.of(DATA)))
          .apply(ParDo.of(new Stamp(null /* attributes */)))
          .apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  @Test
  public void testDynamicTopics() throws IOException {
    List<OutgoingMessage> outgoing =
        ImmutableList.of(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA + "0"))
                    .build(),
                TIMESTAMP,
                getRecordId(DATA + "0"),
                "topic1"),
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA + "1"))
                    .build(),
                TIMESTAMP + 1,
                getRecordId(DATA + "1"),
                "topic1"),
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA + "2"))
                    .build(),
                TIMESTAMP + 2,
                getRecordId(DATA + "2"),
                "topic2"),
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(DATA + "3"))
                    .build(),
                TIMESTAMP + 3,
                getRecordId(DATA + "3"),
                "topic2"));
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(null, outgoing, ImmutableList.of())) {
      PubsubUnboundedSink sink =
          new PubsubUnboundedSink(
              factory,
              null,
              TIMESTAMP_ATTRIBUTE,
              ID_ATTRIBUTE,
              NUM_SHARDS,
              1 /* batchSize */,
              1 /* batchBytes */,
              Duration.standardSeconds(2),
              RecordIdMethod.DETERMINISTIC,
              null);

      List<TimestampedValue<PubsubMessage>> pubsubMessages =
          outgoing.stream()
              .map(
                  o ->
                      TimestampedValue.of(
                          new PubsubMessage(o.getMessage().getData().toByteArray(), null)
                              .withTopic(o.topic()),
                          Instant.ofEpochMilli(o.getTimestampMsSinceEpoch())))
              .collect(Collectors.toList());

      p.apply(Create.timestamped(pubsubMessages).withCoder(PubsubMessageWithTopicCoder.of()))
          .apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  @Test
  public void sendMoreThanOneBatchByNumMessages() throws IOException {
    List<OutgoingMessage> outgoing = new ArrayList<>();
    List<String> data = new ArrayList<>();
    int batchSize = 2;
    int batchBytes = 1000;
    for (int i = 0; i < batchSize * 10; i++) {
      String str = String.valueOf(i);
      outgoing.add(
          OutgoingMessage.of(
              com.google.pubsub.v1.PubsubMessage.newBuilder()
                  .setData(ByteString.copyFromUtf8(str))
                  .build(),
              TIMESTAMP,
              getRecordId(str),
              null));
      data.add(str);
    }
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(TOPIC, outgoing, ImmutableList.of())) {
      PubsubUnboundedSink sink =
          new PubsubUnboundedSink(
              factory,
              StaticValueProvider.of(TOPIC),
              TIMESTAMP_ATTRIBUTE,
              ID_ATTRIBUTE,
              NUM_SHARDS,
              batchSize,
              batchBytes,
              Duration.standardSeconds(2),
              RecordIdMethod.DETERMINISTIC,
              null);
      p.apply(Create.of(data)).apply(ParDo.of(new Stamp())).apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  @Test
  public void sendMoreThanOneBatchByByteSize() throws IOException {
    List<OutgoingMessage> outgoing = new ArrayList<>();
    List<String> data = new ArrayList<>();
    int batchSize = 100;
    int batchBytes = 10;
    int n = 0;
    while (n < batchBytes * 10) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < batchBytes; i++) {
        sb.append(String.valueOf(n));
      }
      String str = sb.toString();
      outgoing.add(
          OutgoingMessage.of(
              com.google.pubsub.v1.PubsubMessage.newBuilder()
                  .setData(ByteString.copyFromUtf8(str))
                  .build(),
              TIMESTAMP,
              getRecordId(str),
              null));
      data.add(str);
      n += str.length();
    }
    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(TOPIC, outgoing, ImmutableList.of())) {
      PubsubUnboundedSink sink =
          new PubsubUnboundedSink(
              factory,
              StaticValueProvider.of(TOPIC),
              TIMESTAMP_ATTRIBUTE,
              ID_ATTRIBUTE,
              NUM_SHARDS,
              batchSize,
              batchBytes,
              Duration.standardSeconds(2),
              RecordIdMethod.DETERMINISTIC,
              null);
      p.apply(Create.of(data)).apply(ParDo.of(new Stamp())).apply(sink);
      p.run();
    }
    // The PubsubTestClientFactory will assert fail on close if the actual published
    // message does not match the expected publish message.
  }

  // TODO: We would like to test that failed Pubsub publish calls cause the already assigned
  // (and random) record ids to be reused. However that can't be done without the test runnner
  // supporting retrying bundles.
}
