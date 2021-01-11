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

import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * I/O transforms for reading from Google Pub/Sub Lite.
 *
 * <p>For the differences between this and Google Pub/Sub, please refer to the <a
 * href="https://cloud.google.com/pubsub/docs/choosing-pubsub-or-lite">product documentation</a>.
 */
@Experimental
public final class PubsubLiteIO {
  private PubsubLiteIO() {}

  /**
   * Read messages from Pub/Sub Lite. These messages may contain duplicates if the publisher
   * retried, which the PubsubLiteIO write method will do. Use the dedupe transform to remove these
   * duplicates.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * SubscriptionPath subscriptionPath =
   *         SubscriptionPath.newBuilder()
   *             .setLocation(zone)
   *             .setProjectNumber(projectNum)
   *             .setName(subscriptionName)
   *             .build();
   *
   * FlowControlSettings flowControlSettings =
   *         FlowControlSettings.builder()
   *             // Set outstanding bytes to 10 MiB per partition.
   *             .setBytesOutstanding(10 * 1024 * 1024L)
   *             .setMessagesOutstanding(Long.MAX_VALUE)
   *             .build();
   *
   * PCollection<SequencedMessage> messages = p.apply(PubsubLiteIO.read(SubscriberOptions.newBuilder()
   *     .setSubscriptionPath(subscriptionPath)
   *     .setFlowControlSettings(flowControlSettings)
   *     .build()), "read");
   * }</pre>
   */
  public static Read.Unbounded<SequencedMessage> read(SubscriberOptions options) {
    return Read.from(new PubsubLiteUnboundedSource(options));
  }

  /**
   * Remove duplicates from the PTransform from a read. Assumes by default that the uuids were added
   * by a call to PubsubLiteIO.addUuids() when published.
   *
   * <pre>{@code
   * PCollection<SequencedMessage> messages = ... (above) ...;
   * messages = messages.apply(PubsubLiteIO.deduplicate(
   *     UuidDeduplicationOptions.newBuilder().build()));
   *
   * }</pre>
   */
  public static PTransform<PCollection<SequencedMessage>, PCollection<SequencedMessage>>
      deduplicate(UuidDeduplicationOptions options) {
    return new UuidDeduplicationTransform(options);
  }

  /**
   * Add Uuids to to-be-published messages that ensures that uniqueness is maintained.
   *
   * <pre>{@code
   * PCollection<Message> messages = ...;
   * messages = messages.apply(PubsubLiteIO.addUuids());
   *
   * }</pre>
   */
  public static PTransform<PCollection<PubSubMessage>, PCollection<PubSubMessage>> addUuids() {
    return new AddUuidsTransform();
  }

  /**
   * Write messages to Pub/Sub Lite.
   *
   * <pre>{@code
   * TopicPath topicPath =
   *         TopicPath.newBuilder()
   *             .setProjectNumber(projectNum)
   *             .setLocation(zone)
   *             .setName(topicName)
   *             .build();
   *
   * PCollection<Message> messages = ...;
   * messages.apply(PubsubLiteIO.write(
   *     PublisherOptions.newBuilder().setTopicPath(topicPath).build());
   *
   * }</pre>
   */
  public static PTransform<PCollection<PubSubMessage>, PDone> write(PublisherOptions options) {
    return new PTransform<PCollection<PubSubMessage>, PDone>("PubsubLiteIO") {
      @Override
      public PDone expand(PCollection<PubSubMessage> input) {
        PubsubLiteSink sink = new PubsubLiteSink(options);
        input.apply("Write", ParDo.of(sink));
        return PDone.in(input.getPipeline());
      }
    };
  }
}
