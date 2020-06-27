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

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UuidDeduplicationTransformTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  private static final Instant START = new Instant(0);

  private static SequencedMessage newMessage() {
    Uuid uuid = Uuid.random();
    return SequencedMessage.newBuilder()
        .setMessage(
            PubSubMessage.newBuilder()
                .putAttributes(
                    Uuid.DEFAULT_ATTRIBUTE,
                    AttributeValues.newBuilder().addValues(uuid.value()).build()))
        .setSizeBytes(10000)
        .setPublishTime(Timestamps.EPOCH)
        .setCursor(Cursor.newBuilder().setOffset(10))
        .build();
  }

  @Test
  public void unrelatedUuidsProxied() {
    SequencedMessage message1 = newMessage();
    SequencedMessage message2 = newMessage();

    TestStream<SequencedMessage> messageStream =
        TestStream.create(ProtoCoder.of(SequencedMessage.class))
            .advanceWatermarkTo(START)
            .addElements(message1)
            .advanceWatermarkTo(START.plus(Deduplicate.DEFAULT_DURATION.dividedBy(2)))
            .addElements(message2)
            .advanceWatermarkToInfinity();
    PCollection<SequencedMessage> results =
        pipeline
            .apply(messageStream)
            .apply(
                new UuidDeduplicationTransform(
                    UuidDeduplicationOptions.newBuilder().setHashPartitions(1).build()));
    PAssert.that(results).containsInAnyOrder(message1, message2);
    pipeline.run();
  }

  @Test
  public void sameUuidsWithinWindowOnlyOne() {
    SequencedMessage message = newMessage();

    TestStream<SequencedMessage> messageStream =
        TestStream.create(ProtoCoder.of(SequencedMessage.class))
            .advanceWatermarkTo(START)
            .addElements(message)
            .advanceWatermarkTo(START.plus(Deduplicate.DEFAULT_DURATION.dividedBy(2)))
            .advanceWatermarkToInfinity();
    PCollection<SequencedMessage> results =
        pipeline
            .apply(messageStream)
            .apply(
                new UuidDeduplicationTransform(
                    UuidDeduplicationOptions.newBuilder().setHashPartitions(1).build()));
    PAssert.that(results).containsInAnyOrder(message);
    pipeline.run();
  }

  @Test
  public void sameUuidsAfterGcOutsideWindowHasBoth() {
    SequencedMessage message1 = newMessage();

    TestStream<SequencedMessage> messageStream =
        TestStream.create(ProtoCoder.of(SequencedMessage.class))
            .advanceWatermarkTo(START)
            .addElements(message1)
            .advanceWatermarkTo(
                START.plus(
                    UuidDeduplicationOptions.DEFAULT_DEDUPLICATE_DURATION.plus(Duration.millis(1))))
            .addElements(message1)
            .advanceWatermarkToInfinity();
    PCollection<SequencedMessage> results =
        pipeline
            .apply(messageStream)
            .apply(
                new UuidDeduplicationTransform(
                    UuidDeduplicationOptions.newBuilder().setHashPartitions(1).build()));
    PAssert.that(results).containsInAnyOrder(message1, message1);
    pipeline.run();
  }

  @Test
  public void dedupesBasedOnReturnedUuid() {
    byte[] bytes = {(byte) 0x123, (byte) 0x456};
    // These messages have different uuids, so they would both appear in the output collection if
    // the extractor is not respected.
    SequencedMessage message1 = newMessage();
    SequencedMessage message2 = newMessage();

    TestStream<SequencedMessage> messageStream =
        TestStream.create(ProtoCoder.of(SequencedMessage.class))
            .advanceWatermarkTo(START)
            .addElements(message1, message2)
            .advanceWatermarkToInfinity();
    PCollection<SequencedMessage> results =
        pipeline
            .apply(messageStream)
            .apply(
                new UuidDeduplicationTransform(
                    UuidDeduplicationOptions.newBuilder()
                        .setHashPartitions(1)
                        .setUuidExtractor(message -> Uuid.of(ByteString.copyFrom(bytes)))
                        .build()));
    PAssert.that(results)
        .satisfies(
            messages -> {
              Preconditions.checkArgument(Iterables.size(messages) == 1);
              return null;
            });
    pipeline.run();
  }
}
