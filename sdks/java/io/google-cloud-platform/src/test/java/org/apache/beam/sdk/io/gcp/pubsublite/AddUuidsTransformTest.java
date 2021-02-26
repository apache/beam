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
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class AddUuidsTransformTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static PubSubMessage newMessage(int identifier) {
    return PubSubMessage.newBuilder()
        .setKey(ByteString.copyFromUtf8(Integer.toString(identifier)))
        .build();
  }

  private static SerializableFunction<Iterable<PubSubMessage>, Void> identifiersInAnyOrder(
      Set<Integer> identifiers) {
    return messages -> {
      Set<Uuid> uuids = new HashSet<>();
      messages.forEach(
          message -> {
            int identifier = Integer.parseInt(message.getKey().toStringUtf8());
            if (!identifiers.remove(identifier)) {
              throw new IllegalStateException("Duplicate element " + identifier);
            }
            if (!uuids.add(
                Uuid.of(
                    Iterables.getOnlyElement(
                        message.getAttributesMap().get(Uuid.DEFAULT_ATTRIBUTE).getValuesList())))) {
              throw new IllegalStateException("Invalid duplicate Uuid: " + message.toString());
            }
          });
      if (!identifiers.isEmpty()) {
        throw new IllegalStateException("Elements not in collection: " + identifiers);
      }
      return null;
    };
  }

  @Test
  public void messagesSameBatch() {
    TestStream<PubSubMessage> messageStream =
        TestStream.create(ProtoCoder.of(PubSubMessage.class))
            .addElements(newMessage(1), newMessage(2), newMessage(85))
            .advanceWatermarkToInfinity();
    PCollection<PubSubMessage> outputs =
        pipeline.apply(messageStream).apply(new AddUuidsTransform());
    PAssert.that(outputs).satisfies(identifiersInAnyOrder(Sets.newHashSet(1, 2, 85)));
    pipeline.run();
  }

  @Test
  public void messagesTimeDelayed() {
    TestStream<PubSubMessage> messageStream =
        TestStream.create(ProtoCoder.of(PubSubMessage.class))
            .addElements(newMessage(1), newMessage(2))
            .advanceProcessingTime(Duration.standardDays(1))
            .addElements(newMessage(85))
            .advanceWatermarkToInfinity();
    PCollection<PubSubMessage> outputs =
        pipeline.apply(messageStream).apply(new AddUuidsTransform());
    PAssert.that(outputs).satisfies(identifiersInAnyOrder(Sets.newHashSet(1, 2, 85)));
    pipeline.run();
  }
}
