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
package org.apache.beam.sdk.io.kafka;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaSourceDescriptionTest {
  @Rule public final ExpectedException expected = ExpectedException.none();

  @Test
  public void testInitializationWithTopicPartition() throws Exception {
    TopicPartition topicPartition = new TopicPartition("test_topic", 1);
    KafkaSourceDescription description =
        KafkaSourceDescription.of(new TopicPartition("test_topic", 1));
    assertEquals(topicPartition, description.getTopicPartition());
    assertEquals(KafkaSourceDescription.UNINITIALIZED_START_OFFSET, description.getStartOffset());
    assertEquals(KafkaSourceDescription.UNINITIALIZED_START_TIME, description.getStartReadTime());
  }

  @Test
  public void testInitializationWithStartOffset() throws Exception {
    TopicPartition topicPartition = new TopicPartition("test_topic", 1);
    KafkaSourceDescription description =
        KafkaSourceDescription.withStartOffset(topicPartition, 10L);
    assertEquals(topicPartition, description.getTopicPartition());
    assertEquals(10L, description.getStartOffset().longValue());
    assertEquals(KafkaSourceDescription.UNINITIALIZED_START_TIME, description.getStartReadTime());

    expected.expect(IllegalArgumentException.class);
    description = KafkaSourceDescription.withStartOffset(topicPartition, -1);
  }

  @Test
  public void testInitializationWithStartTime() throws Exception {
    TopicPartition topicPartition = new TopicPartition("test_topic", 1);
    KafkaSourceDescription description =
        KafkaSourceDescription.withStartReadTime(topicPartition, Instant.ofEpochMilli(1000L));
    assertEquals(topicPartition, description.getTopicPartition());
    assertEquals(Instant.ofEpochMilli(1000L), description.getStartReadTime());
    assertEquals(KafkaSourceDescription.UNINITIALIZED_START_OFFSET, description.getStartOffset());

    expected.expect(IllegalArgumentException.class);
    description = KafkaSourceDescription.withStartReadTime(topicPartition, Instant.EPOCH);
  }

  @Test
  public void testKafkaSourceDescriptionEncodeAndDecode() throws Exception {
    TopicPartition topicPartition = new TopicPartition("test_topic", 1);
    KafkaSourceDescription.Coder coder = new KafkaSourceDescription.Coder();

    KafkaSourceDescription description = KafkaSourceDescription.of(topicPartition);
    byte[] encodedBytes = CoderUtils.encodeToByteArray(coder, description);
    KafkaSourceDescription decodedDescription = CoderUtils.decodeFromByteArray(coder, encodedBytes);
    assertEquals(description.getTopicPartition(), decodedDescription.getTopicPartition());
    assertEquals(decodedDescription.getStartOffset(), description.getStartOffset());
    assertEquals(decodedDescription.getStartReadTime(), description.getStartReadTime());

    description =
        KafkaSourceDescription.withStartReadTime(topicPartition, Instant.ofEpochMilli(1000L));
    encodedBytes = CoderUtils.encodeToByteArray(coder, description);
    decodedDescription = CoderUtils.decodeFromByteArray(coder, encodedBytes);
    assertEquals(description.getTopicPartition(), decodedDescription.getTopicPartition());
    assertEquals(decodedDescription.getStartOffset(), description.getStartOffset());
    assertEquals(decodedDescription.getStartReadTime(), description.getStartReadTime());

    description = KafkaSourceDescription.withStartOffset(topicPartition, 10L);
    encodedBytes = CoderUtils.encodeToByteArray(coder, description);
    decodedDescription = CoderUtils.decodeFromByteArray(coder, encodedBytes);
    assertEquals(description.getTopicPartition(), decodedDescription.getTopicPartition());
    assertEquals(decodedDescription.getStartOffset(), description.getStartOffset());
    assertEquals(decodedDescription.getStartReadTime(), description.getStartReadTime());
  }
}
