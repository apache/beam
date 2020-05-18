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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

/** A source description which includes metadata for reading from Kafka. */
@AutoValue
public abstract class KafkaSourceDescription implements Serializable {

  public static final Long UNINITIALIZED_START_OFFSET = -1L;
  public static final Instant UNINITIALIZED_START_TIME = Instant.EPOCH;

  abstract TopicPartition getTopicPartition();

  abstract Instant getStartReadTime();

  abstract Long getStartOffset();

  public static KafkaSourceDescription of(TopicPartition topicPartition) {
    return new AutoValue_KafkaSourceDescription(
        topicPartition, UNINITIALIZED_START_TIME, UNINITIALIZED_START_OFFSET);
  }

  public static KafkaSourceDescription withStartReadTime(
      TopicPartition topicPartition, Instant timestamp) {
    checkArgument(
        !timestamp.equals(UNINITIALIZED_START_TIME),
        "startReadTime should be greater than Instant.EPOCH");
    return new AutoValue_KafkaSourceDescription(
        topicPartition, timestamp, UNINITIALIZED_START_OFFSET);
  }

  public static KafkaSourceDescription withStartOffset(
      TopicPartition topicPartition, long startOffset) {
    checkArgument(startOffset >= 0, "startOffset should be greater or equal to 0");
    return new AutoValue_KafkaSourceDescription(
        topicPartition, UNINITIALIZED_START_TIME, startOffset);
  }

  public static class Coder extends StructuredCoder<KafkaSourceDescription> {

    @Override
    public void encode(KafkaSourceDescription kafkaSourceDescription, OutputStream outputStream)
        throws IOException {
      StringUtf8Coder.of().encode(kafkaSourceDescription.getTopicPartition().topic(), outputStream);
      VarIntCoder.of().encode(kafkaSourceDescription.getTopicPartition().partition(), outputStream);
      InstantCoder.of().encode(kafkaSourceDescription.getStartReadTime(), outputStream);
      BigEndianLongCoder.of().encode(kafkaSourceDescription.getStartOffset(), outputStream);
    }

    @Override
    public KafkaSourceDescription decode(InputStream inStream) throws IOException {
      String topic = StringUtf8Coder.of().decode(inStream);
      int partition = VarIntCoder.of().decode(inStream);
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      Instant startReadTime = InstantCoder.of().decode(inStream);
      Long startOffset = BigEndianLongCoder.of().decode(inStream);
      if (!startReadTime.equals(UNINITIALIZED_START_TIME)) {
        return KafkaSourceDescription.withStartReadTime(topicPartition, startReadTime);
      } else if (!startOffset.equals(UNINITIALIZED_START_OFFSET)) {
        return KafkaSourceDescription.withStartOffset(topicPartition, startOffset);
      }
      return KafkaSourceDescription.of(topicPartition);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public void registerByteSizeObserver(
        KafkaSourceDescription value, ElementByteSizeObserver observer) throws Exception {
      StringUtf8Coder.of().registerByteSizeObserver(value.getTopicPartition().topic(), observer);
      VarIntCoder.of().registerByteSizeObserver(value.getTopicPartition().partition(), observer);
      InstantCoder.of().registerByteSizeObserver(value.getStartReadTime(), observer);
      BigEndianLongCoder.of().registerByteSizeObserver(value.getStartOffset(), observer);
    }
  }
}
