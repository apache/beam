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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * ConsumerSpEL to handle multiple of versions of Consumer API between Kafka 0.9 and 2.1.0 onwards.
 * It auto detects the input type List/Collection/Varargs, to eliminate the method definition
 * differences.
 */
class ConsumerSpEL {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerSpEL.class);

  private static final SpelParserConfiguration config = new SpelParserConfiguration(true, true);
  private static final ExpressionParser parser = new SpelExpressionParser(config);

  // This method changed from accepting varargs to accepting a Collection.
  private static final Expression seek2endExpression =
      parser.parseExpression("#consumer.seekToEnd(#tp)");
  // This method changed from accepting a List to accepting a Collection.
  private static final Expression assignExpression =
      parser.parseExpression("#consumer.assign(#tp)");

  private static boolean hasRecordTimestamp;
  private static boolean hasHeaders;
  private static boolean hasOffsetsForTimes;
  private static boolean deserializerSupportsHeaders;

  static {
    try {
      // It is supported by Kafka Client 0.10.0.0 onwards.
      hasRecordTimestamp =
          ConsumerRecord.class
              .getMethod("timestamp", (Class<?>[]) null)
              .getReturnType()
              .equals(Long.TYPE);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Timestamp for Kafka message is not available.");
    }

    try {
      // It is supported by Kafka Client 0.11.0.0 onwards.
      hasHeaders =
          "org.apache.kafka.common.header.Headers"
              .equals(
                  ConsumerRecord.class
                      .getMethod("headers", (Class<?>[]) null)
                      .getReturnType()
                      .getName());
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Headers is not available");
    }

    try {
      // It is supported by Kafka Client 0.10.1.0 onwards.
      hasOffsetsForTimes =
          Consumer.class.getMethod("offsetsForTimes", Map.class).getReturnType().equals(Map.class);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("OffsetsForTimes is not available.");
    }

    try {
      // It is supported by Kafka Client 2.1.0 onwards.
      deserializerSupportsHeaders =
          "T"
              .equals(
                  Deserializer.class
                      .getDeclaredMethod("deserialize", String.class, Headers.class, byte[].class)
                      .getGenericReturnType()
                      .getTypeName());
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Deserializer interface does not support Kafka headers");
    }
  }

  public static void evaluateSeek2End(Consumer<?, ?> consumer, TopicPartition topicPartition) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartition);
    seek2endExpression.getValue(mapContext);
  }

  public static void evaluateAssign(
      Consumer<?, ?> consumer, Collection<TopicPartition> topicPartitions) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartitions);
    assignExpression.getValue(mapContext);
  }

  public static long getRecordTimestamp(ConsumerRecord<byte[], byte[]> rawRecord) {
    if (hasRecordTimestamp) {
      return rawRecord.timestamp();
    }
    return -1L; // This is the timestamp used in Kafka for older messages without timestamps.
  }

  public static KafkaTimestampType getRecordTimestampType(
      ConsumerRecord<byte[], byte[]> rawRecord) {
    if (hasRecordTimestamp) {
      return KafkaTimestampType.forOrdinal(rawRecord.timestampType().ordinal());
    } else {
      return KafkaTimestampType.NO_TIMESTAMP_TYPE;
    }
  }

  public static boolean hasOffsetsForTimes() {
    return hasOffsetsForTimes;
  }

  public static boolean hasHeaders() {
    return hasHeaders;
  }

  public static boolean deserializerSupportsHeaders() {
    return deserializerSupportsHeaders;
  }

  public static <T> T deserializeKey(
      Deserializer<T> deserializer, ConsumerRecord<byte[], byte[]> rawRecord) {
    if (deserializerSupportsHeaders) {
      // Kafka API 2.1.0 onwards
      return deserializer.deserialize(rawRecord.topic(), rawRecord.headers(), rawRecord.key());
    } else {
      return deserializer.deserialize(rawRecord.topic(), rawRecord.key());
    }
  }

  public static <T> T deserializeValue(
      Deserializer<T> deserializer, ConsumerRecord<byte[], byte[]> rawRecord) {
    if (deserializerSupportsHeaders) {
      // Kafka API 2.1.0 onwards
      return deserializer.deserialize(rawRecord.topic(), rawRecord.headers(), rawRecord.value());
    } else {
      return deserializer.deserialize(rawRecord.topic(), rawRecord.value());
    }
  }

  /**
   * Look up the offset for the given partition by timestamp. Throws RuntimeException if there are
   * no messages later than timestamp or if this partition does not support timestamp based offset.
   */
  @SuppressWarnings("unchecked")
  public static long offsetForTime(
      Consumer<?, ?> consumer, TopicPartition topicPartition, Instant time) {

    checkArgument(hasOffsetsForTimes, "This Kafka Client must support Consumer.OffsetsForTimes().");

    // 'value' in the map returned by offsetFoTime() is null if there is no offset for the time.
    OffsetAndTimestamp offsetAndTimestamp =
        Iterables.getOnlyElement(
            consumer.offsetsForTimes(ImmutableMap.of(topicPartition, time.getMillis())).values());

    if (offsetAndTimestamp == null) {
      throw new RuntimeException(
          "There are no messages has a timestamp that is greater than or "
              + "equals to the target time or the message format version in this partition is "
              + "before 0.10.0, topicPartition is: "
              + topicPartition);
    } else {
      return offsetAndTimestamp.offset();
    }
  }
}
