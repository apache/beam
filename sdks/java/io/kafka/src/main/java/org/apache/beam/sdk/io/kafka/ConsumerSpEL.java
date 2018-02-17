/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * ConsumerSpEL to handle multiple of versions of Consumer API between Kafka 0.9 and 0.10.
 * It auto detects the input type List/Collection/Varargs,
 * to eliminate the method definition differences.
 */
class ConsumerSpEL {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerSpEL.class);

  private SpelParserConfiguration config = new SpelParserConfiguration(true, true);
  private ExpressionParser parser = new SpelExpressionParser(config);

  private Expression seek2endExpression =
      parser.parseExpression("#consumer.seekToEnd(#tp)");

  private Expression assignExpression =
      parser.parseExpression("#consumer.assign(#tp)");

  private boolean hasRecordTimestamp = false;
  private boolean hasOffsetsForTimes = false;

  public ConsumerSpEL() {
    try {
      // It is supported by Kafka Client 0.10.0.0 onwards.
      hasRecordTimestamp = ConsumerRecord
        .class
        .getMethod("timestamp", (Class<?>[]) null)
        .getReturnType()
        .equals(Long.TYPE);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Timestamp for Kafka message is not available.");
    }

    try {
      // It is supported by Kafka Client 0.10.1.0 onwards.
      hasOffsetsForTimes = Consumer
        .class
        .getMethod("offsetsForTimes", Map.class)
        .getReturnType()
        .equals(Map.class);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("OffsetsForTimes is not available.");
    }
  }

  public void evaluateSeek2End(Consumer consumer, TopicPartition topicPartition) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartition);
    seek2endExpression.getValue(mapContext);
  }

  public void evaluateAssign(Consumer consumer, Collection<TopicPartition> topicPartitions) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartitions);
    assignExpression.getValue(mapContext);
  }

  public long getRecordTimestamp(ConsumerRecord<byte[], byte[]> rawRecord) {
    if (hasRecordTimestamp) {
      return rawRecord.timestamp();
    }
    return -1L; // This is the timestamp used in Kafka for older messages without timestamps.
  }

  public KafkaTimestampType getRecordTimestamptType(
    ConsumerRecord<byte[], byte[]> rawRecord) {
    if (hasRecordTimestamp) {
      return KafkaTimestampType.forOrdinal(rawRecord.timestampType().ordinal());
    } else {
      return KafkaTimestampType.NO_TIMESTAMP_TYPE;
    }
  }

  public boolean hasOffsetsForTimes() {
    return hasOffsetsForTimes;
  }

  /**
   * Look up the offset for the given partition by timestamp.
   * Throws RuntimeException if there are no messages later than timestamp or if this partition
   * does not support timestamp based offset.
   */
  @SuppressWarnings("unchecked")
  public long offsetForTime(Consumer<?, ?> consumer, TopicPartition topicPartition, Instant time) {

    checkArgument(hasOffsetsForTimes,
        "This Kafka Client must support Consumer.OffsetsForTimes().");

    // 'value' in the map returned by offsetFoTime() is null if there is no offset for the time.
    OffsetAndTimestamp offsetAndTimestamp = Iterables.getOnlyElement(
      consumer
        .offsetsForTimes(ImmutableMap.of(topicPartition, time.getMillis()))
        .values());

    if (offsetAndTimestamp == null) {
      throw new RuntimeException("There are no messages has a timestamp that is greater than or "
          + "equals to the target time or the message format version in this partition is "
          + "before 0.10.0, topicPartition is: " + topicPartition);
    } else {
      return offsetAndTimestamp.offset();
    }
  }
}
