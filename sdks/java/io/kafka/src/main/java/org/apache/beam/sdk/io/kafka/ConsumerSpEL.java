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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

  private Method timestampMethod;
  private boolean hasRecordTimestamp = false;

  private Method offsetGetterMethod;
  private Method offsetsForTimesMethod;
  private boolean hasOffsetsForTimes = false;

  public ConsumerSpEL() {
    try {
      // It is supported by Kafka Client 0.10.0.0 onwards.
      timestampMethod = ConsumerRecord.class.getMethod("timestamp", (Class<?>[]) null);
      hasRecordTimestamp = timestampMethod.getReturnType().equals(Long.TYPE);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Timestamp for Kafka message is not available.");
    }

    try {
      // It is supported by Kafka Client 0.10.1.0 onwards.
      offsetGetterMethod = Class.forName("org.apache.kafka.clients.consumer.OffsetAndTimestamp")
          .getMethod("offset", (Class<?>[]) null);
      offsetsForTimesMethod = Consumer.class.getMethod("offsetsForTimes", Map.class);
      hasOffsetsForTimes = offsetsForTimesMethod.getReturnType().equals(Map.class);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      LOG.debug("OffsetsForTimes is not available.");
    }
  }

  public void evaluateSeek2End(Consumer consumer, TopicPartition topicPartitions) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartitions);
    seek2endExpression.getValue(mapContext);
  }

  public void evaluateAssign(Consumer consumer, Collection<TopicPartition> topicPartitions) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartitions);
    assignExpression.getValue(mapContext);
  }

  public long getRecordTimestamp(ConsumerRecord<byte[], byte[]> rawRecord) {
    long timestamp;
    try {
      //for Kafka 0.9, set to System.currentTimeMillis();
      //for kafka 0.10, when NO_TIMESTAMP also set to System.currentTimeMillis();
      if (!hasRecordTimestamp || (timestamp = (long) timestampMethod.invoke(rawRecord)) <= 0L) {
        timestamp = System.currentTimeMillis();
      }
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      // Not expected. Method timestamp() is already checked.
      throw new RuntimeException(e);
    }
    return timestamp;
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

    Map<TopicPartition, Long> timestampsToSearch =
        ImmutableMap.of(topicPartition, time.getMillis());
    try {
      Map offsetsByTimes = (Map) offsetsForTimesMethod.invoke(consumer, timestampsToSearch);
      Object offsetAndTimestamp = Iterables.getOnlyElement(offsetsByTimes.values());

      if (offsetAndTimestamp == null) {
        throw new RuntimeException("There are no messages has a timestamp that is greater than or "
            + "equals to the target time or the message format version in this partition is "
            + "before 0.10.0, topicPartition is: " + topicPartition);
      } else {
        return (long) offsetGetterMethod.invoke(offsetAndTimestamp);
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }

  }

}
