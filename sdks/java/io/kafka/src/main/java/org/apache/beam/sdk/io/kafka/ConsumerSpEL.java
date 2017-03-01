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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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
  private boolean hasEventTimestamp = false;

  public ConsumerSpEL() {
    try {
      timestampMethod = ConsumerRecord.class.getMethod("timestamp", (Class<?>[]) null);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Timestamp for Kafka message is not available.");
    }
    hasEventTimestamp = hasTimestamp();
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

  private boolean hasTimestamp(){
    boolean hasEventTimestamp = false;
    try {
      hasEventTimestamp = timestampMethod != null
          && timestampMethod.getReturnType().equals(Long.class);
    } catch (SecurityException e) {
      hasEventTimestamp = false;
    }
    return hasEventTimestamp;
  }

  public long getEventTimestamp(ConsumerRecord<byte[], byte[]> rawRecord) {
    if (!hasEventTimestamp){
      return System.currentTimeMillis();
    }
    try {
      return (long) timestampMethod.invoke(rawRecord);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
