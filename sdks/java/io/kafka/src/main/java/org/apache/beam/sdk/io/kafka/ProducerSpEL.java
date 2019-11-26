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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthorizationException;

/**
 * ProducerSpEL to handle newer versions Producer API. The API is updated in Kafka 0.11 to support
 * exactly-once semantics.
 */
class ProducerSpEL {

  private static boolean supportsTransactions;

  private static Method initTransactionsMethod;
  private static Method beginTransactionMethod;
  private static Method commitTransactionMethod;
  private static Method abortTransactionMethod;
  private static Method sendOffsetsToTransactionMethod;

  static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
  static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";

  private static Class<?> producerFencedExceptionClass;
  private static Class<?> outOfOrderSequenceExceptionClass;

  static {
    try {
      initTransactionsMethod = Producer.class.getMethod("initTransactions");
      beginTransactionMethod = Producer.class.getMethod("beginTransaction");
      commitTransactionMethod = Producer.class.getMethod("commitTransaction");
      abortTransactionMethod = Producer.class.getMethod("abortTransaction");
      sendOffsetsToTransactionMethod =
          Producer.class.getMethod("sendOffsetsToTransaction", Map.class, String.class);

      producerFencedExceptionClass =
          Class.forName("org.apache.kafka.common.errors.ProducerFencedException");
      outOfOrderSequenceExceptionClass =
          Class.forName("org.apache.kafka.common.errors.OutOfOrderSequenceException");

      supportsTransactions = true;
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      supportsTransactions = false;
    }
  }

  /**
   * Wraps an unrecoverable producer exceptions, including the ones related transactions introduced
   * in 0.11 (as described in documentation for {@link Producer}). The calller should close the
   * producer when this exception is thrown.
   */
  static class UnrecoverableProducerException extends ApiException {
    UnrecoverableProducerException(ApiException cause) {
      super(cause);
    }
  }

  static boolean supportsTransactions() {
    return supportsTransactions;
  }

  private static void ensureTransactionsSupport() {
    checkArgument(
        supportsTransactions(),
        "This version of Kafka client library does not support transactions. ",
        "Please used version 0.11 or later.");
  }

  private static void invoke(Method method, Object obj, Object... args) {
    try {
      method.invoke(obj, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (ApiException e) {
      Class<?> eClass = e.getClass();
      if (producerFencedExceptionClass.isAssignableFrom(eClass)
          || outOfOrderSequenceExceptionClass.isAssignableFrom(eClass)
          || AuthorizationException.class.isAssignableFrom(eClass)) {
        throw new UnrecoverableProducerException(e);
      }
      throw e;
    }
  }

  static void initTransactions(Producer<?, ?> producer) {
    ensureTransactionsSupport();
    invoke(initTransactionsMethod, producer);
  }

  static void beginTransaction(Producer<?, ?> producer) {
    ensureTransactionsSupport();
    invoke(beginTransactionMethod, producer);
  }

  static void commitTransaction(Producer<?, ?> producer) {
    ensureTransactionsSupport();
    invoke(commitTransactionMethod, producer);
  }

  static void abortTransaction(Producer<?, ?> producer) {
    ensureTransactionsSupport();
    invoke(abortTransactionMethod, producer);
  }

  static void sendOffsetsToTransaction(
      Producer<?, ?> producer,
      Map<TopicPartition, OffsetAndMetadata> offsets,
      String consumerGroupId) {
    ensureTransactionsSupport();
    invoke(sendOffsetsToTransactionMethod, producer, offsets, consumerGroupId);
  }
}
