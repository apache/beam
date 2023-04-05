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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * ProducerSpEL to handle newer versions Producer API. The API is updated in Kafka 0.11 to support
 * exactly-once semantics.
 */
class ProducerSpEL {

  private static class TransactionsImplementation {
    private Method initTransactionsMethod;
    private Method beginTransactionMethod;
    private Method commitTransactionMethod;
    private Method abortTransactionMethod;
    private Method sendOffsetsToTransactionMethod;
    private Class<?> producerFencedExceptionClass;
    private Class<?> outOfOrderSequenceExceptionClass;

    private TransactionsImplementation() throws NoSuchMethodException, ClassNotFoundException {
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
    }

    private void initTransactions(Producer<?, ?> producer) {
      invoke(initTransactionsMethod, producer);
    }

    private void beginTransaction(Producer<?, ?> producer) {
      invoke(beginTransactionMethod, producer);
    }

    private void commitTransaction(Producer<?, ?> producer) {
      invoke(commitTransactionMethod, producer);
    }

    private void abortTransaction(Producer<?, ?> producer) {
      invoke(abortTransactionMethod, producer);
    }

    private void sendOffsetsToTransaction(
        Producer<?, ?> producer,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        @Nullable String consumerGroupId) {
      invoke(sendOffsetsToTransactionMethod, producer, offsets, consumerGroupId);
    }

    private void invoke(Method method, Object obj, @Nullable Object... args) {
      try {
        @SuppressWarnings({"nullness", "unused"}) // JDK annotation does not allow the nulls
        Object ignored = method.invoke(obj, args);
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
  }

  private static @Nullable TransactionsImplementation transactionsImplementation;

  static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
  static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";

  static {
    try {
      transactionsImplementation = new TransactionsImplementation();
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      transactionsImplementation = null;
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
    return transactionsImplementation != null;
  }

  private static TransactionsImplementation getTransactionsImplementation() {
    return Preconditions.checkStateNotNull(
        transactionsImplementation,
        "This version of Kafka client library does not support transactions."
            + " Please used version 0.11 or later.");
  }

  static void initTransactions(Producer<?, ?> producer) {
    getTransactionsImplementation().initTransactions(producer);
  }

  static void beginTransaction(Producer<?, ?> producer) {
    getTransactionsImplementation().beginTransaction(producer);
  }

  static void commitTransaction(Producer<?, ?> producer) {
    getTransactionsImplementation().commitTransaction(producer);
  }

  static void abortTransaction(Producer<?, ?> producer) {
    getTransactionsImplementation().abortTransaction(producer);
  }

  static void sendOffsetsToTransaction(
      Producer<?, ?> producer,
      Map<TopicPartition, OffsetAndMetadata> offsets,
      @Nullable String consumerGroupId) {
    getTransactionsImplementation().sendOffsetsToTransaction(producer, offsets, consumerGroupId);
  }
}
