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
package org.apache.beam.sdk.io.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * RabbitMqIO documents "using a queue" vs "using an exchange", but AMQP always interacts with an
 * exchange. The 'queue' semantics only make sense for a "direct exchange" where the routing key and
 * queue name match.
 *
 * <p>To facilitate the many combinations of queue bindings, routing keys, and exchange declarations
 * that could be used, this class has been implemented to help represent the parameters of a test
 * oriented around reading messages published to an exchange.
 */
class ExchangeTestPlan {
  static final String DEFAULT_ROUTING_KEY = "someRoutingKey";

  private final RabbitMqIO.Read read;
  private final int numRecords;
  private final int numRecordsToPublish;
  private final AMQP.@Nullable BasicProperties publishProperties;

  public ExchangeTestPlan(RabbitMqIO.Read read, int maxRecordsRead) {
    this(read, maxRecordsRead, maxRecordsRead);
  }

  public ExchangeTestPlan(RabbitMqIO.Read read, int maxRecordsRead, int numRecordsToPublish) {
    this(read, maxRecordsRead, numRecordsToPublish, null);
  }

  /**
   * @param read Read semantics to use for a test
   * @param maxRecordsRead Maximum messages to be processed by Beam within a test
   * @param numRecordsToPublish Number of messages that will be published to the exchange as part of
   *     a test. Note that this will frequently be the same value as {@code numRecordsRead} in which
   *     case it's simpler to use {@link #ExchangeTestPlan(RabbitMqIO.Read, int)}, but when testing
   *     topic exchanges or exchanges where not all messages will be routed to the queue being read
   *     from, these numbers will differ.
   * @param publishProperties AMQP Properties to be used when publishing
   */
  public ExchangeTestPlan(
      RabbitMqIO.Read read,
      int maxRecordsRead,
      int numRecordsToPublish,
      AMQP.@Nullable BasicProperties publishProperties) {
    this.read = read;
    this.numRecords = maxRecordsRead;
    this.numRecordsToPublish = numRecordsToPublish;
    this.publishProperties = publishProperties;
  }

  public RabbitMqIO.Read getRead() {
    return this.read;
  }

  public int getNumRecords() {
    return numRecords;
  }

  public int getNumRecordsToPublish() {
    return numRecordsToPublish;
  }

  public AMQP.BasicProperties getPublishProperties() {
    return publishProperties;
  }

  /**
   * @return The routing key to be used for an arbitrary message to be published to the exchange for
   *     a test. The default implementation uses a fixed value of {@link #DEFAULT_ROUTING_KEY}
   */
  public Supplier<String> publishRoutingKeyGen() {
    return () -> DEFAULT_ROUTING_KEY;
  }

  /** @return The expected parsed (String) messages read from the queue during the test. */
  public List<String> expectedResults() {
    return RabbitMqTestUtils.generateRecords(numRecordsToPublish).stream()
        .map(RabbitMqTestUtils::recordToString)
        .collect(Collectors.toList());
  }
}
