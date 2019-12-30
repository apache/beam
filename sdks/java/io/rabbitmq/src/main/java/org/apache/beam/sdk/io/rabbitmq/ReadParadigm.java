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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.rabbitmq.client.BuiltinExchangeType;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * RabbitMQ/AMQP is a multi-paradigm message broker system. This class and implementations help
 * enforce invariants in Beam-compatible ways of using RabbitMQ.
 *
 * <p>In general, there are two approaches here:
 *
 * <ol>
 *   <li>ExistingQueue - if a queue already exists, it can be read from with no other configuration
 *   <li>NewQueue - if a queue may need to be declared and bound along with a routing key in some
 *       supported exchange type
 * </ol>
 *
 * In general, it's safest to define the queue and binding (routing key) outside of beam, so that
 * messages begin queueing up even before Beam runs and using a durable, non-auto-delete queue with
 * persistent messages such that no data is lost. However it can be useful to allow Beam to declare
 * a queue and bind to it in an idempotent fashion.
 *
 * <p>Some known limitations of these approaches:
 *
 * <ul>
 *   <li>RabbitMqIO intentionally does not support exchange declaration. This feels out of scope and
 *       there's unlikely a good use case for having beam declare the exchange along with the queue.
 *   <li>RabbitMqIO requires a queue name to be specified. Ephemeral queues (ones which auto-delete
 *       after the client disconnects) are not very Beam-friendly as clients can be spun up and down
 *       among many connections throughout the lifecycle of a pipeline. Further, generating a random
 *       queue name would be difficult to manage across multiple readers.
 *   <li>Headers exchanges are not supported. Only exchanges using routing keys are supported at
 *       this time, including the default exchange.
 * </ul>
 */
public abstract class ReadParadigm implements Serializable {
  private ReadParadigm() {}

  public abstract String queueName();

  /**
   * Specifies that RabbitMqIO should read from an existing queue. This is the safest and simplest
   * setup and is recommended whenever possible.
   */
  public static class ExistingQueue extends ReadParadigm {
    private final String queueName;

    private ExistingQueue(String queueName) {
      this.queueName = queueName;
    }

    public static ExistingQueue of(String queueName) {
      checkArgument(
          queueName != null && !queueName.trim().isEmpty(),
          "Non-empty queue name is required to read from an existing queue");
      return new ExistingQueue(queueName);
    }

    public String getQueueName() {
      return queueName;
    }

    @Override
    public String queueName() {
      return getQueueName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExistingQueue that = (ExistingQueue) o;
      return queueName.equals(that.queueName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queueName);
    }
  }

  /**
   * Specifies that RabbitMqIO should declare a queue and bind it to the specified exchange via the
   * specified routing key.
   *
   * <p>If the specified queue already exists, and is durable, non-auto-deleted, and non-exclusive,
   * queue declaration will have no effect, but specified routing key will replace any previously
   * defined routing key binding the queue to the exchange. Note the routing key has no impact for
   * direct exchanges or fanout exchanges. For a direct exchange, the queue name will be used as the
   * routing key, and for fanout exchanges the routing key is unused (all queues receive all
   * messages).
   */
  public static class NewQueue extends ReadParadigm {
    private final String exchange;
    private final BuiltinExchangeType exchangeType;
    private final String queueName;
    private final String routingKey;

    private NewQueue(
        String exchange,
        BuiltinExchangeType exchangeType,
        String queueName,
        @Nullable String routingKey) {
      checkArgument(
          queueName != null && !queueName.trim().isEmpty(), "A queue name must be provided");
      this.exchange = exchange;
      this.exchangeType = exchangeType;
      this.queueName = queueName.trim();
      this.routingKey =
          Optional.ofNullable(routingKey).map(String::trim).filter(s -> !s.isEmpty()).orElse("");
    }

    /**
     * Specifies use of a particular topic exchange, bounding messages to the specified queue using
     * the specified routingKey.
     *
     * @param exchangeName the name of the (pre-existing) topic exchange to use
     * @param queueName the name of the queue to read from
     * @param routingKey the topic pattern to bind incoming messages on the exchange to the
     *     specified queue
     */
    public static NewQueue topicExchange(String exchangeName, String queueName, String routingKey) {
      return new NewQueue(exchangeName, BuiltinExchangeType.TOPIC, queueName, routingKey);
    }

    /**
     * Specifies the use of a particular fanout exchange.
     *
     * <p>All messages published to the given exchange will end up in the specified queue.
     *
     * @param exchangeName the name of the (pre-existing) fanout exchange to use
     * @param queueName the name of the queue to read from and bind all of the exchange's messages
     *     to
     */
    public static NewQueue fanoutExchange(String exchangeName, String queueName) {
      return new NewQueue(exchangeName, BuiltinExchangeType.FANOUT, queueName, "");
    }

    /**
     * Specifies the use of a particular direct exchange.
     *
     * <p>In a direct exchange, only messages published with a routing key equal to the supplied
     * queue name will be routed to the queue.
     *
     * <p>To use the default exchange, use {@link #defaultExchange(String)} instead.
     *
     * @param exchangeName the name of the (pre-existing) direct exchange to use
     * @param queueName the name of the queue to read from
     */
    public static NewQueue directExchange(String exchangeName, String queueName) {
      return new NewQueue(exchangeName, BuiltinExchangeType.DIRECT, queueName, queueName);
    }

    /**
     * Specifies use of a new queue on the default (direct) exchange.
     *
     * <p>Note this will require no queue bindings as the default exchange will route any incoming
     * message with a routing key equal to the given queue name to said queue.
     *
     * @param queueName the name of the queue to read from
     */
    public static NewQueue defaultExchange(String queueName) {
      return directExchange("", queueName);
    }

    public String getExchange() {
      return exchange;
    }

    public BuiltinExchangeType getExchangeType() {
      return exchangeType;
    }

    public String getQueueName() {
      return queueName;
    }

    public String getRoutingKey() {
      return routingKey;
    }

    @Override
    public String queueName() {
      return getQueueName();
    }

    public boolean isDefaultExchange() {
      return "".equals(exchange);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NewQueue newQueue = (NewQueue) o;
      return exchange.equals(newQueue.exchange)
          && exchangeType.equals(newQueue.exchangeType)
          && queueName.equals(newQueue.queueName)
          && routingKey.equals(newQueue.routingKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(exchange, exchangeType, queueName, routingKey);
    }
  }
}
