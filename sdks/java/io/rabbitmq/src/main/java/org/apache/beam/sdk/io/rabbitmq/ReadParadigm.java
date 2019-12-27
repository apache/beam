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

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public abstract class ReadParadigm implements Serializable {
  private ReadParadigm() {}

  public abstract String queueName();

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
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ExistingQueue that = (ExistingQueue) o;
      return queueName.equals(that.queueName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queueName);
    }
  }

  public static class NewQueue extends ReadParadigm {
    private final String exchange;
    private final String exchangeType;
    private final String queueName;
    private final String routingKey;

    private NewQueue(
        String exchange, String exchangeType, String queueName, @Nullable String routingKey) {
      checkArgument(
          queueName != null && !queueName.trim().isEmpty(), "A queue name must be provided");
      this.exchange = exchange;
      this.exchangeType = exchangeType;
      this.queueName = queueName.trim();
      this.routingKey =
          Optional.ofNullable(routingKey).map(String::trim).filter(s -> !s.isEmpty()).orElse("");
    }

    public static NewQueue topicExchange(String exchangeName, String queueName, String routingKey) {
      return new NewQueue(exchangeName, "topic", queueName, routingKey);
    }

    public static NewQueue fanoutExchange(String exchangeName, String queueName) {
      return new NewQueue(exchangeName, "fanout", queueName, "");
    }

    public static NewQueue directExchange(String exchangeName, String queueName) {
      checkArgument(
          queueName != null && !queueName.trim().isEmpty(),
          "Queue name is mandatory when using a direct exchange");
      return new NewQueue(exchangeName, "direct", queueName, queueName);
    }

    public static NewQueue defaultExchange(String queueName) {
      return directExchange("", queueName);
    }

    public String getExchange() {
      return exchange;
    }

    public String getExchangeType() {
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

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
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
