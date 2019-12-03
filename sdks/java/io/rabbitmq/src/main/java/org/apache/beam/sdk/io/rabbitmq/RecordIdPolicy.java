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

import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/**
 *  @todo use singletons for common policies
 */
@FunctionalInterface
public interface RecordIdPolicy extends Function<RabbitMqMessage, byte[]> {

  static RecordIdPolicy correlationId() {
    return new CorrelationIdPropertyPolicy();
  }

  static RecordIdPolicy messageId() {
    return new MessageIdPropertyPolicy();
  }

  static RecordIdPolicy body() {
    return new BodyPolicy();
  }

  static RecordIdPolicy bodySha256() {
    return new BodySha256Policy();
  }

  abstract class StringPolicy implements RecordIdPolicy {
    protected abstract String extractString(RabbitMqMessage rabbitMqMessage);

    @Override
    public final byte[] apply(RabbitMqMessage rabbitMqMessage) {
      return extractString(rabbitMqMessage).getBytes(StandardCharsets.UTF_8);
    }
  }

  class CorrelationIdPropertyPolicy extends StringPolicy {
    @Override
    protected String extractString(RabbitMqMessage rabbitMqMessage) {
      return rabbitMqMessage.getCorrelationId();
    }
  }

  class MessageIdPropertyPolicy extends StringPolicy {
    @Override
    protected String extractString(RabbitMqMessage rabbitMqMessage) {
      return rabbitMqMessage.getMessageId();
    }
  }

  class BodyPolicy implements RecordIdPolicy {
    @Override
    public byte[] apply(RabbitMqMessage rabbitMqMessage) {
      return rabbitMqMessage.getBody();
    }
  }

  class BodySha256Policy implements RecordIdPolicy {
    @Override
    public byte[] apply(RabbitMqMessage rabbitMqMessage) {
      return Hashing.sha256().hashBytes(rabbitMqMessage.getBody()).asBytes();
    }
  }
}
