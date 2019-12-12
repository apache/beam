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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/**
 * Defines the approach for determining a unique identifier for a given incoming message, vital for
 * deduping messages, especially if messages are re-delivered due to severed Connections or Channels.
 */
@FunctionalInterface
public interface RecordIdPolicy extends Function<RabbitMqMessage, byte[]> {

  /** @return a policy that defines the message id by the amqp {@code correlation-id} property */
  static RecordIdPolicy correlationId() {
    return new CorrelationIdPropertyPolicy();
  }

  /** @return a policy that defines the message id by the amqp {@code message-id} property */
  static RecordIdPolicy messageId() {
    return new MessageIdPropertyPolicy();
  }

  /**
   * @return a policy that defines the message id as the full body of the message. This is not a
   *     good policy to use in production.
   */
  static RecordIdPolicy body() {
    return new BodyPolicy();
  }

  /** @return a policy that defines the message id by a Sha256 hash of the body of the message. */
  static RecordIdPolicy bodySha256() {
    return new BodySha256Policy();
  }

  /**
   * @return a policy that creates a unique id for every incoming message. This *will* result in
   *     messages being replayed if the Connection or Channel is ever severed before messages are
   *     acknowledged and should be considered unsafe.
   */
  static RecordIdPolicy alwaysUnique() {
    return new AlwaysUniquePolicy();
  }

  /** Abstraction for defining policies based on String values extracted from a RabbitMqMessage. */
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

  class AlwaysUniquePolicy implements RecordIdPolicy {
    @Override
    public byte[] apply(RabbitMqMessage rabbitMqMessage) {
      try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream outStream = new DataOutputStream(bytesOut)) {

        UUID uuid = UUID.randomUUID();
        outStream.writeLong(uuid.getMostSignificantBits());
        outStream.writeLong(uuid.getLeastSignificantBits());
        outStream.flush();
        return bytesOut.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
