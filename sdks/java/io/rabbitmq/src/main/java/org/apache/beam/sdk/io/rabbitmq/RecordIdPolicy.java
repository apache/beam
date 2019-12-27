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
import java.util.Date;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;

/**
 * Defines the approach for determining a unique identifier for a given incoming message, vital for
 * deduping messages, especially if messages are re-delivered due to severed Connections or
 * Channels.
 */
@FunctionalInterface
public interface RecordIdPolicy extends Function<RabbitMqMessage, byte[]> {

  /**
   * @return a policy that defines the message id by the amqp {@code correlation-id} property
   * @deprecated CorrelationId is not a good per-message unique identifier as many messages may be
   *     correlated to a single upstream event. This is here for Beam legacy purposes (the old
   *     RabbitMqIO used this property exclusively for unique identification), but consider using
   *     messageId instead.
   */
  @Deprecated
  static RecordIdPolicy correlationId() {
    return new CorrelationIdPropertyPolicy();
  }

  /** @return a policy that defines the message id by the amqp {@code message-id} property */
  static RecordIdPolicy messageId() {
    return new MessageIdPropertyPolicy();
  }

  /**
   * @return a policy that defines the message id by a Sha256 hash of the body of the message. Note
   *     that this policy is not appropriate if messages are replayed on a schedule and have no
   *     differentiating characterstics between them. For example, a message "execute" emitted once
   *     per second will be treated as a duplicate each instance after the first by this policy.
   */
  static RecordIdPolicy bodySha256() {
    return new BodySha256Policy();
  }

  /**
   * @return a policy that defines the message id by a hash of a combination of the body and the
   *     amqp Timestamp property. This is a reasonable choice in environments where messages are not
   *     always unique but are unique within any given 1-second time window, yet a messageId amqp
   *     property cannot be supplied. This requires the timestamp amqp property to be set.
   * @see <a href="https://github.com/rabbitmq/rabbitmq-message-timestamp">the RabbitMq Message
   *     Timestamp Plugin</a> for a consistent means of setting this property within the RabbitMq
   *     broker.
   */
  static RecordIdPolicy bodyWithTimestamp() {
    return new BodyWithTimestampPolicy();
  }

  /**
   * @return a policy that creates a unique id for every incoming message. This *will* result in
   *     messages being replayed if the Connection or Channel is ever severed before messages are
   *     acknowledged and should be considered unsafe. This policy may be appropriate for pipelines
   *     with at-least-once semantics and are ok with significant numbers of replayed messages.
   *     Generally speaking, this policy is likely most appropriate for testing.
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
      return rabbitMqMessage.correlationId();
    }
  }

  class MessageIdPropertyPolicy extends StringPolicy {
    @Override
    protected String extractString(RabbitMqMessage rabbitMqMessage) {
      return rabbitMqMessage.messageId();
    }
  }

  class BodySha256Policy implements RecordIdPolicy {
    @Override
    public byte[] apply(RabbitMqMessage rabbitMqMessage) {
      return Hashing.sha256().hashBytes(rabbitMqMessage.body()).asBytes();
    }
  }

  class BodyWithTimestampPolicy implements RecordIdPolicy {
    @Override
    public byte[] apply(RabbitMqMessage rabbitMqMessage) {
      Date timestamp = rabbitMqMessage.timestamp();
      if (timestamp == null) {
        throw new IllegalArgumentException("Rabbit message's Timestamp property is null");
      }
      try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
          DataOutputStream outStream = new DataOutputStream(bytesOut)) {
        outStream.writeLong(timestamp.getTime());
        outStream.write(rabbitMqMessage.body());
        return Hashing.sha256().hashBytes(bytesOut.toByteArray()).asBytes();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
