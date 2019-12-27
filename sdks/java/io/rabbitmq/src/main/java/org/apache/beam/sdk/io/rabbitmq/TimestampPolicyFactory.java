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

import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An extendable factory to create a {@link TimestampPolicy} for each queue at runtime by RabbitMqIO
 * reader. Subclasses implement {@link #createTimestampPolicy}, which is invoked by the the reader
 * while starting or resuming from a checkpoint. Two commonly used policies are provided. See {@link
 * #withTimestampPluginCompat(Duration)} and {@link #withProcessingTime()}.
 */
@FunctionalInterface
public interface TimestampPolicyFactory extends Serializable {
  /**
   * Creates a TimestampPolicy for a queue. This is invoked by the reader at the start or while
   * resuming from previous checkpoint.
   *
   * @param previousWatermark The latest check-pointed watermark. This is set when the reader is
   *     resuming from a checkpoint. This is a good value to return by implementations of {@link
   *     TimestampPolicy#getWatermark(TimestampPolicy.LastRead)} until a better watermark can be
   *     established as more records are read.
   */
  TimestampPolicy createTimestampPolicy(Optional<Instant> previousWatermark);

  /**
   * @return A simple policy that uses current time for event time and watermark. This should be
   *     used if no better timestamps are available such as a custom header like timestamp_in_ms or
   *     amqp property timestamp.
   */
  static TimestampPolicyFactory withProcessingTime() {
    return prev -> new ProcessingTimePolicy();
  }

  /**
   * Produces a {@link CustomTimestampPolicyWithLimitedDelay} policy based on extracting timestamps
   * compatible with the <a href="https://github.com/rabbitmq/rabbitmq-message-timestamp">RabbitMQ
   * Message Timestamp</a> plugin, that first looks for header {@code timestamp_in_ms} then falls
   * back to amqp property {@code timestamp}. If neither are specified or both are malformed, a
   * runtime exception will be thrown.
   *
   * <p>To use a custom timestamp extraction strategy with {@link
   * CustomTimestampPolicyWithLimitedDelay}, use {@link #withTimestamp(Duration,
   * SerializableFunction)}.
   *
   * @param maxDelay For any record in the mostly-monotonically-increasing queue, the timestamp of
   *     any subsequent record is expected to be after {@code current record timestamp - maxDelay}.
   *     This value is meant to reflect the maximum 'out-of-orderness' that can be expected in the
   *     queue before considering a message a late arrival.
   */
  static TimestampPolicyFactory withTimestampPluginCompat(Duration maxDelay) {
    return withTimestamp(maxDelay, TimestampPolicy.RABBITMQ_MESSAGE_TIMESTAMP_PLUGIN_FORMAT);
  }

  /**
   * Produces a {@link CustomTimestampPolicyWithLimitedDelay} policy based on the supplied timestamp
   * extractor.
   *
   * <p>To use an implementation compatible with the <a
   * href="https://github.com/rabbitmq/rabbitmq-message-timestamp">RabbitMQ Message Timestamp
   * plugin</a>, use {@link #withTimestampPluginCompat(Duration)} or provide argument {@link
   * TimestampPolicy#RABBITMQ_MESSAGE_TIMESTAMP_PLUGIN_FORMAT}.
   *
   * @param maxDelay maxDelay For any record in the mostly-monotonically-increasing queue, the
   *     timestamp of any subsequent record is expected to be after {@code current record timestamp
   *     - maxDelay}. This value is meant to reflect the maximum 'out-of-orderness' that can be
   *     expected in the queue before considering a message a late arrival.
   * @param timestampExtractor a means of extracting the event time from a rabbitmq message
   */
  static TimestampPolicyFactory withTimestamp(
      Duration maxDelay, SerializableFunction<RabbitMqMessage, Instant> timestampExtractor) {
    return previousWatermark ->
        new CustomTimestampPolicyWithLimitedDelay(maxDelay, timestampExtractor, previousWatermark);
  }

  /**
   * A simple policy that uses current time for event time and watermark. This should be used when
   * better timestamps (like the Timestamp property) are not available for all messages on the
   * queue.
   */
  class ProcessingTimePolicy extends TimestampPolicy {
    @Override
    public Instant getTimestampForRecord(RabbitMqMessage record) {
      return Instant.now();
    }

    @Override
    public Instant getWatermark(LastRead context) {
      return Instant.now();
    }
  }
}
