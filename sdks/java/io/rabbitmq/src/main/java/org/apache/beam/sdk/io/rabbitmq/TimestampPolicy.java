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
import java.util.Date;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * A timestamp policy to assign event time for messages from a RabbitMq queue and watermark for it.
 *
 * <p>Inspired by a similar concept in KafkaIO
 */
public abstract class TimestampPolicy {

  /**
   * A timestamp extractor compatible with the <a
   * href="https://github.com/rabbitmq/rabbitmq-message-timestamp">RabbitMQ Message Timestamp
   * plugin</a>, which first looks for header {@code timestamp_in_ms} then falls back to amqp
   * message property {@code timestamp}
   */
  public static SerializableFunction<RabbitMqMessage, Instant>
      RABBITMQ_MESSAGE_TIMESTAMP_PLUGIN_FORMAT =
          record -> {
            Object rawTimestampMillis = record.headers().get("timestamp_in_ms");
            if (rawTimestampMillis != null) {
              try {
                return Instant.ofEpochMilli(Long.parseLong(rawTimestampMillis.toString()));
              } catch (NumberFormatException | NullPointerException e) {
                /* ignored */
              }
            }
            Date timestamp = record.timestamp();
            if (timestamp == null) {
              throw new IllegalArgumentException(
                  "Neither timestamp_in_ms header nor timestamp property contain a valid timestamp value");
            }
            return new Instant(timestamp);
          };

  /** Analogous to KafkaIO's TimestampPolicy.PartitionContext. */
  public abstract static class LastRead implements Serializable {
    /** Estimate of current queue depth per {@code GetResponse.messageCount}. */
    public abstract int getMessageBacklog();

    /**
     * @return the timestamp (wall clock) at which the last read was attempted, regardless of
     *     whether a message was available
     */
    public abstract Instant getBacklogCheckTime();
  }

  /**
   * Determines the current event timestamp for a given record.
   *
   * <p>NOTE: this may be a side-effecting operation. Implementations are free to utilize the
   * resulting value to update internal state, such as "most recent event time", as a result of
   * calling this function. That is, calls to this method are expected to happen once for every
   * incoming message.
   *
   * @param record the record to determine the event time for
   */
  public abstract Instant getTimestampForRecord(RabbitMqMessage record);

  /**
   * Returns watermark for the queue. It is the timestamp before or at the timestamps of all future
   * records consumed from the partition. See {@link UnboundedSource.UnboundedReader#getWatermark()}
   * for more guidance on watermarks.
   *
   * <p>NOTE: implementations may use data from previous calls to {@link
   * #getTimestampForRecord(RabbitMqMessage)} to produce an appropriate watermark here. It is
   * reasonable to determine a watermark taking the latest event time calculated in {@link
   * #getTimestampForRecord(RabbitMqMessage)} into consideration.
   */
  public abstract Instant getWatermark(LastRead context);
}
