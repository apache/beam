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
import java.util.Optional;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An extendable factory to create a {@link TimestampPolicy} for each queue at runtime by RabbitMqIO
 * reader. Subclasses implement {@link #createTimestampPolicy}, which is invoked by the the reader
 * while starting or resuming from a checkpoint. Two commonly used policies are provided. See {@link
 * #withTimestampProperty(Duration)} and {@link #withProcessingTime()}.
 */
@FunctionalInterface
public interface TimestampPolicyFactory extends Serializable {
  /**
   * Creates a TimestampPolicy for a queue. This is invoked by the reader at the start or while
   * resuming from previous checkpoint.
   *
   * @param previousWatermark The latest check-pointed watermark. This is set when the reader is
   *     resuming from a checkpoint. This is a good value to return by implementations of {@link
   *     TimestampPolicy#getWatermark(TimestampPolicy.LastRead, RabbitMqMessage)} until a better
   *     watermark can be established as more records are read.
   */
  TimestampPolicy createTimestampPolicy(Optional<Instant> previousWatermark);

  static TimestampPolicyFactory withProcessingTime() {
    return prev -> new ProcessingTimePolicy();
  }

  static TimestampPolicyFactory withTimestampProperty(Duration maxDelay) {
    SerializableFunction<RabbitMqMessage, Instant> timestampFunction =
        record -> {
          checkArgument(
              record.getTimestamp() != null, "Rabbit message's Timestamp property is null");
          return new Instant(record.getTimestamp());
        };

    return previousWatermark ->
        new CustomTimestampPolicyWithLimitedDelay(maxDelay, timestampFunction, previousWatermark);
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
    public Instant getWatermark(LastRead context, RabbitMqMessage record) {
      return Instant.now();
    }
  }
}
