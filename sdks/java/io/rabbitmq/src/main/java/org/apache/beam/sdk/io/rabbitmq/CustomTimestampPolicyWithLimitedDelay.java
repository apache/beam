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

import java.util.Optional;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A policy for custom record timestamps where timestamps within the queue are expected to be
 * roughly monotonically increasing with a cap on out of order event delays (say 1 minute). The
 * watermark at any time is '({@code Min(now(), Max(event timestamp so far)) - max delay})'.
 * However, watermark is never set in future and capped to 'now - max delay'. In addition, watermark
 * advanced to 'now - max delay' when the queue has caught up (previous read attempt returned no
 * message)
 *
 * @see "org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay for the inspiration and
 *     corresponding KafkaIO policy"
 */
public class CustomTimestampPolicyWithLimitedDelay extends TimestampPolicy {
  private final Duration maxDelay;
  private final SerializableFunction<RabbitMqMessage, Instant> timestampFunction;
  private Instant maxEventTimestamp;

  /**
   * A policy for custom record timestamps where timestamps are expected to be roughly monotonically
   * increasing with out of order event delays less than {@code maxDelay}. The watermark at any time
   * is {@code Min(now(), max_event_timestamp) - maxDelay}.
   *
   * @param timestampFunction A function to extract timestamp from the record
   * @param maxDelay For any record in the Kafka partition, the timestamp of any subsequent record
   *     is expected to be after {@code current record timestamp - maxDelay}.
   * @param previousWatermark Latest check-pointed watermark
   */
  public CustomTimestampPolicyWithLimitedDelay(
      Duration maxDelay,
      SerializableFunction<RabbitMqMessage, Instant> timestampFunction,
      Optional<Instant> previousWatermark) {
    this.maxDelay = maxDelay;
    this.timestampFunction = timestampFunction;
    // 'previousWatermark' is not the same as maxEventTimestamp (e.g. it could have been in future).
    // Initialize it such that watermark before reading any event same as previousWatermark.
    maxEventTimestamp = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE).plus(maxDelay);
  }

  @Override
  public Instant getTimestampForRecord(RabbitMqMessage record) {
    Instant ts = timestampFunction.apply(record);
    if (ts.isAfter(maxEventTimestamp)) {
      maxEventTimestamp = ts;
    }
    return ts;
  }

  @Override
  public Instant getWatermark(LastRead ctx, RabbitMqMessage record) {
    // Watermark == maxEventTime - maxDelay, except in two special cases:
    //   a) maxEventTime in future : probably due to incorrect timestamps. Cap it to 'now'.
    //   b) queue is empty : Need to advance watermark if there are no records in the queue.
    //         We assume that future records will have timestamp >= 'now - maxDelay' and advance
    //         the watermark accordingly.
    // The above handles majority of common use cases for custom timestamps. Users can implement
    // their own policy if this does not work.

    return getWatermark(ctx, Instant.now());
  }

  @VisibleForTesting
  Instant getWatermark(LastRead ctx, Instant now) {
    if (maxEventTimestamp.isAfter(now)) {
      return now.minus(maxDelay); // (a) above.
    } else if (ctx.getMessageBacklog() == 0
        && ctx.getBacklogCheckTime().minus(maxDelay).isAfter(maxEventTimestamp) // Idle
        && maxEventTimestamp.getMillis() > 0) { // Read at least one record with positive timestamp.
      return ctx.getBacklogCheckTime().minus(maxDelay);
    } else {
      return maxEventTimestamp.minus(maxDelay);
    }
  }
}
