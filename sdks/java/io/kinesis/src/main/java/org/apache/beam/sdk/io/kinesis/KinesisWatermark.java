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
package org.apache.beam.sdk.io.kinesis;

import java.util.function.BooleanSupplier;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.util.MovingFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Keeps track of current watermark using {@link MovingFunction}. If the pipeline is up to date with
 * the processing, watermark would be around 'now - {@link KinesisWatermark#SAMPLE_PERIOD}' for a
 * stream with steady traffic, and around 'now - {@link KinesisWatermark#UPDATE_THRESHOLD}' for a
 * stream with low traffic.
 */
class KinesisWatermark {
  /** Period of updates to determine watermark. */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /** Period of samples to determine watermark. */
  static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /**
   * Period after which watermark should be updated regardless of number of samples. It has to be
   * longer than {@link KinesisWatermark#SAMPLE_PERIOD}, so that for most of the cases value
   * returned from {@link MovingFunction#isSignificant()} is sufficient to decide about watermark
   * update.
   */
  static final Duration UPDATE_THRESHOLD = SAMPLE_PERIOD.multipliedBy(2);

  /** Constant representing the maximum Kinesis stream retention period. */
  static final Duration MAX_KINESIS_STREAM_RETENTION_PERIOD = Duration.standardDays(7);

  /** Minimum number of unread messages required before considering updating watermark. */
  static final int MIN_MESSAGES = 10;

  /**
   * Minimum number of SAMPLE_UPDATE periods over which unread messages should be spread before
   * considering updating watermark.
   */
  private static final int MIN_SPREAD = 2;

  private Instant lastWatermark = Instant.now().minus(MAX_KINESIS_STREAM_RETENTION_PERIOD);
  private Instant lastUpdate = new Instant(0L);
  private final MovingFunction minReadTimestampMsSinceEpoch =
      new MovingFunction(
          SAMPLE_PERIOD.getMillis(),
          SAMPLE_UPDATE.getMillis(),
          MIN_SPREAD,
          MIN_MESSAGES,
          Min.ofLongs());

  public Instant getCurrent(BooleanSupplier shardsUpToDate) {
    Instant now = Instant.now();
    Instant readMin = getMinReadTimestamp(now);
    if (readMin == null) {
      if (shardsUpToDate.getAsBoolean()) {
        updateLastWatermark(now.minus(SAMPLE_PERIOD), now);
      }
    } else if (shouldUpdate(now)) {
      updateLastWatermark(readMin, now);
    }
    return lastWatermark;
  }

  public void update(Instant recordArrivalTime) {
    minReadTimestampMsSinceEpoch.add(Instant.now().getMillis(), recordArrivalTime.getMillis());
  }

  private Instant getMinReadTimestamp(Instant now) {
    long readMin = minReadTimestampMsSinceEpoch.get(now.getMillis());
    if (readMin == Min.ofLongs().identity()) {
      return null;
    } else {
      return new Instant(readMin);
    }
  }

  /**
   * In case of streams with low traffic, {@link MovingFunction} could never get enough samples in
   * {@link KinesisWatermark#SAMPLE_PERIOD} to move watermark. To prevent this situation, we need to
   * check if watermark is stale (it was not updated during {@link
   * KinesisWatermark#UPDATE_THRESHOLD}) and force its update if it is.
   *
   * @param now - current timestamp
   * @return should the watermark be updated
   */
  private boolean shouldUpdate(Instant now) {
    boolean hasEnoughSamples = minReadTimestampMsSinceEpoch.isSignificant();
    boolean isStale = lastUpdate.isBefore(now.minus(UPDATE_THRESHOLD));
    return hasEnoughSamples || isStale;
  }

  private void updateLastWatermark(Instant newWatermark, Instant now) {
    if (newWatermark.isAfter(lastWatermark)) {
      lastWatermark = newWatermark;
      lastUpdate = now;
    }
  }
}
