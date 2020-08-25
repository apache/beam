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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** {@code WatermarkParameters} contains the parameters used for watermark computation. */
@AutoValue
public abstract class WatermarkParameters implements Serializable {

  private static final SerializableFunction<KinesisRecord, Instant> ARRIVAL_TIME_FN =
      KinesisRecord::getApproximateArrivalTimestamp;
  private static final Duration STANDARD_WATERMARK_IDLE_DURATION_THRESHOLD =
      Duration.standardMinutes(2);

  abstract Instant getCurrentWatermark();

  abstract Instant getEventTime();

  abstract Instant getLastUpdateTime();

  abstract SerializableFunction<KinesisRecord, Instant> getTimestampFn();

  abstract Duration getWatermarkIdleDurationThreshold();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_WatermarkParameters.Builder()
        .setCurrentWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE)
        .setEventTime(BoundedWindow.TIMESTAMP_MIN_VALUE)
        .setTimestampFn(ARRIVAL_TIME_FN)
        .setLastUpdateTime(Instant.now())
        .setWatermarkIdleDurationThreshold(STANDARD_WATERMARK_IDLE_DURATION_THRESHOLD);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setCurrentWatermark(Instant currentWatermark);

    abstract Builder setEventTime(Instant eventTime);

    abstract Builder setLastUpdateTime(Instant now);

    abstract Builder setWatermarkIdleDurationThreshold(Duration watermarkIdleDurationThreshold);

    abstract Builder setTimestampFn(SerializableFunction<KinesisRecord, Instant> timestampFn);

    abstract WatermarkParameters build();
  }

  public static WatermarkParameters create() {
    return builder().build();
  }

  /**
   * Specify the {@code SerializableFunction} to extract the event time from a {@code
   * KinesisRecord}. The default event timestamp is the arrival timestamp of the record.
   *
   * @param timestampFn Serializable function to extract the timestamp from a record.
   */
  public WatermarkParameters withTimestampFn(
      SerializableFunction<KinesisRecord, Instant> timestampFn) {
    checkArgument(timestampFn != null, "timestampFn function is null");
    return builder().setTimestampFn(timestampFn).build();
  }

  /**
   * Specify the watermark idle duration to consider before advancing the watermark. The default
   * watermark idle duration threshold is 2 minutes.
   */
  public WatermarkParameters withWatermarkIdleDurationThreshold(Duration idleDurationThreshold) {
    checkArgument(idleDurationThreshold != null, "watermark idle duration threshold is null");
    return builder().setWatermarkIdleDurationThreshold(idleDurationThreshold).build();
  }
}
