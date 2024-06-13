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
package org.apache.beam.sdk.io.solace.read;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** {@code WatermarkParameters} contains the parameters used for watermark computation. */
@AutoValue
abstract class WatermarkParameters<T> implements Serializable {

  private static final Duration STANDARD_WATERMARK_IDLE_DURATION_THRESHOLD =
      Duration.standardSeconds(30);

  abstract Instant getCurrentWatermark();

  abstract Instant getLastSavedWatermark();

  abstract Instant getLastUpdateTime();

  abstract SerializableFunction<T, Instant> getTimestampFn();

  abstract Duration getWatermarkIdleDurationThreshold();

  abstract Builder<T> toBuilder();

  static <T> Builder<T> builder() {
    return new AutoValue_WatermarkParameters.Builder<T>()
        .setCurrentWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE)
        .setLastSavedWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE)
        .setLastUpdateTime(Instant.now())
        .setWatermarkIdleDurationThreshold(STANDARD_WATERMARK_IDLE_DURATION_THRESHOLD);
  }

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setCurrentWatermark(Instant currentWatermark);

    abstract Builder<T> setLastSavedWatermark(Instant eventTime);

    abstract Builder<T> setLastUpdateTime(Instant now);

    abstract Builder<T> setWatermarkIdleDurationThreshold(Duration watermarkIdleDurationThreshold);

    abstract Builder<T> setTimestampFn(SerializableFunction<T, Instant> timestampFn);

    abstract WatermarkParameters<T> build();
  }

  /**
   * Create an instance of {@link WatermarkParameters} with a {@code SerializableFunction} to
   * extract the event time.
   */
  static <T> WatermarkParameters<T> create(SerializableFunction<T, Instant> timestampFn) {
    Preconditions.checkArgument(timestampFn != null, "timestampFn function is null");
    return WatermarkParameters.<T>builder().setTimestampFn(timestampFn).build();
  }

  /**
   * Specify the watermark idle duration to consider before advancing the watermark. The default
   * watermark idle duration threshold is {@link #STANDARD_WATERMARK_IDLE_DURATION_THRESHOLD}.
   */
  WatermarkParameters<T> withWatermarkIdleDurationThreshold(Duration idleDurationThreshold) {
    Preconditions.checkArgument(
        idleDurationThreshold != null, "watermark idle duration threshold is null");
    return toBuilder().setWatermarkIdleDurationThreshold(idleDurationThreshold).build();
  }
}
