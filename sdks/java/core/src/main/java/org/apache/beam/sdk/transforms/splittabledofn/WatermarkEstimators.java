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
package org.apache.beam.sdk.transforms.splittabledofn;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Instant;

/**
 * A set of {@link WatermarkEstimator}s that users can use to advance the output watermark for their
 * associated {@link DoFn splittable DoFn}s.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public class WatermarkEstimators {
  /** Concrete implementation of a {@link ManualWatermarkEstimator}. */
  public static class Manual implements ManualWatermarkEstimator<Instant> {
    private Instant watermark;

    public Manual(Instant watermark) {
      this.watermark = checkNotNull(watermark, "watermark must not be null.");
      if (watermark.isBefore(GlobalWindow.TIMESTAMP_MIN_VALUE)
          || watermark.isAfter(GlobalWindow.TIMESTAMP_MAX_VALUE)) {
        throw new IllegalArgumentException(
            String.format(
                "Provided watermark %s must be within bounds [%s, %s].",
                watermark, GlobalWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.TIMESTAMP_MAX_VALUE));
      }
    }

    @Override
    public void setWatermark(Instant watermark) {
      if (watermark.isBefore(GlobalWindow.TIMESTAMP_MIN_VALUE)
          || watermark.isAfter(GlobalWindow.TIMESTAMP_MAX_VALUE)) {
        throw new IllegalArgumentException(
            String.format(
                "Provided watermark %s must be within bounds [%s, %s].",
                watermark, GlobalWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.TIMESTAMP_MAX_VALUE));
      }
      if (watermark.isBefore(this.watermark)) {
        throw new IllegalArgumentException(
            String.format(
                "Watermark must be monotonically increasing. Provided watermark %s is less then "
                    + "current watermark %s.",
                watermark, this.watermark));
      }
      this.watermark = watermark;
    }

    @Override
    public Instant currentWatermark() {
      return watermark;
    }

    @Override
    public Instant getState() {
      return watermark;
    }
  }

  /** A watermark estimator that tracks wall time. */
  public static class WallTime implements WatermarkEstimator<Instant> {
    private Instant watermark;

    public WallTime(Instant watermark) {
      this.watermark = checkNotNull(watermark, "watermark must not be null.");
      if (watermark.isBefore(GlobalWindow.TIMESTAMP_MIN_VALUE)
          || watermark.isAfter(GlobalWindow.TIMESTAMP_MAX_VALUE)) {
        throw new IllegalArgumentException(
            String.format(
                "Provided watermark %s must be within bounds [%s, %s].",
                watermark, GlobalWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.TIMESTAMP_MAX_VALUE));
      }
    }

    @Override
    public Instant currentWatermark() {
      Instant now = Instant.now();
      this.watermark = now.isAfter(watermark) ? now : watermark;
      return watermark;
    }

    @Override
    public Instant getState() {
      return watermark;
    }
  }

  /**
   * A watermark estimator that observes and timestamps of records output from a DoFn reporting the
   * timestamp of the last element seen as the current watermark.
   *
   * <p>Note that this watermark estimator requires output timestamps in monotonically increasing
   * order.
   */
  public static class MonotonicallyIncreasing
      implements TimestampObservingWatermarkEstimator<Instant> {
    private Instant watermark;

    public MonotonicallyIncreasing(Instant watermark) {
      this.watermark = checkNotNull(watermark, "timestamp must not be null.");
      if (watermark.isBefore(GlobalWindow.TIMESTAMP_MIN_VALUE)
          || watermark.isAfter(GlobalWindow.TIMESTAMP_MAX_VALUE)) {
        throw new IllegalArgumentException(
            String.format(
                "Provided watermark %s must be within bounds [%s, %s].",
                watermark, GlobalWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.TIMESTAMP_MAX_VALUE));
      }
    }

    @Override
    public void observeTimestamp(Instant timestamp) {
      // Beyond bounds error checking isn't important since the system is expected to perform output
      // timestamp bounds checking already.
      if (timestamp.isBefore(this.watermark)) {
        throw new IllegalArgumentException(
            String.format(
                "Timestamp must be monotonically increasing. Provided timestamp %s is less then "
                    + "previously provided timestamp %s.",
                timestamp, this.watermark));
      }
      this.watermark = timestamp;
    }

    @Override
    public Instant currentWatermark() {
      return watermark;
    }

    @Override
    public Instant getState() {
      return watermark;
    }
  }
}
