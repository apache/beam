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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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
    private Instant lastReportedWatermark;

    public Manual(Instant watermark) {
      BoundedWindow.validateTimestampBounds(watermark);
      this.watermark = checkNotNull(watermark, "watermark must not be null.");
    }

    @Override
    public void setWatermark(Instant watermark) {
      BoundedWindow.validateTimestampBounds(watermark);
      this.lastReportedWatermark = watermark;
    }

    @Override
    public Instant currentWatermark() {
      // Beyond bounds error checking isn't important since the runner is expected to perform
      // watermark bounds checking.
      if (lastReportedWatermark != null && lastReportedWatermark.isAfter(watermark)) {
        watermark = lastReportedWatermark;
      }
      return watermark;
    }

    @Override
    public Instant getState() {
      return watermark;
    }
  }

  /**
   * A watermark estimator that tracks wall time.
   *
   * <p>Note that this watermark estimator expects wall times of all machines performing the
   * processing to be close to each other. Any machine with a wall clock that is far in the past may
   * cause the pipeline to perform poorly while a watermark far in the future may cause records to
   * be marked as late.
   */
  public static class WallTime implements WatermarkEstimator<Instant> {
    private Instant watermark;

    public WallTime(Instant watermark) {
      BoundedWindow.validateTimestampBounds(watermark);
      this.watermark = checkNotNull(watermark, "watermark must not be null.");
    }

    @Override
    public Instant currentWatermark() {
      // Beyond bounds error checking isn't important since the runner is expected to perform
      // watermark bounds checking.
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
   * A watermark estimator that observes timestamps of records output from a DoFn reporting the
   * timestamp of the last element seen as the current watermark.
   *
   * <p>Note that this watermark estimator expects output timestamps in monotonically increasing
   * order. If they are not, then the watermark will advance based upon the last observed timestamp
   * as long as it is greater then any previously reported watermark.
   */
  public static class MonotonicallyIncreasing
      implements TimestampObservingWatermarkEstimator<Instant> {
    private Instant watermark;
    private Instant lastObservedTimestamp;

    public MonotonicallyIncreasing(Instant watermark) {
      BoundedWindow.validateTimestampBounds(watermark);
      this.watermark = checkNotNull(watermark, "timestamp must not be null.");
    }

    @Override
    public void observeTimestamp(Instant timestamp) {
      this.lastObservedTimestamp = timestamp;
    }

    @Override
    public Instant currentWatermark() {
      // Beyond bounds error checking isn't important since the runner is expected to perform
      // watermark bounds checking.
      if (lastObservedTimestamp != null && lastObservedTimestamp.isAfter(watermark)) {
        watermark = lastObservedTimestamp;
      }
      return watermark;
    }

    @Override
    public Instant getState() {
      return watermark;
    }
  }

  // prevent instantiation
  private WatermarkEstimators() {}
}
