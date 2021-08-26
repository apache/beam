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

import java.io.Serializable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Implement this interface to create a {@code WatermarkPolicy}. Used by the {@code
 * ShardRecordsIterator} to create a watermark policy for every shard.
 */
public interface WatermarkPolicyFactory extends Serializable {

  WatermarkPolicy createWatermarkPolicy();

  /** Returns an ArrivalTimeWatermarkPolicy. */
  static WatermarkPolicyFactory withArrivalTimePolicy() {
    return ArrivalTimeWatermarkPolicy::new;
  }

  /**
   * Returns an ArrivalTimeWatermarkPolicy.
   *
   * @param watermarkIdleDurationThreshold watermark idle duration threshold.
   */
  static WatermarkPolicyFactory withArrivalTimePolicy(Duration watermarkIdleDurationThreshold) {
    return () -> new ArrivalTimeWatermarkPolicy(watermarkIdleDurationThreshold);
  }

  /** Returns an ProcessingTimeWatermarkPolicy. */
  static WatermarkPolicyFactory withProcessingTimePolicy() {
    return ProcessingTimeWatermarkPolicy::new;
  }

  /**
   * Returns an custom WatermarkPolicyFactory.
   *
   * @param watermarkParameters Watermark parameters (timestamp extractor, watermark lag) for the
   *     policy.
   */
  static WatermarkPolicyFactory withCustomWatermarkPolicy(WatermarkParameters watermarkParameters) {
    return () -> new CustomWatermarkPolicy(watermarkParameters);
  }

  /**
   * ArrivalTimeWatermarkPolicy uses {@link CustomWatermarkPolicy} for watermark computation. It
   * uses the arrival time of the record as the event time for watermark calculations.
   */
  class ArrivalTimeWatermarkPolicy implements WatermarkPolicy {
    private final CustomWatermarkPolicy watermarkPolicy;

    ArrivalTimeWatermarkPolicy() {
      this.watermarkPolicy =
          new CustomWatermarkPolicy(
              WatermarkParameters.create()
                  .withTimestampFn(KinesisRecord::getApproximateArrivalTimestamp));
    }

    ArrivalTimeWatermarkPolicy(Duration idleDurationThreshold) {
      WatermarkParameters watermarkParameters =
          WatermarkParameters.create()
              .withTimestampFn(KinesisRecord::getApproximateArrivalTimestamp)
              .withWatermarkIdleDurationThreshold(idleDurationThreshold);
      this.watermarkPolicy = new CustomWatermarkPolicy(watermarkParameters);
    }

    @Override
    public Instant getWatermark() {
      return watermarkPolicy.getWatermark();
    }

    @Override
    public void update(KinesisRecord record) {
      watermarkPolicy.update(record);
    }
  }

  /**
   * CustomWatermarkPolicy uses parameters defined in {@link WatermarkParameters} to compute
   * watermarks. This can be used as a standard heuristic to compute watermarks. Used by {@link
   * ArrivalTimeWatermarkPolicy}.
   */
  class CustomWatermarkPolicy implements WatermarkPolicy {
    private WatermarkParameters watermarkParameters;

    CustomWatermarkPolicy(WatermarkParameters watermarkParameters) {
      this.watermarkParameters = watermarkParameters;
    }

    @Override
    public Instant getWatermark() {
      Instant now = Instant.now();
      Instant watermarkIdleThreshold =
          now.minus(watermarkParameters.getWatermarkIdleDurationThreshold());

      Instant newWatermark =
          watermarkParameters.getLastUpdateTime().isBefore(watermarkIdleThreshold)
              ? watermarkIdleThreshold
              : watermarkParameters.getEventTime();

      if (newWatermark.isAfter(watermarkParameters.getCurrentWatermark())) {
        watermarkParameters =
            watermarkParameters.toBuilder().setCurrentWatermark(newWatermark).build();
      }
      return watermarkParameters.getCurrentWatermark();
    }

    @Override
    public void update(KinesisRecord record) {
      watermarkParameters =
          watermarkParameters
              .toBuilder()
              .setEventTime(
                  Ordering.natural()
                      .max(
                          watermarkParameters.getEventTime(),
                          watermarkParameters.getTimestampFn().apply(record)))
              .setLastUpdateTime(Instant.now())
              .build();
    }
  }

  /** Watermark policy where the processing time is used as the event time. */
  class ProcessingTimeWatermarkPolicy implements WatermarkPolicy {
    @Override
    public Instant getWatermark() {
      return Instant.now();
    }

    @Override
    public void update(KinesisRecord record) {
      // do nothing
    }
  }
}
