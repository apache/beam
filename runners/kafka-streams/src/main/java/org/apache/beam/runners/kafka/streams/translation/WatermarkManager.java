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
package org.apache.beam.runners.kafka.streams.translation;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * Tracks one fused stage's input watermark from the committed watermarks reported by the upstream
 * source partitions feeding it. Kept free of Kafka wiring so it can be unit-tested on its own.
 *
 * <p>It counts source partitions rather than producer instances. A partition count is fixed, known,
 * and travels in-band with every report, whereas instances come and go on every rebalance and can
 * die without notice; a dead instance's partitions are reassigned and the new owner keeps
 * reporting.
 *
 * <p>Until every source partition has reported, the stage's input watermark is undefined and {@link
 * #advance()} returns {@link BoundedWindow#TIMESTAMP_MIN_VALUE}. A change in the partition count
 * clears the reports and re-opens that hold, which subsumes an explicit epoch rule.
 *
 * <p>Watermarks must not go backwards, so each partition's watermark is held monotonic and the
 * emitted one is clamped against the last emitted — a newly appeared partition may report an older
 * watermark than the stage has already reached.
 *
 * <p>Not thread-safe; the calling Kafka Streams processor thread serializes access.
 */
public final class WatermarkManager {

  /** Total source partitions feeding this stage, learned in-band; -1 until the first report. */
  private int expectedSourcePartitionCount = -1;

  /** Latest committed watermark per source partition (kept monotonic non-decreasing). */
  private final Map<Integer, Instant> committedWatermarkByPartition = new HashMap<>();

  /** Last watermark {@link #advance()} emitted, to enforce a non-decreasing output. */
  private Instant lastEmitted = BoundedWindow.TIMESTAMP_MIN_VALUE;

  /**
   * Record a committed watermark reported for one source partition, together with the total source
   * partition count carried in-band with the report.
   *
   * @param sourcePartition the source partition the report is for, in {@code [0,
   *     totalSourcePartitions)}
   * @param committedWatermark the committed watermark for that partition
   * @param totalSourcePartitions the total number of upstream source partitions feeding this stage
   */
  public void observe(int sourcePartition, Instant committedWatermark, int totalSourcePartitions) {
    if (committedWatermark == null) {
      throw new IllegalArgumentException("committedWatermark must not be null");
    }
    if (totalSourcePartitions <= 0) {
      throw new IllegalArgumentException(
          "totalSourcePartitions must be positive: " + totalSourcePartitions);
    }
    if (sourcePartition < 0 || sourcePartition >= totalSourcePartitions) {
      throw new IllegalArgumentException(
          "sourcePartition "
              + sourcePartition
              + " out of range for totalSourcePartitions "
              + totalSourcePartitions);
    }
    if (totalSourcePartitions != expectedSourcePartitionCount) {
      // The source partition set changed (e.g. a repartition). The previous per-partition
      // watermarks describe a different partitioning, so drop them entirely and re-open the hold
      // until the new full set reports. The output watermark still cannot regress (lastEmitted is
      // retained).
      expectedSourcePartitionCount = totalSourcePartitions;
      committedWatermarkByPartition.clear();
    }
    // A source partition's watermark is monotonic non-decreasing; ignore an out-of-order lower
    // report.
    committedWatermarkByPartition.merge(
        sourcePartition, committedWatermark, (oldW, newW) -> newW.isAfter(oldW) ? newW : oldW);
  }

  /** True once a committed watermark has been seen for every current source partition. */
  public boolean isReady() {
    return expectedSourcePartitionCount > 0
        && committedWatermarkByPartition.size() == expectedSourcePartitionCount;
  }

  /**
   * Advance and return the stage input watermark.
   *
   * <p>Returns {@link BoundedWindow#TIMESTAMP_MIN_VALUE} while the stage is still holding (not
   * every source partition has reported) — the caller emits nothing meaningful downstream in that
   * case. Once ready, returns {@code min()} over all source partitions, clamped to never regress
   * below the previously emitted value. The sequence of values returned across calls is
   * non-decreasing.
   */
  public Instant advance() {
    if (!isReady()) {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    // isReady() guarantees the map is non-empty, so the seed is always replaced by a real value.
    Instant min = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (Instant w : committedWatermarkByPartition.values()) {
      if (w.isBefore(min)) {
        min = w;
      }
    }
    Instant emit = min.isAfter(lastEmitted) ? min : lastEmitted;
    lastEmitted = emit;
    return emit;
  }

  /** The total source partition count learned in-band, or -1 if nothing reported yet. */
  @VisibleForTesting
  int expectedSourcePartitionCount() {
    return expectedSourcePartitionCount;
  }

  /** How many distinct source partitions have reported so far. */
  @VisibleForTesting
  int reportedPartitionCount() {
    return committedWatermarkByPartition.size();
  }
}
