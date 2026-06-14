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
import java.util.OptionalLong;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * In-memory tracker of a single fused stage's input watermark, computed from the committed
 * watermarks reported by its upstream <i>source partitions</i>.
 *
 * <p>This is the core of the Kafka Streams runner's watermark propagation, decoupled from the Kafka
 * wiring so it can be unit-tested in isolation. The wiring that produces the reports (flushing
 * {@code (sourcePartition, committedWatermark, totalSourcePartitions)} atomically with each offset
 * commit and fanning it out to every downstream partition) and consumes them lands in a follow-up.
 *
 * <h3>Why source partitions, not producer instances</h3>
 *
 * <p>The question a stage has to answer is "have I received the watermark from every upstream
 * producer, so that {@code min()} across them is meaningful?". Counting <i>producer instances</i>
 * is hard: an instance can be killed without notice, leaving stale state, and the number changes on
 * every rebalance. Counting <i>source partitions</i> is robust instead, because the partition count
 * is fixed and known: it travels in-band with every report ({@code totalSourcePartitions}), a
 * partition is always owned by exactly one live instance, and when an instance dies its partitions
 * are reassigned and the new owner keeps reporting. So the manager only ever reasons about
 * partitions, never about instances. (Design agreed with the mentor; see the watermark
 * coordination-channel PoC findings.)
 *
 * <h3>Holding until complete</h3>
 *
 * <p>Until a committed watermark has been seen for <i>every</i> source partition, the stage's input
 * watermark is undefined and must be held at {@link BoundedWindow#TIMESTAMP_MIN_VALUE} — i.e. the
 * stage emits no watermark downstream. {@link #advance()} returns {@link OptionalLong#empty()} in
 * that case. A change in {@code totalSourcePartitions} (e.g. a repartition) re-opens this hold
 * until the new full set has reported, which subsumes the "new epoch / revert" rule without an
 * explicit epoch.
 *
 * <h3>Monotonicity</h3>
 *
 * <p>Beam watermarks must be non-decreasing. Each source partition's watermark is held monotonic (a
 * lower report is ignored), and the emitted stage watermark is additionally clamped so it never
 * regresses below the previously emitted value — relevant if a newly appeared partition reports an
 * older watermark after the stage had already advanced.
 *
 * <p>Not thread-safe; the caller (a single Kafka Streams processor thread) serializes access.
 */
public final class WatermarkManager {

  /** Millis the stage holds at while it cannot yet emit a watermark (the Beam minimum). */
  public static final long HOLD_MILLIS = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();

  /** Total source partitions feeding this stage, learned in-band; -1 until the first report. */
  private int expectedSourcePartitionCount = -1;

  /** Latest committed watermark per source partition (kept monotonic non-decreasing). */
  private final Map<Integer, Long> committedWatermarkByPartition = new HashMap<>();

  /** Last watermark {@link #advance()} emitted, to enforce a non-decreasing output. */
  private long lastEmittedMillis = HOLD_MILLIS;

  /**
   * Record a committed watermark reported for one source partition, together with the total source
   * partition count carried in-band with the report.
   *
   * @param sourcePartition the source partition the report is for, in {@code [0,
   *     totalSourcePartitions)}
   * @param committedWatermarkMillis the committed watermark for that partition, in event-time
   *     millis
   * @param totalSourcePartitions the total number of source partitions feeding this stage
   */
  public void observe(
      int sourcePartition, long committedWatermarkMillis, int totalSourcePartitions) {
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
      expectedSourcePartitionCount = totalSourcePartitions;
      // On a partition-count decrease, drop reports for partitions that no longer exist so
      // completeness and min() are computed over the current partition set only.
      committedWatermarkByPartition.keySet().removeIf(p -> p >= totalSourcePartitions);
    }
    // A source partition's watermark is monotonic non-decreasing; ignore an out-of-order lower
    // report.
    committedWatermarkByPartition.merge(sourcePartition, committedWatermarkMillis, Math::max);
  }

  /** True once a committed watermark has been seen for every current source partition. */
  public boolean isComplete() {
    return expectedSourcePartitionCount > 0
        && committedWatermarkByPartition.size() == expectedSourcePartitionCount;
  }

  /**
   * Advance and return the stage input watermark.
   *
   * <p>Returns {@link OptionalLong#empty()} while the stage is still holding (not every source
   * partition has reported) — the caller must emit nothing downstream in that case. Once complete,
   * returns {@code min()} over all source partitions, clamped to never regress below the previously
   * emitted value. The sequence of present values returned across calls is non-decreasing.
   */
  public OptionalLong advance() {
    if (!isComplete()) {
      return OptionalLong.empty();
    }
    long min = Long.MAX_VALUE;
    for (long w : committedWatermarkByPartition.values()) {
      min = Math.min(min, w);
    }
    long emit = Math.max(min, lastEmittedMillis);
    lastEmittedMillis = emit;
    return OptionalLong.of(emit);
  }

  /** The total source partition count learned in-band, or -1 if nothing reported yet. */
  public int expectedSourcePartitionCount() {
    return expectedSourcePartitionCount;
  }

  /** How many distinct source partitions have reported so far. */
  public int reportedPartitionCount() {
    return committedWatermarkByPartition.size();
  }
}
