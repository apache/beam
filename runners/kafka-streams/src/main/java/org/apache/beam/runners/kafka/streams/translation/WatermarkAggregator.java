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
import java.util.Set;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;

/**
 * Computes a transform's input watermark from the reports of its upstream transforms.
 *
 * <p>A report says which transform produced it, which partition of that transform it is for, and
 * how many partitions that transform has (see {@link WatermarkPayload}); a producer stamps its own
 * identity without regard to who consumes it. This is the consuming side, used by every transform
 * that aggregates a watermark — ExecutableStage, GroupByKey, Flatten.
 *
 * <p>It is constructed with the upstream transform ids the consumer expects, known from the
 * pipeline graph at translation time, and tracks each with its own {@link WatermarkManager}. The
 * input watermark is the minimum across them, defined only once every expected upstream is ready;
 * until then {@link #advance()} returns {@link BoundedWindow#TIMESTAMP_MIN_VALUE} and the caller
 * emits nothing.
 *
 * <p>Not thread-safe; the calling Kafka Streams processor thread serializes access.
 */
final class WatermarkAggregator {

  /** Upstream transform ids this consumer must hear from, fixed by the pipeline graph. */
  private final Set<String> expectedUpstreamTransformIds;

  /** Per-upstream-transform partition tracking. */
  private final Map<String, WatermarkManager> managerByTransformId = new HashMap<>();

  WatermarkAggregator(Set<String> expectedUpstreamTransformIds) {
    Preconditions.checkArgument(
        !expectedUpstreamTransformIds.isEmpty(), "expectedUpstreamTransformIds must not be empty");
    this.expectedUpstreamTransformIds = ImmutableSet.copyOf(expectedUpstreamTransformIds);
  }

  /**
   * Records one upstream watermark report. A report from a transform this consumer does not expect
   * indicates a translation wiring bug and fails fast.
   */
  void observe(WatermarkPayload report) {
    String transformId = report.getTransformId();
    if (!expectedUpstreamTransformIds.contains(transformId)) {
      throw new IllegalStateException(
          "Received a watermark report from unexpected transform "
              + transformId
              + "; expected one of "
              + expectedUpstreamTransformIds);
    }
    managerByTransformId
        .computeIfAbsent(transformId, id -> new WatermarkManager())
        .observe(
            report.getSourcePartition(),
            new Instant(report.getWatermarkMillis()),
            report.getTotalSourcePartitions());
  }

  /**
   * Returns the aggregate input watermark: {@code min()} across all expected upstream transforms,
   * or {@link BoundedWindow#TIMESTAMP_MIN_VALUE} while any upstream transform has not yet fully
   * reported (the hold).
   */
  Instant advance() {
    if (managerByTransformId.size() < expectedUpstreamTransformIds.size()) {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    Instant min = BoundedWindow.TIMESTAMP_MAX_VALUE;
    for (WatermarkManager manager : managerByTransformId.values()) {
      // A not-yet-ready manager advances to TIMESTAMP_MIN_VALUE, which correctly holds the min.
      Instant watermark = manager.advance();
      if (watermark.isBefore(min)) {
        min = watermark;
      }
    }
    return min;
  }
}
