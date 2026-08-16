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
 * Computes a transform's input watermark from the watermark reports of its upstream transforms.
 *
 * <p>A watermark report carries three orthogonal pieces of information (see {@link
 * WatermarkPayload}): <i>which transform</i> produced it, <i>which partition</i> (physical
 * instance) of that transform it is for, and <i>how many partitions</i> that transform has. A
 * producer stamps its own identity without regard to who consumes the report. This aggregator is
 * the consuming side, used by every transform that aggregates a watermark — ExecutableStage,
 * GroupByKey, Flatten (and CombinePerKey later):
 *
 * <ul>
 *   <li>It is constructed with the set of upstream transform ids the consumer expects, known from
 *       the pipeline graph at translation time (a single-input transform passes its one parent; a
 *       Flatten passes the producers of all of its input PCollections).
 *   <li>Per upstream transform it tracks partitions with a dedicated {@link WatermarkManager},
 *       which holds until every partition of that transform has reported and keeps each partition
 *       monotonic.
 *   <li>The aggregate input watermark is the {@code min()} across the upstream transforms'
 *       watermarks, defined only once <em>every</em> expected upstream transform is ready; until
 *       then {@link #advance()} returns {@link BoundedWindow#TIMESTAMP_MIN_VALUE} and the caller
 *       emits nothing.
 * </ul>
 *
 * <p>Not thread-safe; the caller (a single Kafka Streams processor thread) serializes access.
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
