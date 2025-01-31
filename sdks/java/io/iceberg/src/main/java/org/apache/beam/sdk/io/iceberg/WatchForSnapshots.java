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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.transforms.Watch.Growth.PollResult;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Keeps watch over an Iceberg table and continuously outputs a range of snapshots, at the specified
 * interval.
 *
 * <p>A downstream transform will create a list of read tasks for each range.
 */
class WatchForSnapshots extends PTransform<PBegin, PCollection<SnapshotRange>> {
  private final Duration pollInterval;
  private final IcebergScanConfig scanConfig;

  WatchForSnapshots(IcebergScanConfig scanConfig, Duration pollInterval) {
    this.pollInterval = pollInterval;
    this.scanConfig = scanConfig;
  }

  @Override
  public PCollection<SnapshotRange> expand(PBegin input) {
    return input
        .apply(Create.of(scanConfig.getTableIdentifier()))
        .apply(
            "Watch for Snapshots",
            Watch.growthOf(new SnapshotPollFn(scanConfig)).withPollInterval(pollInterval))
        .apply(
            "Strip key",
            MapElements.into(TypeDescriptor.of(SnapshotRange.class)).via(KV::getValue));
  }

  private static class SnapshotPollFn extends Watch.Growth.PollFn<String, SnapshotRange> {
    private final Gauge latestSnapshot = Metrics.gauge(SnapshotPollFn.class, "latestSnapshot");
    private final IcebergScanConfig scanConfig;
    private @Nullable Long fromSnapshot;

    SnapshotPollFn(IcebergScanConfig scanConfig) {
      this.scanConfig = scanConfig;
      this.fromSnapshot = scanConfig.getFromSnapshotExclusive();
    }

    @Override
    public PollResult<SnapshotRange> apply(String tableIdentifier, Context c) {
      // fetch a fresh table to catch updated snapshots
      Table table =
          TableCache.getRefreshed(tableIdentifier, scanConfig.getCatalogConfig().catalog());
      Instant timestamp = Instant.now();

      Long currentSnapshot = table.currentSnapshot().snapshotId();
      if (currentSnapshot.equals(fromSnapshot)) {
        // no new snapshot since last poll. return empty result.
        return getPollResult(null, timestamp);
      }

      // if no upper bound is specified, we read up to the current snapshot
      Long toSnapshot = MoreObjects.firstNonNull(scanConfig.getSnapshot(), currentSnapshot);
      latestSnapshot.set(toSnapshot);

      SnapshotRange range =
          SnapshotRange.builder()
              .setFromSnapshotExclusive(fromSnapshot)
              .setToSnapshot(toSnapshot)
              .setTableIdentifierString(tableIdentifier)
              .build();

      // update lower bound to current snapshot
      fromSnapshot = currentSnapshot;

      return getPollResult(range, timestamp);
    }

    /** Returns an appropriate PollResult based on the requested boundedness. */
    private PollResult<SnapshotRange> getPollResult(
        @Nullable SnapshotRange range, Instant timestamp) {
      List<TimestampedValue<SnapshotRange>> timestampedValues =
          range == null
              ? Collections.emptyList()
              : Collections.singletonList(TimestampedValue.of(range, timestamp));

      return scanConfig.getToSnapshot() != null
          ? PollResult.complete(timestampedValues) // stop at specified snapshot
          : PollResult.incomplete(timestampedValues); // continue forever
    }
  }
}
