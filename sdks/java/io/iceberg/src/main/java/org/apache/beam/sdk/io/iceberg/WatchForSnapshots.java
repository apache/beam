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
import java.util.stream.Collectors;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.iceberg.Snapshot;
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
class WatchForSnapshots extends PTransform<PBegin, PCollection<SnapshotInfo>> {
  private final Duration pollInterval;
  private final IcebergScanConfig scanConfig;

  WatchForSnapshots(IcebergScanConfig scanConfig, Duration pollInterval) {
    this.pollInterval = pollInterval;
    this.scanConfig = scanConfig;
  }

  @Override
  public PCollection<SnapshotInfo> expand(PBegin input) {
    return input
        .apply(Create.of(scanConfig.getTableIdentifier()))
        .apply(
            "Watch for Snapshots",
            Watch.growthOf(new SnapshotPollFn(scanConfig))
                .withPollInterval(pollInterval)
                .withOutputCoder(SnapshotInfo.getCoder()))
        .apply(
            "Strip key", MapElements.into(TypeDescriptor.of(SnapshotInfo.class)).via(KV::getValue));
  }

  private static class SnapshotPollFn extends Watch.Growth.PollFn<String, SnapshotInfo> {
    private final Gauge latestSnapshot = Metrics.gauge(SnapshotPollFn.class, "latestSnapshot");
    private final IcebergScanConfig scanConfig;
    private @Nullable Long fromSnapshotId;

    SnapshotPollFn(IcebergScanConfig scanConfig) {
      this.scanConfig = scanConfig;
    }

    @Override
    public PollResult<SnapshotInfo> apply(String tableIdentifier, Context c) {
      // fetch a fresh table to catch updated snapshots
      Table table =
          TableCache.getRefreshed(tableIdentifier, scanConfig.getCatalogConfig().catalog());
      @Nullable Long userSpecifiedToSnapshot = ReadUtils.getToSnapshot(table, scanConfig);
      boolean isComplete = userSpecifiedToSnapshot != null;
      if (fromSnapshotId == null) {
        fromSnapshotId = ReadUtils.getFromSnapshotExclusive(table, scanConfig);
      }

      Snapshot currentSnapshot = table.currentSnapshot();
      if (currentSnapshot == null || Objects.equal(currentSnapshot.snapshotId(), fromSnapshotId)) {
        // no new snapshots since last poll. return empty result.
        return getPollResult(null, isComplete);
      }
      Long currentSnapshotId = currentSnapshot.snapshotId();

      // if no upper bound is specified, we poll up to the current snapshot
      long toSnapshotId = MoreObjects.firstNonNull(userSpecifiedToSnapshot, currentSnapshotId);
      latestSnapshot.set(toSnapshotId);

      List<SnapshotInfo> snapshots =
          ReadUtils.snapshotsBetween(table, tableIdentifier, fromSnapshotId, toSnapshotId);
      return getPollResult(snapshots, isComplete);
    }

    /** Returns an appropriate PollResult based on the requested boundedness. */
    private PollResult<SnapshotInfo> getPollResult(
        @Nullable List<SnapshotInfo> snapshots, boolean isComplete) {
      List<TimestampedValue<SnapshotInfo>> timestampedValues =
          snapshots == null
              ? Collections.emptyList()
              : snapshots.stream()
                  .map(
                      snapshot ->
                          TimestampedValue.of(
                              snapshot, Instant.ofEpochMilli(snapshot.getTimestampMillis())))
                  .collect(Collectors.toList());

      return isComplete
          ? PollResult.complete(timestampedValues) // stop at specified snapshot
          : PollResult.incomplete(timestampedValues); // continue forever
    }
  }
}
