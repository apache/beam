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
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.iceberg.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Watches for Iceberg table {@link org.apache.iceberg.Snapshot}s and continuously outputs a range
 * of snapshots.
 *
 * <p>Downstream, a collection of scan tasks are created for each range.
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
        .apply(Impulse.create())
        .apply(
            "Watch for snapshots",
            Watch.growthOf(new SnapshotPollFn(scanConfig)).withPollInterval(pollInterval))
        .apply(
            "Create snapshot intervals",
            MapElements.into(TypeDescriptor.of(SnapshotRange.class))
                .via(
                    result -> {
                      long from = result.getValue().getKey();
                      long to = result.getValue().getValue();

                      return SnapshotRange.builder()
                          .setTable(scanConfig.getTableIdentifier())
                          .setFromSnapshot(from)
                          .setToSnapshot(to)
                          .build();
                    }));
  }

  private static class SnapshotPollFn extends Watch.Growth.PollFn<byte[], KV<Long, Long>> {
    private final Gauge latestSnapshot = Metrics.gauge(SnapshotPollFn.class, "latestSnapshot");
    private final IcebergScanConfig scanConfig;
    private Long fromSnapshot;

    SnapshotPollFn(IcebergScanConfig scanConfig) {
      this.scanConfig = scanConfig;
      this.fromSnapshot =
          scanConfig.getFromSnapshotExclusive() != null
              ? scanConfig.getFromSnapshotExclusive()
              : -1;
    }

    @Override
    public PollResult<KV<Long, Long>> apply(byte[] element, Context c) throws Exception {
      // fetch a fresh table to catch updated snapshots
      Table table =
          TableCache.getRefreshed(
              scanConfig.getTableIdentifier(), scanConfig.getCatalogConfig().catalog());
      Instant timestamp = Instant.now();

      Long currentSnapshot = table.currentSnapshot().snapshotId();
      if (currentSnapshot.equals(fromSnapshot)) {
        // no new values since last poll. return empty result.
        return getPollResult(null, timestamp);
      }

      // we are reading data either up to a specified snapshot or up to the latest available
      // snapshot
      Long toSnapshot =
          scanConfig.getToSnapshot() != null ? scanConfig.getToSnapshot() : currentSnapshot;
      latestSnapshot.set(toSnapshot);

      KV<Long, Long> fromTo = KV.of(fromSnapshot, toSnapshot);

      // update lower bound to current snapshot
      fromSnapshot = currentSnapshot;

      return getPollResult(fromTo, timestamp);
    }

    /** Returns an appropriate PollResult based on the requested boundedness. */
    private PollResult<KV<Long, Long>> getPollResult(
        @Nullable KV<Long, Long> fromTo, Instant timestamp) {
      List<TimestampedValue<KV<Long, Long>>> timestampedValues =
          fromTo == null
              ? Collections.emptyList()
              : Collections.singletonList(TimestampedValue.of(fromTo, timestamp));

      return scanConfig.getToSnapshot() != null
          ? PollResult.complete(timestampedValues) // stop at specified
          : PollResult.incomplete(timestampedValues); // continue forever
    }
  }
}
