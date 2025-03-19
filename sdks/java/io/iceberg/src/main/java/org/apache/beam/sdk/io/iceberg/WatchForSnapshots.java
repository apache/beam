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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps watch over an Iceberg table and continuously outputs a range of snapshots, at the specified
 * interval.
 *
 * <p>A downstream transform will create a list of read tasks for each range.
 */
class WatchForSnapshots extends PTransform<PBegin, PCollection<KV<String, List<SnapshotInfo>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(WatchForSnapshots.class);
  private final Duration pollInterval;
  private final IcebergScanConfig scanConfig;

  WatchForSnapshots(IcebergScanConfig scanConfig, Duration pollInterval) {
    this.pollInterval = pollInterval;
    this.scanConfig = scanConfig;
  }

  @Override
  public PCollection<KV<String, List<SnapshotInfo>>> expand(PBegin input) {
    return input
        .apply(Create.of(scanConfig.getTableIdentifier()))
        .apply(
            "Scan Table Snapshots",
            Watch.growthOf(new SnapshotPollFn(scanConfig))
                .withPollInterval(pollInterval)
                .withOutputCoder(ListCoder.of(SnapshotInfo.getCoder())))
        .apply("Persist Snapshot Progress", ParDo.of(new PersistSnapshotProgress()));
  }

  /**
   * Periodically scans the table for new snapshots, emitting a list for each new snapshot range.
   *
   * <p>This tracks progress locally but is not resilient to retries -- upon worker failure, it will
   * restart from the initial starting strategy. Resilience is handled downstream by {@link
   * PersistSnapshotProgress}.
   */
  private static class SnapshotPollFn extends Watch.Growth.PollFn<String, List<SnapshotInfo>> {
    private final IcebergScanConfig scanConfig;
    private @Nullable Long fromSnapshotId;
    boolean isCacheSetup = false;

    SnapshotPollFn(IcebergScanConfig scanConfig) {
      this.scanConfig = scanConfig;
    }

    @Override
    public PollResult<List<SnapshotInfo>> apply(String tableIdentifier, Context c) {
      if (!isCacheSetup) {
        TableCache.setup(scanConfig);
        isCacheSetup = true;
      }
      Table table = TableCache.getRefreshed(tableIdentifier);

      @Nullable Long userSpecifiedToSnapshot = ReadUtils.getToSnapshot(table, scanConfig);
      boolean isComplete = userSpecifiedToSnapshot != null;
      if (fromSnapshotId == null) {
        // first scan, initialize starting point with user config
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

      List<SnapshotInfo> snapshots =
          ReadUtils.snapshotsBetween(table, tableIdentifier, fromSnapshotId, toSnapshotId);

      fromSnapshotId = currentSnapshotId;
      return getPollResult(snapshots, isComplete);
    }

    private PollResult<List<SnapshotInfo>> getPollResult(
        @Nullable List<SnapshotInfo> snapshots, boolean isComplete) {
      ImmutableList.Builder<TimestampedValue<List<SnapshotInfo>>> timestampedSnapshots =
          ImmutableList.builder();
      if (snapshots != null) {
        // watermark based on the oldest observed snapshot in this poll interval
        Instant watermark = Instant.ofEpochMilli(snapshots.get(0).getTimestampMillis());
        timestampedSnapshots.add(TimestampedValue.of(snapshots, watermark));
      }

      return isComplete
          ? PollResult.complete(timestampedSnapshots.build()) // stop at specified snapshot
          : PollResult.incomplete(timestampedSnapshots.build()); // continue forever
    }
  }

  /**
   * Stateful DoFn that persists the latest observed snapshot ID to state, making sure we pick up
   * where we left off in case of a worker crash.
   */
  // Ideally, Watch.Growth would support state out of the box, but that is a bigger change.
  static class PersistSnapshotProgress
      extends DoFn<KV<String, List<SnapshotInfo>>, KV<String, List<SnapshotInfo>>> {
    private final Gauge latestSnapshot = Metrics.gauge(SnapshotPollFn.class, "latestSnapshot");
    private final Counter snapshotsObserved =
        Metrics.counter(SnapshotPollFn.class, "snapshotsObserved");

    @StateId("latestObservedSnapshotId")
    @SuppressWarnings("UnusedVariable")
    private final StateSpec<ValueState<Long>> latestObservedSnapshotId = StateSpecs.value();

    @ProcessElement
    public void process(
        @Element KV<String, List<SnapshotInfo>> element,
        final @AlwaysFetched @StateId("latestObservedSnapshotId") ValueState<Long>
                latestObservedSnapshotId,
        OutputReceiver<KV<String, List<SnapshotInfo>>> out) {
      List<SnapshotInfo> snapshots = element.getValue();

      @Nullable Long latest = latestObservedSnapshotId.read();
      if (latest != null) {
        int newSnapshotIndex = 0;
        for (int i = 0; i < snapshots.size(); i++) {
          if (snapshots.get(i).getSnapshotId() == latest) {
            newSnapshotIndex = i + 1;
            break;
          }
        }
        if (newSnapshotIndex > 0) {
          snapshots = snapshots.subList(newSnapshotIndex, snapshots.size());
        }
      }

      SnapshotInfo checkpoint = Iterables.getLast(snapshots);
      out.output(KV.of(element.getKey(), snapshots));
      LOG.info(
          "New poll fetched {} snapshots: {}. Checkpointing at snapshot {} of timestamp {}.",
          snapshots.size(),
          snapshots.stream().map(SnapshotInfo::getSnapshotId).collect(Collectors.toList()),
          checkpoint.getSnapshotId(),
          checkpoint.getTimestampMillis());

      latestObservedSnapshotId.write(checkpoint.getSnapshotId());
      latestSnapshot.set(checkpoint.getSnapshotId());
      snapshotsObserved.inc(snapshots.size());
    }
  }
}
