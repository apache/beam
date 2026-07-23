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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SplittableDoFn that watches an Iceberg table for new {@link Snapshot}s and emits them one at a
 * time, advancing the watermark per snapshot. Each snapshot is later processed by {@link
 * ChangelogScanner}.
 *
 * <p>The restriction tracks Snapshots via their <b>sequence numbers</b>, which are monotonic,
 * unlike snapshot IDs. The initial range starts at the sequence number of the user-configured
 * starting snapshot (or one if none configured) and runs to {@link Long#MAX_VALUE}. Each call to
 * {@code @ProcessElement} claims the sequence numbers of newly discovered snapshots in
 * chronological order.
 *
 * <p>Uses a {@link Manual} watermark estimator. After emitting a snapshot, the watermark is set to
 * that snapshot's commit time. On empty polls, the watermark is bumped to {@code now() -
 * MAX_SNAPSHOT_DISCOVERY_DELAY} to prevent downstream windows from stalling indefinitely during
 * quiet periods.
 */
@DoFn.UnboundedPerElement
class WatchForSnapshotsSdf extends DoFn<String, Long> {
  private static final Logger LOG = LoggerFactory.getLogger(WatchForSnapshotsSdf.class);
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(60);
  private static final long DEFAULT_STARTING_SEQ = 1L;

  private static final Counter snapshotsEmitted =
      Metrics.counter(WatchForSnapshotsSdf.class, "snapshotsEmitted");
  private static final Gauge latestEmittedSnapshotId =
      Metrics.gauge(WatchForSnapshotsSdf.class, "latestEmittedSnapshotId");
  // TODO(ahmedabu98): consider exposing this as a config option
  private static final Duration MAX_SNAPSHOT_DISCOVERY_DELAY = Duration.standardMinutes(5);
  private static final Long POLL_FOREVER = Long.MAX_VALUE;

  private final IcebergScanConfig scanConfig;
  private final Duration pollInterval;

  WatchForSnapshotsSdf(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
    this.pollInterval =
        MoreObjects.firstNonNull(scanConfig.getPollInterval(), DEFAULT_POLL_INTERVAL);
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction() {
    Table table =
        TableCache.getRefreshed(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());
    checkArgument(
        TableUtil.formatVersion(table) >= 2,
        "Reading CDC records from an Iceberg table requires a table version > 2, "
            + "but table %s has version %s",
        scanConfig.getTableIdentifier(),
        TableUtil.formatVersion(table));

    long toSnapshotExclusiveSeq = POLL_FOREVER;
    @Nullable Long toSnapshotId = ReadUtils.getToSnapshot(table, scanConfig);
    if (toSnapshotId != null) {
      toSnapshotExclusiveSeq =
          Preconditions.checkStateNotNull(
                      table.snapshot(toSnapshotId),
                      "Configured end snapshot %s does not exist",
                      toSnapshotId)
                  .sequenceNumber()
              + 1;
    }

    @Nullable Long fromSnapshotInclusiveId = ReadUtils.getFromSnapshotInclusive(table, scanConfig);
    long fromSnapshotInclusiveSeq;
    if (fromSnapshotInclusiveId == null) {
      fromSnapshotInclusiveSeq = DEFAULT_STARTING_SEQ; // sequence numbers start at 1
    } else {
      Snapshot fromSnapshotInclusive =
          Preconditions.checkArgumentNotNull(
              table.snapshot(fromSnapshotInclusiveId),
              "The specified starting snapshot %s does not exist",
              fromSnapshotInclusiveId);
      fromSnapshotInclusiveSeq = fromSnapshotInclusive.sequenceNumber();

      boolean sameLineage =
          toSnapshotId == null
              ? SnapshotUtil.isAncestorOf(table, fromSnapshotInclusiveId)
              : SnapshotUtil.isAncestorOf(table, toSnapshotId, fromSnapshotInclusiveId);
      checkArgument(
          sameLineage,
          "Configured starting snapshot %s is not an ancestor of %s",
          fromSnapshotInclusiveId,
          toSnapshotId == null ? "the current table" : "end snapshot " + toSnapshotId);
    }

    return new OffsetRange(
        fromSnapshotInclusiveSeq, Math.max(fromSnapshotInclusiveSeq, toSnapshotExclusiveSeq));
  }

  @NewTracker
  public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
    if (restriction.getTo() == POLL_FOREVER) {
      return new GrowableOffsetRangeTracker(
          restriction.getFrom(), this::estimateCurrentRangeEndExclusive);
    }

    return new OffsetRangeTracker(restriction);
  }

  private long estimateCurrentRangeEndExclusive() {
    Table table =
        TableCache.getAndRefreshIfStale(
            scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());

    @Nullable Long toSnapshotId = ReadUtils.getToSnapshot(table, scanConfig);
    if (toSnapshotId != null) {
      @Nullable Snapshot toSnapshot = table.snapshot(toSnapshotId);
      return toSnapshot == null ? DEFAULT_STARTING_SEQ : toSnapshot.sequenceNumber() + 1;
    }

    @Nullable Snapshot current = table.currentSnapshot();
    return current == null ? DEFAULT_STARTING_SEQ : current.sequenceNumber() + 1;
  }

  @GetRestrictionCoder
  public Coder<OffsetRange> restrictionCoder() {
    return new OffsetRange.Coder();
  }

  @GetInitialWatermarkEstimatorState
  public Instant initialWatermarkState() {
    return BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant state) {
    return new Manual(state);
  }

  @ProcessElement
  public ProcessContinuation process(
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermark,
      OutputReceiver<Long> out) {
    Table table =
        TableCache.getRefreshed(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());

    @Nullable Long userToSnapshotId = ReadUtils.getToSnapshot(table, scanConfig);
    boolean bounded = userToSnapshotId != null;

    @Nullable Snapshot current = table.currentSnapshot();
    if (current == null) {
      // no snapshots yet.
      LOG.info("Skipping scan: table is empty with no snapshots yet");
      return pauseOrStop(watermark, bounded);
    }

    // Resolve the upper bound: user-specified bounded mode, or "current" for unbounded.
    long toSnapshotId;
    long toSnapshotSeq;
    if (userToSnapshotId != null) {
      toSnapshotId = userToSnapshotId;
      toSnapshotSeq =
          Preconditions.checkStateNotNull(
                  table.snapshot(userToSnapshotId),
                  "Configured toSnapshotId %s does not exist",
                  userToSnapshotId)
              .sequenceNumber();
    } else {
      toSnapshotId = current.snapshotId();
      toSnapshotSeq = current.sequenceNumber();
    }

    long nextSeqInclusive = tracker.currentRestriction().getFrom();
    if (toSnapshotSeq < nextSeqInclusive) {
      // Nothing new since last poll.
      LOG.info("Skipping scan: nothing new since last poll.");
      return pauseOrStop(watermark, bounded);
    }

    // Collect snapshots in [nextSeqInclusive, toSnapshotSeq] chronologically
    String tableId = scanConfig.getTableIdentifier();
    List<SnapshotInfo> fresh = snapshotsAfter(table, tableId, nextSeqInclusive, toSnapshotId);
    LOG.info("Collected snapshots: {}", fresh);

    for (SnapshotInfo snap : fresh) {
      if (!tracker.tryClaim(snap.getSequenceNumber())) {
        return ProcessContinuation.stop();
      }
      Instant ts = Instant.ofEpochMilli(snap.getTimestampMillis());
      out.outputWithTimestamp(snap.getSnapshotId(), ts);

      if (watermark.currentWatermark().isBefore(ts)) {
        watermark.setWatermark(ts);
      }
      snapshotsEmitted.inc();
      latestEmittedSnapshotId.set(snap.getSnapshotId());
      LOG.info(
          "Emitted snapshot {} (sequence id: {}, commit ts: {})",
          snap.getSnapshotId(),
          snap.getSequenceNumber(),
          ts);
    }

    return pauseOrStop(watermark, bounded);
  }

  /**
   * On an empty poll, bump the watermark to {@code now() - MAX_SNAPSHOT_DISCOVERY_DELAY} so
   * downstream windows can still fire. Returns {@code stop()} when end snapshot has been reached,
   * otherwise {@code resume()} after the poll interval.
   */
  private ProcessContinuation pauseOrStop(
      ManualWatermarkEstimator<Instant> watermark, boolean bounded) {
    Duration delay =
        MoreObjects.firstNonNull(
            scanConfig.getMaxSnapshotDiscoveryDelay(), MAX_SNAPSHOT_DISCOVERY_DELAY);
    Instant idleWatermark = Instant.now().minus(delay);
    if (watermark.currentWatermark().isBefore(idleWatermark)) {
      LOG.info(
          "Sitting idle for {} seconds. Bumping watermark to {}",
          TimeUnit.MILLISECONDS.toSeconds(
              Instant.now().getMillis() - watermark.currentWatermark().getMillis()),
          idleWatermark);
      watermark.setWatermark(idleWatermark);
    }
    return bounded
        ? ProcessContinuation.stop()
        : ProcessContinuation.resume().withResumeDelay(pollInterval);
  }

  /**
   * Returns snapshots with sequence number in {@code [nextSeqInclusive, toSnapshotSeq]}, keyed off
   * the lineage ending at {@code toSnapshotId}.
   */
  @SuppressWarnings("return") // ancestorsOf accepts null returns as a "stop" signal
  static List<SnapshotInfo> snapshotsAfter(
      Table table, String tableIdentifier, long nextSeqInclusive, long toSnapshotId) {

    List<SnapshotInfo> snapshots = new ArrayList<>();
    // ancestorsOf returns an iterable of snapshots looking backwards.
    // we'll need to reverse it to process snapshots chronologically.
    for (Snapshot snapshot :
        SnapshotUtil.ancestorsOf(
            toSnapshotId, snapshotId -> snapshotId != null ? table.snapshot(snapshotId) : null)) {
      if (snapshot.sequenceNumber() < nextSeqInclusive) {
        break;
      }
      snapshots.add(SnapshotInfo.fromSnapshot(snapshot, tableIdentifier));
    }
    Collections.reverse(snapshots);
    return snapshots;
  }
}
