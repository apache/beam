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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ThreadPools;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CDC sink's commit stage: commits each {@code (destination, window)}'s merged writer outputs
 * (represented as {@link ShardDeltaFiles}) as a single Iceberg snapshot, in ascending window-end
 * order. Re-keys by destination, gathers all shards per {@code (dest, window)}, captures the window
 * end, then re-windows into the global window for the stateful {@link OrderedCommitFn}.
 *
 * <p>Each commit writes the window's end millis to the snapshot summary as an idempotency token,
 * keyed by the sink's unique {@code sinkId}. The committer recovers it by scanning snapshot
 * ancestry: once on first touch of a destination, and again on every commit fire. Any window whose
 * end is at or below the recovered token has already been committed, so it is skipped.
 */
class CommitDeltas
    extends PTransform<PCollection<ShardDeltaFiles>, PCollection<KV<String, SnapshotInfo>>> {

  private static final Logger LOG = LoggerFactory.getLogger(CommitDeltas.class);

  // test-only attributes
  @VisibleForTesting static volatile @Nullable Runnable preCommitHookForTest = null;
  @VisibleForTesting static volatile @Nullable BiConsumer<Long, List<Long>> onFireForTest = null;

  private final IcebergCatalogConfig catalogConfig;
  private final String sinkId;
  private final Map<String, String> snapshotProperties;
  private final long heartbeatMillis;

  /** The expansion's runId. */
  private final String runId;

  private Clock clock = System::currentTimeMillis;

  @VisibleForTesting
  CommitDeltas(IcebergCatalogConfig catalogConfig, String sinkId) {
    this(catalogConfig, sinkId, null, null);
  }

  @VisibleForTesting
  CommitDeltas(
      IcebergCatalogConfig catalogConfig,
      String sinkId,
      @Nullable Map<String, String> snapshotProperties,
      @Nullable Long tokenHeartbeatMillis) {
    this(
        catalogConfig,
        sinkId,
        snapshotProperties,
        tokenHeartbeatMillis,
        UUID.randomUUID().toString());
  }

  CommitDeltas(
      IcebergCatalogConfig catalogConfig,
      String sinkId,
      @Nullable Map<String, String> snapshotProperties,
      @Nullable Long tokenHeartbeatMillis,
      String runId) {
    this.catalogConfig = catalogConfig;
    this.sinkId = sinkId;
    this.snapshotProperties =
        snapshotProperties == null ? Collections.emptyMap() : snapshotProperties;
    this.heartbeatMillis = tokenHeartbeatMillis == null ? 0L : tokenHeartbeatMillis;
    this.runId = runId;
  }

  /** Overrides the committer's clock to test skew deterministically. */
  @VisibleForTesting
  CommitDeltas withClockForTest(Clock clock) {
    this.clock = clock;
    return this;
  }

  @Override
  public PCollection<KV<String, SnapshotInfo>> expand(PCollection<ShardDeltaFiles> input) {
    boolean streaming = input.isBounded() == PCollection.IsBounded.UNBOUNDED;
    return input
        .apply("KeyByDestination", WithKeys.of(ShardDeltaFiles::getTableIdentifierString))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), WriteDeltas.shardDeltaFilesCoder()))
        // One element per (dest, window): every shard's output for the pair.
        .apply("GatherShardsPerWindow", GroupByKey.create())
        .apply("CaptureWindowEnd", ParDo.of(new CaptureWindowEndFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), windowedCommitCoder()))
        .apply("ToGlobalWindow", Window.into(new GlobalWindows()))
        .apply(
            "OrderedCommit",
            ParDo.of(
                new OrderedCommitFn(
                    catalogConfig,
                    sinkId,
                    runId,
                    snapshotProperties,
                    heartbeatMillis,
                    clock,
                    streaming)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), snapshotInfoCoder()));
  }

  private static Coder<SnapshotInfo> snapshotInfoCoder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(SnapshotInfo.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Could not build a coder for SnapshotInfo.", e);
    }
  }

  static Coder<WindowedCommit> windowedCommitCoder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(WindowedCommit.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Could not build a coder for WindowedCommit.", e);
    }
  }

  /** Max number of file paths listed in a skip-path WARN before truncating. */
  private static final int SKIP_PATHS_LOGGED = 5;

  /** Every file path a window carries: data files first then delete files. */
  @VisibleForTesting
  static List<String> filePaths(WindowedCommit wc) {
    List<String> paths = new ArrayList<>();
    List<String> deletePaths = new ArrayList<>();
    for (ShardDeltaFiles shard : wc.getFiles()) {
      for (SerializableDataFile dataFile : shard.getDataFiles()) {
        paths.add(dataFile.getPath());
      }
      for (SerializableDeleteFile deleteFile : shard.getDeleteFiles()) {
        deletePaths.add(deleteFile.getLocation());
      }
    }
    paths.addAll(deletePaths);
    return paths;
  }

  /** Renders up to {@link #SKIP_PATHS_LOGGED} of a skipped window's file paths, plus a count. */
  @VisibleForTesting
  static String describeSkippedFiles(WindowedCommit wc) {
    return describePaths(filePaths(wc));
  }

  private static String describePaths(List<String> paths) {
    StringBuilder sb = new StringBuilder();
    int shown = Math.min(SKIP_PATHS_LOGGED, paths.size());
    for (int i = 0; i < shown; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(paths.get(i));
    }
    if (paths.size() > SKIP_PATHS_LOGGED) {
      sb.append(" (… ").append(paths.size() - SKIP_PATHS_LOGGED).append(" more)");
    }
    return sb.toString();
  }

  /**
   * One {@code (destination, window)}'s merged writer outputs, tagged with the window's end.
   *
   * <p>The window end is a deterministic {@code FixedWindows} boundary and doubles as the
   * restart-safe idempotency token. A window whose end is at or below the recovered
   * committed-through token is skipped.
   */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class WindowedCommit {
    @SchemaFieldNumber("0")
    public abstract long getWindowEndMs();

    @SchemaFieldNumber("1")
    public abstract List<ShardDeltaFiles> getFiles();

    public static WindowedCommit of(long windowEndMs, List<ShardDeltaFiles> files) {
      return new AutoValue_CommitDeltas_WindowedCommit(windowEndMs, files);
    }
  }

  /** Folds a {@code (dest, window)}'s shard outputs into one {@link WindowedCommit}. */
  static class CaptureWindowEndFn
      extends DoFn<KV<String, Iterable<ShardDeltaFiles>>, KV<String, WindowedCommit>> {
    @ProcessElement
    public void process(
        @Element KV<String, Iterable<ShardDeltaFiles>> element,
        BoundedWindow window,
        OutputReceiver<KV<String, WindowedCommit>> out) {
      long windowEndMs = window.maxTimestamp().getMillis();
      List<ShardDeltaFiles> files = Lists.newArrayList(element.getValue());
      out.outputWithTimestamp(
          KV.of(element.getKey(), WindowedCommit.of(windowEndMs, files)), window.maxTimestamp());
    }
  }

  /**
   * A serializable millisecond clock; injectable via {@link #withClockForTest} so heartbeat tests
   * can skew "now" deterministically.
   */
  @FunctionalInterface
  interface Clock extends LongSupplier, Serializable {}

  /** The committer's metrics, all namespaced under {@link CommitDeltas}. */
  static final class CommitterMetrics implements Serializable {

    final Counter snapshotsCreated = Metrics.counter(CommitDeltas.class, "snapshotsCreated");
    final Counter committedDataFiles = Metrics.counter(CommitDeltas.class, "committedDataFiles");
    final Counter committedDeleteFiles =
        Metrics.counter(CommitDeltas.class, "committedDeleteFiles");
    final Counter committedRecords = Metrics.counter(CommitDeltas.class, "committedRecords");
    final Counter committedEqualityDeleteRecords =
        Metrics.counter(CommitDeltas.class, "committedEqualityDeleteRecords");
    final Counter committedBytes = Metrics.counter(CommitDeltas.class, "committedBytes");
    final Distribution commitDurationMs =
        Metrics.distribution(CommitDeltas.class, "commitDurationMs");
    final Counter commitFailures = Metrics.counter(CommitDeltas.class, "commitFailures");

    final Counter alreadyCommittedWindowsSkipped =
        Metrics.counter(CommitDeltas.class, "alreadyCommittedWindowsSkipped");
    final Counter orphanFiles = Metrics.counter(CommitDeltas.class, "orphanFiles");
    final Counter tokenParseFailures = Metrics.counter(CommitDeltas.class, "tokenParseFailures");
    final Counter suspectedTokenExpiry =
        Metrics.counter(CommitDeltas.class, "suspectedTokenExpiry");
    final Counter crossWindowSequenceInversions =
        Metrics.counter(CommitDeltas.class, "crossWindowSequenceInversions");
    final Counter specMismatchedWindows =
        Metrics.counter(CommitDeltas.class, "specMismatchedWindows");

    final Counter heartbeatCommits = Metrics.counter(CommitDeltas.class, "heartbeatCommits");
  }

  /**
   * The ordered, idempotent committer. Keyed by destination; keeps the last-committed window-end
   * and a bag of pending windows, plus one event-time timer armed at the earliest pending end. On
   * fire, it commits every pending window at or below the input watermark, in ascending order, each
   * as its own single-snapshot commit. A configured {@code tokenHeartbeatMillis} adds an idle
   * token-refresh timer ({@link #onHeartbeat}).
   */
  static class OrderedCommitFn extends DoFn<KV<String, WindowedCommit>, KV<String, SnapshotInfo>> {

    private final CommitterMetrics metrics = new CommitterMetrics();

    private final IcebergCatalogConfig catalogConfig;
    private final String sinkId;
    private final String runId;
    private final Map<String, String> snapshotProperties;
    private final CommitToken token;

    /** Idle token-refresh heartbeat interval in millis; {@code 0} = disabled. */
    private final long heartbeatMillis;

    private final Clock clock;
    private final boolean streaming;

    @StateId("lastCommittedEndMs")
    private final StateSpec<ValueState<Long>> lastCommittedEndMsSpec =
        StateSpecs.value(VarLongCoder.of());

    /** The max source sequence number committed so far. */
    @StateId("lastCommittedMaxSeq")
    private final StateSpec<ValueState<Long>> lastCommittedMaxSeqSpec =
        StateSpecs.value(VarLongCoder.of());

    @StateId("pending")
    private final StateSpec<BagState<WindowedCommit>> pendingSpec;

    /**
     * The earliest uncommitted window-end in {@link #pendingSpec} (what the commit timer must be
     * armed at).
     */
    @StateId("earliestPending")
    private final StateSpec<ValueState<Long>> earliestPendingSpec =
        StateSpecs.value(VarLongCoder.of());

    /** The partition-spec id pinned for this destination under {@link #pinnedRunIdSpec}'s runId. */
    @StateId("pinnedSpecId")
    private final StateSpec<ValueState<Integer>> pinnedSpecIdSpec =
        StateSpecs.value(VarIntCoder.of());

    /** The runId the spec pin was taken under; a different live runId re-pins. */
    @StateId("pinnedRunId")
    private final StateSpec<ValueState<String>> pinnedRunIdSpec =
        StateSpecs.value(StringUtf8Coder.of());

    @TimerId("commit")
    private final TimerSpec commitTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    /** Processing-time timer that fires the idle token-refresh heartbeat (when configured). */
    @TimerId("heartbeat")
    private final TimerSpec heartbeatTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    OrderedCommitFn(
        IcebergCatalogConfig catalogConfig,
        String sinkId,
        String runId,
        Map<String, String> snapshotProperties,
        long heartbeatMillis,
        Clock clock,
        boolean streaming) {
      this.catalogConfig = catalogConfig;
      this.sinkId = sinkId;
      this.runId = runId;
      this.snapshotProperties = snapshotProperties;
      this.heartbeatMillis = heartbeatMillis;
      this.clock = clock;
      this.streaming = streaming;
      this.token =
          new CommitToken(sinkId, runId, metrics.tokenParseFailures, metrics.suspectedTokenExpiry);
      this.pendingSpec = StateSpecs.bag(windowedCommitCoder());
    }

    @RequiresStableInput
    @ProcessElement
    public void process(
        @Element KV<String, WindowedCommit> element,
        @StateId("lastCommittedEndMs") ValueState<Long> lastCommittedEndMs,
        @StateId("lastCommittedMaxSeq") ValueState<Long> lastMaxSeq,
        @StateId("pending") BagState<WindowedCommit> pending,
        @StateId("earliestPending") ValueState<Long> earliestPending,
        @TimerId("commit") Timer commitTimer,
        @TimerId("heartbeat") Timer heartbeatTimer) {
      String dest = element.getKey();
      WindowedCommit wc = element.getValue();

      long lastCommittedMs = recoverOrReadCommitted(dest, lastCommittedEndMs, lastMaxSeq);
      // Sets the idle heartbeat timer on every element; it fires after a full interval of idleness.
      setHeartbeat(heartbeatTimer);
      if (wc.getWindowEndMs() <= lastCommittedMs) {
        // Already committed (retry/duplicate, or a rerun under a stable sink_id).
        skipAlreadyCommitted(dest, wc, lastCommittedMs);
        return;
      }

      long earliest =
          Math.min(firstNonNull(earliestPending.read(), Long.MAX_VALUE), wc.getWindowEndMs());
      pending.add(wc);
      setCommitTimer(earliest, earliestPending, commitTimer);
    }

    /** Returns the last committed window-end for {@code dest}. */
    private long recoverOrReadCommitted(
        String dest, ValueState<Long> lastCommittedEndMs, ValueState<Long> lastMaxSeq) {
      @Nullable Long stored = lastCommittedEndMs.read();
      if (stored != null) {
        return stored;
      }
      CommitToken.Recovered recovered = token.recoverFromTable(catalogConfig, dest);
      // Throw before writing to state
      checkRecoveredTokenNotBatchEnd(dest, recovered.committedThroughMs);
      lastCommittedEndMs.write(recovered.committedThroughMs);
      lastMaxSeq.write(recovered.maxCommittedSeq);
      return recovered.committedThroughMs;
    }

    /**
     * Fails a streaming destination whose recovered token is the batch token (the global-window end
     * every bounded load commits under). Every real-time window's end falls below it, so the run
     * would silently skip every window forever.
     */
    private void checkRecoveredTokenNotBatchEnd(String dest, long recoveredMs) {
      if (!streaming || recoveredMs != GlobalWindow.INSTANCE.maxTimestamp().getMillis()) {
        return;
      }
      throw new IllegalStateException(
          "CDC sink '"
              + sinkId
              + "' recovered a committed-through token for table '"
              + dest
              + "' equal to the global-window end ("
              + recoveredMs
              + " ms): this sink_id was last used by a batch (bounded) load, whose single "
              + "global-window commit claims every event-time window. A streaming run reusing it "
              + "would skip every window forever. Use a different sink_id for the streaming "
              + "continuation.");
    }

    @RequiresStableInput
    @OnTimer("commit")
    public void onCommit(
        OnTimerContext c,
        @Key String dest,
        @StateId("lastCommittedEndMs") ValueState<Long> lastCommittedEndMs,
        @StateId("lastCommittedMaxSeq") ValueState<Long> lastMaxSeq,
        @StateId("pending") BagState<WindowedCommit> pending,
        @StateId("earliestPending") ValueState<Long> earliestPending,
        @StateId("pinnedSpecId") ValueState<Integer> pinnedSpecId,
        @StateId("pinnedRunId") ValueState<String> pinnedRunId,
        @TimerId("commit") Timer timer,
        OutputReceiver<KV<String, SnapshotInfo>> out) {
      // The "commit" timer is always armed at the earliest pending window's end.
      long earliestPendingWindowEnd = c.timestamp().getMillis();
      // Current input watermark tells us that all past pending windows are safe to commit.
      long inputWatermark = timer.getCurrentRelativeTime().getMillis();
      long fireWatermarkMs = Math.max(earliestPendingWindowEnd, inputWatermark);

      List<WindowedCommit> all = Lists.newArrayList(pending.read());
      all.sort(Comparator.comparingLong(WindowedCommit::getWindowEndMs));

      long committedMaxSeq = firstNonNull(lastMaxSeq.read(), Long.MIN_VALUE);
      // One table load + ancestry scan per fire
      Table table = TableCache.getRefreshed(catalogConfig, dest);

      long tableTokenMs = token.recoverFrom(table, dest).committedThroughMs;
      checkRecoveredTokenNotBatchEnd(dest, tableTokenMs);

      long lastCommittedMs =
          Math.max(firstNonNull(lastCommittedEndMs.read(), Long.MIN_VALUE), tableTokenMs);

      // Ascending triage:
      // 1. already-committed ends skip loudly
      // 2. ends past the fire watermark stay pending
      // 3. the rest commit now (in this fire)
      // Duplicate window-ends are parked and skipped only after its window's commit lands.
      List<WindowedCommit> committable = new ArrayList<>();
      List<WindowedCommit> duplicates = new ArrayList<>();
      List<WindowedCommit> remaining = new ArrayList<>();
      long selectedThrough = lastCommittedMs;
      for (WindowedCommit wc : all) {
        if (wc.getWindowEndMs() <= lastCommittedMs) {
          skipAlreadyCommitted(dest, wc, lastCommittedMs);
        } else if (wc.getWindowEndMs() > fireWatermarkMs) {
          // not yet safe to commit. there could be earlier windows that haven't reached the pending
          // bag yet
          remaining.add(wc);
        } else if (wc.getWindowEndMs() <= selectedThrough) {
          duplicates.add(wc); // same end as a window this fire is about to commit
        } else {
          committable.add(wc);
          selectedThrough = wc.getWindowEndMs();
        }
      }

      // Commit each committable window as its own snapshot
      List<Long> committedEnds = new ArrayList<>(committable.size());
      // The spec this run pinned, or null when the pin is from an earlier run. A pipeline update
      // keeps committer state but regenerates the runId. We drop the pin if it's stale (if the
      // construction-time runId doesn't match the preserved state's pinnedRunId). Doing this lets
      // the new run re-pin onto the current live spec.
      @Nullable
      Integer currentSpecId = runId.equals(pinnedRunId.read()) ? pinnedSpecId.read() : null;
      for (WindowedCommit wc : committable) {
        CommitSummary summary;
        try {
          summary = commitOneWindow(table, wc, currentSpecId);
        } catch (RuntimeException e) {
          // Later windows must not commit past a failed one.
          // Count and rethrow before any state write, so the retry re-fires with the pending bag.
          metrics.commitFailures.inc();
          throw commitFailure(dest, wc, all, lastCommittedMs, e);
        }

        currentSpecId =
            rePinAndWarnOnMismatch(dest, wc, summary, currentSpecId, pinnedSpecId, pinnedRunId);

        // Pair the window with its own snapshot by token
        SnapshotInfo info = identifyCommitted(table, dest, wc, summary);
        lastCommittedMs = wc.getWindowEndMs();
        out.output(KV.of(dest, info));
        committedMaxSeq = detectSequenceInversion(dest, wc, committedMaxSeq);
        committedEnds.add(wc.getWindowEndMs());
        for (WindowedCommit dup : duplicates) {
          if (dup.getWindowEndMs() == wc.getWindowEndMs()) {
            // Release this duplicate now that its window's commit succeeded.
            skipSameFireDuplicate(dest, dup, wc);
          }
        }
      }
      lastCommittedEndMs.write(lastCommittedMs);
      lastMaxSeq.write(committedMaxSeq);
      pending.clear();
      long earliest = Long.MAX_VALUE;
      for (WindowedCommit wc : remaining) {
        pending.add(wc);
        earliest = Math.min(earliest, wc.getWindowEndMs());
      }
      // Re-arm the timer at the next-earliest pending end to account for remaining windows
      setCommitTimer(earliest, earliestPending, timer);
      BiConsumer<Long, List<Long>> onFire = onFireForTest;
      if (onFire != null) {
        onFire.accept(fireWatermarkMs, committedEnds);
      }
    }

    /**
     * Commits one window's merged files as a single Iceberg snapshot and returns its volume
     * summary.
     */
    private CommitSummary commitOneWindow(Table table, WindowedCommit wc, @Nullable Integer pin) {
      WindowFiles files = reconstructFiles(table, wc);
      CommitSummary summary = CommitSummary.of(files);
      // The spec id to stamp this commit with.
      // If the run has no pinned spec yet, rePinAndWarnOnMismatch() pins this same value after
      // the commit.
      @Nullable Integer stampSpecId = pin != null ? pin : summary.firstSpecId;
      long commitStart = clock.getAsLong();
      applyCommit(table, wc.getWindowEndMs(), files, stampSpecId);
      metrics.commitDurationMs.update(clock.getAsLong() - commitStart);
      return summary;
    }

    /**
     * Establishes the run's spec pin on the first window committed under this runId. Returns the
     * current spec id pin.
     *
     * <p>Future calls will warn and count {@code specMismatchedWindows} when a committed window
     * mixes spec ids against the pin and carries any equality deletes. This can happen when a user
     * evolves a table's spec mid-run (bad practice).
     *
     * <p>This case is problematic because equality deletes apply only to data files of its own
     * {@code (spec id, partition)}, so a mixed-spec window means deletes may be intended for rows
     * written under another spec. In such a case, those rows will incorrectly remain live. The
     * window is already committed either way (reconstruction is per-file-spec). Deletes written
     * under an unpartitioned spec are global and do reach all rows.
     */
    private @Nullable Integer rePinAndWarnOnMismatch(
        String dest,
        WindowedCommit wc,
        CommitSummary summary,
        @Nullable Integer currentSpecId,
        ValueState<Integer> pinnedSpecId,
        ValueState<String> pinnedRunId) {
      if (currentSpecId == null) {
        currentSpecId = summary.firstSpecId;
        if (currentSpecId == null) {
          return null; // a window with no files cannot seed a pin
        }
        pinnedSpecId.write(currentSpecId);
        pinnedRunId.write(runId);
      }
      if (!summary.hasEqualityDeletes) {
        return currentSpecId;
      }
      for (int specId : summary.specIds) {
        if (specId != currentSpecId) {
          metrics.specMismatchedWindows.inc();
          LOG.warn(
              "CDC sink '{}' committed window-end {} ms for table '{}' with equality deletes in "
                  + "a window mixing partition specs (pinned spec id {}, saw spec id {}): the "
                  + "partition spec evolved mid-run. An equality delete applies only to data "
                  + "files of its own (spec id, partition), so deletes may not reach rows "
                  + "written under the other spec. Run rewrite_data_files so that data is correctly "
                  + "repartitioned, then drain and restart the pipeline to converge on one spec.",
              sinkId,
              wc.getWindowEndMs(),
              dest,
              currentSpecId,
              specId);
          break;
        }
      }
      return currentSpecId;
    }

    /**
     * Pairs a just-committed window with the snapshot carrying its token, records its metrics, and
     * returns its {@link SnapshotInfo}. We match by token because there could be concurrent foreign
     * writers that overwrite {@code currentSnapshot()}.
     */
    private SnapshotInfo identifyCommitted(
        Table table, String dest, WindowedCommit wc, CommitSummary summary) {
      Snapshot snapshot =
          token.findRecentlyCommittedTokenSnapshot(table, dest, wc.getWindowEndMs());
      recordCommitMetrics(dest, wc.getWindowEndMs(), summary, snapshot);
      return SnapshotInfo.fromSnapshot(snapshot);
    }

    /**
     * Reconstructs live {@link DataFile}/{@link DeleteFile} objects from one window's serialized
     * shard outputs. Each file is rebuilt against its own recorded partition-spec id (and
     * sort-order id) so a bundle written before a spec evolution still reconstructs under the spec
     * it was written with.
     */
    private static WindowFiles reconstructFiles(Table table, WindowedCommit wc) {
      Map<Integer, PartitionSpec> specs = table.specs();
      Map<Integer, SortOrder> sortOrders = sortOrdersForReconstruction(table);
      WindowFiles files = new WindowFiles();
      for (ShardDeltaFiles shard : wc.getFiles()) {
        for (SerializableDataFile dataFile : shard.getDataFiles()) {
          files.dataFiles.add(dataFile.createDataFile(specs));
        }
        for (SerializableDeleteFile deleteFile : shard.getDeleteFiles()) {
          files.deleteFiles.add(deleteFile.createDeleteFile(specs, sortOrders));
        }
        files.maxSequenceNumber = Math.max(files.maxSequenceNumber, shard.getMaxSequenceNumber());
      }
      return files;
    }

    /**
     * {@code table.sortOrders()}, guaranteed to contain the unsorted order (id {@code 0}). A table
     * created with a sort order stores only that order, without the unsorted id {@code 0}.
     */
    private static Map<Integer, SortOrder> sortOrdersForReconstruction(Table table) {
      Map<Integer, SortOrder> sortOrders = table.sortOrders();
      int unsortedId = SortOrder.unsorted().orderId();
      if (sortOrders.containsKey(unsortedId)) {
        return sortOrders;
      }
      Map<Integer, SortOrder> withUnsorted = new HashMap<>(sortOrders);
      withUnsorted.put(unsortedId, SortOrder.unsorted());
      return withUnsorted;
    }

    /**
     * Builds and commits one window's Iceberg operation: {@link AppendFiles} when the window has no
     * delete files, else {@link RowDelta}. Windows commit in strict ascending order, so this
     * window's equality deletes apply to all lower-sequence-number data.
     */
    private void applyCommit(
        Table table, long windowEndMs, WindowFiles files, @Nullable Integer stampSpecId) {
      Runnable hook = preCommitHookForTest;
      if (hook != null) {
        hook.run();
      }
      SnapshotUpdate<?> op;
      if (files.deleteFiles.isEmpty()) {
        AppendFiles append = table.newAppend(); // append fast path: the window has no deletes
        files.dataFiles.forEach(append::appendFile);
        op = append;
      } else {
        RowDelta rowDelta = table.newRowDelta();
        files.dataFiles.forEach(rowDelta::addRows);
        files.deleteFiles.forEach(rowDelta::addDeletes);
        op = rowDelta;
      }
      // User snapshot-summary properties first
      snapshotProperties.forEach(op::set);
      token.writeTo(op, windowEndMs, files.maxSequenceNumber, stampSpecId);
      // Parallelize manifest scanning on Iceberg's process-global worker pool.
      op.scanManifestsWith(ThreadPools.getWorkerPool());
      op.commit();
    }

    /** Records a successful commit's volume/latency/liveness metrics, from data already in hand. */
    private void recordCommitMetrics(
        String dest, long windowEndMs, CommitSummary summary, Snapshot snapshot) {
      metrics.committedDataFiles.inc(summary.dataFileCount);
      metrics.committedDeleteFiles.inc(summary.deleteFileCount);
      metrics.committedRecords.inc(summary.dataRecords);
      metrics.committedEqualityDeleteRecords.inc(summary.equalityDeleteRecords);
      metrics.committedBytes.inc(summary.bytes);
      metrics.snapshotsCreated.inc();
      LOG.info(
          "CDC sink '{}' committed window-end {} ms for table '{}' as snapshot {}.",
          sinkId,
          windowEndMs,
          dest,
          snapshot.snapshotId());
    }

    /**
     * Wraps a window's commit failure with operator-triage context; {@code committed} is already
     * advanced past this fire's earlier commits, so the pending numbers describe the retry.
     */
    private RuntimeException commitFailure(
        String dest,
        WindowedCommit failing,
        List<WindowedCommit> all,
        long committed,
        RuntimeException cause) {
      long earliestPending = earliestUncommitted(all, committed);
      int pendingCount = countUncommitted(all, committed);
      String message =
          "CDC sink '"
              + sinkId
              + "' failed to commit table '"
              + dest
              + "' at window-end "
              + failing.getWindowEndMs()
              + " ms (earliest pending window-end "
              + earliestPending
              + " ms, "
              + pendingCount
              + " pending window(s)). The destination is halted until this commit succeeds; "
              + "see commitFailures. The pending bag is "
              + "untouched, so the retry re-fires with the same windows and the same files. "
              + "Windows committed earlier in this same fire stay committed with their tokens "
              + "durable in the table, so the retry recovers those tokens and skips them as "
              + "already-committed (counted by alreadyCommittedWindowsSkipped).";
      return new RuntimeException(message, cause);
    }

    /**
     * Skips a window if the committed-through token already covers it. This only proves a window
     * with this end has already committed. It does not prove that the same content was committed.
     * For that reason, we log every file name and count it as a potential orphan.
     */
    private void skipAlreadyCommitted(String dest, WindowedCommit wc, long committed) {
      metrics.alreadyCommittedWindowsSkipped.inc();
      metrics.orphanFiles.inc(filePaths(wc).size());

      LOG.warn(
          "CDC sink '{}' skipping window-end {} ms for table '{}': already committed "
              + "(committed-through token = {} ms). The files below were written by this attempt. "
              + "If a different attempt committed this window, they are unreferenced and hold rows "
              + "the table never received. Files: {}",
          sinkId,
          wc.getWindowEndMs(),
          dest,
          committed,
          describeSkippedFiles(wc));
    }

    /**
     * The idempotent skip of a duplicate window: this very fire already published a window with the
     * identical end, so the duplicate's rows are in the table by construction.
     *
     * <p>Compares file paths to count and name orphans: a redelivered duplicate carries the
     * identical files (live table data, nothing orphaned), a second pane of the same window carries
     * distinct files (genuine orphans).
     */
    private void skipSameFireDuplicate(String dest, WindowedCommit dup, WindowedCommit committed) {
      metrics.alreadyCommittedWindowsSkipped.inc();
      Set<String> committedPaths = new HashSet<>(filePaths(committed));
      List<String> orphaned = new ArrayList<>();
      for (String path : filePaths(dup)) {
        if (!committedPaths.contains(path)) {
          orphaned.add(path);
        }
      }
      metrics.orphanFiles.inc(orphaned.size());
      if (orphaned.isEmpty()) {
        LOG.info(
            "CDC sink '{}' skipping window-end {} ms for table '{}': this commit fire already "
                + "published a window with the same end. Every "
                + "file this entry names was published by that commit, so it is a pure "
                + "redelivery and nothing is orphaned.",
            sinkId,
            committed.getWindowEndMs(),
            dest);
      } else {
        LOG.warn(
            "CDC sink '{}' skipping window-end {} ms for table '{}': this commit fire already "
                + "published a window with the same end. {} of this "
                + "entry's files are not among the ones that commit published. The sink cannot prove whether "
                + "they are redundant copies or hold rows the table never received. Files: {}",
            sinkId,
            committed.getWindowEndMs(),
            dest,
            orphaned.size(),
            describePaths(orphaned));
      }
    }

    /**
     * Flags a committed window whose min source sequence is below an earlier window's committed
     * max, a possible ordering violation. Returns the updated running max.
     */
    private long detectSequenceInversion(String dest, WindowedCommit wc, long prevMax) {
      long windowMinSeq = Long.MAX_VALUE;
      long windowMaxSeq = Long.MIN_VALUE;
      for (ShardDeltaFiles shard : wc.getFiles()) {
        windowMinSeq = Math.min(windowMinSeq, shard.getMinSequenceNumber());
        windowMaxSeq = Math.max(windowMaxSeq, shard.getMaxSequenceNumber());
      }
      if (windowMinSeq < prevMax) {
        metrics.crossWindowSequenceInversions.inc();
        LOG.warn(
            "CDC sink '{}' committed window-end {} ms for table '{}' with min sequence {} below "
                + "the previously committed max sequence {}: possible ordering violation (source "
                + "event-time not monotonic with the sequence number); final table state for keys "
                + "spanning these windows may be incorrect; this check has benign false positives "
                + "when the windows touch disjoint keys.",
            sinkId,
            wc.getWindowEndMs(),
            dest,
            windowMinSeq,
            prevMax);
      }
      return Math.max(prevMax, windowMaxSeq);
    }

    /** Records the earliest uncommitted pending window and resets the commit timer at it. */
    private void setCommitTimer(
        long earliest, ValueState<Long> earliestPending, Timer commitTimer) {
      earliestPending.write(earliest);
      if (earliest != Long.MAX_VALUE) {
        commitTimer.set(new Instant(earliest));
      }
    }

    /** Sets the processing-time heartbeat timer {@code heartbeatMillis} from now. */
    private void setHeartbeat(Timer heartbeatTimer) {
      if (heartbeatMillis > 0) {
        heartbeatTimer.offset(Duration.millis(heartbeatMillis)).setRelative();
      }
    }

    /**
     * Idle token-refresh heartbeat: when a destination has committed, has nothing pending, and its
     * newest token snapshot has aged past the interval, emit an empty append re-writing the current
     * token. The purpose is to always maintain a recent token-bearing snapshot that is young enough
     * to survive {@code expire_snapshots}.
     */
    @OnTimer("heartbeat")
    public void onHeartbeat(
        @Key String dest,
        @StateId("lastCommittedEndMs") ValueState<Long> lastCommittedEndMs,
        @StateId("lastCommittedMaxSeq") ValueState<Long> lastMaxSeq,
        @StateId("pending") BagState<WindowedCommit> pending,
        @StateId("pinnedSpecId") ValueState<Integer> pinnedSpecId,
        @StateId("pinnedRunId") ValueState<String> pinnedRunId,
        @TimerId("heartbeat") Timer heartbeatTimer) {
      try {
        Long lastCommitted = lastCommittedEndMs.read();
        if (lastCommitted == null || lastCommitted == Long.MIN_VALUE) {
          return; // never committed for this destination; no token to refresh
        }
        if (!pending.isEmpty().read()) {
          return; // a real commit is imminent; the token will refresh naturally
        }

        Table table = TableCache.getRefreshed(catalogConfig, dest);
        if (!token.shouldHeartbeat(table, heartbeatMillis, clock.getAsLong())) {
          return;
        }
        AppendFiles append = table.newAppend(); // empty append: a token refresh, no new files
        snapshotProperties.forEach(append::set);
        @Nullable Integer pin = runId.equals(pinnedRunId.read()) ? pinnedSpecId.read() : null;
        long maxCommittedSeq = firstNonNull(lastMaxSeq.read(), Long.MIN_VALUE);
        token.writeHeartbeatTo(append, lastCommitted, maxCommittedSeq, pin);
        append.commit();

        metrics.heartbeatCommits.inc();
        LOG.info(
            "CDC sink '{}' emitted an idle token-refresh (heartbeat) commit for table '{}' at "
                + "committed-through {} ms.",
            sinkId,
            dest,
            lastCommitted);
      } finally {
        setHeartbeat(heartbeatTimer); // keep firing through idle
      }
    }

    /** The earliest window-end strictly greater than {@code committed} (else {@code MIN}). */
    private static long earliestUncommitted(List<WindowedCommit> windows, long committed) {
      long min = Long.MAX_VALUE;
      for (WindowedCommit wc : windows) {
        if (wc.getWindowEndMs() > committed) {
          min = Math.min(min, wc.getWindowEndMs());
        }
      }
      return min == Long.MAX_VALUE ? Long.MIN_VALUE : min;
    }

    /** Count of windows whose end is strictly greater than {@code committed}. */
    private static int countUncommitted(List<WindowedCommit> windows, long committed) {
      int count = 0;
      for (WindowedCommit wc : windows) {
        if (wc.getWindowEndMs() > committed) {
          count++;
        }
      }
      return count;
    }

    /** One window's reconstructed files plus the max source sequence they cover. */
    private static final class WindowFiles {
      final List<DataFile> dataFiles = new ArrayList<>();
      final List<DeleteFile> deleteFiles = new ArrayList<>();
      long maxSequenceNumber = Long.MIN_VALUE;
    }

    private static final class CommitSummary {
      final long dataFileCount;
      final long deleteFileCount;
      final long dataRecords;
      final long equalityDeleteRecords;
      final long bytes;
      final @Nullable Integer firstSpecId;
      final Set<Integer> specIds;
      final boolean hasEqualityDeletes;

      private CommitSummary(
          long dataFileCount,
          long deleteFileCount,
          long dataRecords,
          long equalityDeleteRecords,
          long bytes,
          @Nullable Integer firstSpecId,
          Set<Integer> specIds,
          boolean hasEqualityDeletes) {
        this.dataFileCount = dataFileCount;
        this.deleteFileCount = deleteFileCount;
        this.dataRecords = dataRecords;
        this.equalityDeleteRecords = equalityDeleteRecords;
        this.bytes = bytes;
        this.firstSpecId = firstSpecId;
        this.specIds = specIds;
        this.hasEqualityDeletes = hasEqualityDeletes;
      }

      static CommitSummary of(WindowFiles files) {
        long dataRecords = 0;
        long bytes = 0;
        @Nullable Integer firstSpecId = null;
        Set<Integer> specIds = new HashSet<>();
        for (DataFile f : files.dataFiles) {
          dataRecords += f.recordCount();
          bytes += f.fileSizeInBytes();
          firstSpecId = firstSpecId == null ? f.specId() : firstSpecId;
          specIds.add(f.specId());
        }
        long equalityDeleteRecords = 0;
        boolean hasEqualityDeletes = false;
        for (DeleteFile f : files.deleteFiles) {
          bytes += f.fileSizeInBytes();
          firstSpecId = firstSpecId == null ? f.specId() : firstSpecId;
          specIds.add(f.specId());
          if (f.content() == FileContent.EQUALITY_DELETES) {
            equalityDeleteRecords += f.recordCount();
            hasEqualityDeletes = true;
          }
        }
        return new CommitSummary(
            files.dataFiles.size(),
            files.deleteFiles.size(),
            dataRecords,
            equalityDeleteRecords,
            bytes,
            firstSpecId,
            specIds,
            hasEqualityDeletes);
      }
    }
  }
}
