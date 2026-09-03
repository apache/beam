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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.util.SnapshotUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CDC sink's idempotency-token contract: the sink-id-namespaced snapshot-summary keys that make
 * commits idempotent, plus every token-keyed ancestry walk the committer performs.
 */
final class CommitToken implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CommitToken.class);

  /** Marks a snapshot as committed by the CDC sink instance named by its value. */
  static final String SINK_ID_KEY = "beam.cdc.sink-id";

  /** Prefix of the committed-through window-end token key ({@code + sinkId}). */
  static final String COMMITTED_THROUGH_MS_PREFIX = "beam.cdc.committed-through-ms.";

  /** Prefix of the max-committed source-sequence key ({@code + sinkId}). */
  static final String MAX_COMMITTED_SEQ_PREFIX = "beam.cdc.max-committed-seq.";

  /** Prefix of the run-spec stamp key ({@code + sinkId}); value {@code <runId>:<specId>}. */
  static final String RUN_SPEC_PREFIX = "beam.cdc.run-spec.";

  private final String sinkId;
  private final String runId;
  private final Counter tokenParseFailures;
  private final Counter suspectedTokenExpiry;

  /**
   * @param runId the run runId stamped into the run-spec key
   * @param tokenParseFailures counts unparseable token/max-seq summary values met during recovery
   * @param suspectedTokenExpiry counts recoveries where the sink-id marker survives but no token
   *     does (the token-bearing snapshots were likely expired away)
   */
  CommitToken(
      String sinkId, String runId, Counter tokenParseFailures, Counter suspectedTokenExpiry) {
    this.sinkId = sinkId;
    this.runId = runId;
    this.tokenParseFailures = tokenParseFailures;
    this.suspectedTokenExpiry = suspectedTokenExpiry;
  }

  /** Writes the three token keys and the {@code pinnedSpecId} onto a pending snapshot operation. */
  void writeTo(
      SnapshotUpdate<?> op,
      long committedThroughMs,
      long maxCommittedSeq,
      @Nullable Integer pinnedSpecId) {
    op.set(COMMITTED_THROUGH_MS_PREFIX + sinkId, Long.toString(committedThroughMs));
    op.set(MAX_COMMITTED_SEQ_PREFIX + sinkId, Long.toString(maxCommittedSeq));
    if (pinnedSpecId != null) {
      op.set(RUN_SPEC_PREFIX + sinkId, runId + ":" + pinnedSpecId);
    }
    op.set(SINK_ID_KEY, sinkId);
  }

  /**
   * Writes the token keys for an idle token-refresh (heartbeat) commit. Unlike {@link #writeTo},
   * the max-committed-seq key is omitted when unknown ({@code MIN}, meaning recovery found a token
   * whose snapshot carried no parseable max-seq).
   */
  void writeHeartbeatTo(
      SnapshotUpdate<?> op,
      long committedThroughMs,
      long maxCommittedSeq,
      @Nullable Integer pinnedSpecId) {
    op.set(COMMITTED_THROUGH_MS_PREFIX + sinkId, Long.toString(committedThroughMs));
    if (maxCommittedSeq != Long.MIN_VALUE) {
      op.set(MAX_COMMITTED_SEQ_PREFIX + sinkId, Long.toString(maxCommittedSeq));
    }
    if (pinnedSpecId != null) {
      op.set(RUN_SPEC_PREFIX + sinkId, runId + ":" + pinnedSpecId);
    }
    op.set(SINK_ID_KEY, sinkId);
  }

  /**
   * Returns the spec id stamped for {@code sinkId} under {@code runId}, read from the most recent
   * stamp-bearing snapshot on {@code table}'s current branch. {@code null} when that stamp is
   * absent, unparseable, or another run's (the caller falls back to the current spec).
   */
  static @Nullable Integer readRunSpec(Table table, String sinkId, String runId) {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return null;
    }
    String key = RUN_SPEC_PREFIX + sinkId;
    String wantedPrefix = runId + ":";
    for (Snapshot s : SnapshotUtil.ancestorsOf(current.snapshotId(), table::snapshot)) {
      Map<String, String> summary = s.summary();
      if (summary == null) {
        continue;
      }
      String value = summary.get(key);
      if (value == null) {
        continue;
      }
      // Only the newest stamp counts; a foreign runId or garbage value reads as no stamp.
      if (!value.startsWith(wantedPrefix)) {
        return null;
      }
      try {
        return Integer.parseInt(value.substring(wantedPrefix.length()));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  /**
   * The state recovered from a table's ancestry: the committed-through-ms window token and the
   * max-committed sequence from the same snapshot. The sequence seeds the cross-window inversion
   * detector when committer state is empty, which is a relaunch or the first time a destination is
   * seen. {@link #FRESH_START} means neither was found.
   */
  static final class Recovered {
    static final Recovered FRESH_START = new Recovered(Long.MIN_VALUE, Long.MIN_VALUE);

    final long committedThroughMs;
    final long maxCommittedSeq;

    private Recovered(long committedThroughMs, long maxCommittedSeq) {
      this.committedThroughMs = committedThroughMs;
      this.maxCommittedSeq = maxCommittedSeq;
    }
  }

  /**
   * Loads {@code dest} (forcing a refresh) and recovers this sink's token from its ancestry. The
   * table may not exist yet, so a missing table is tolerated as a fresh start.
   */
  Recovered recoverFromTable(IcebergCatalogConfig catalogConfig, String dest) {
    Table table;
    try {
      table = TableCache.getRefreshed(catalogConfig, dest);
    } catch (RuntimeException e) {
      if (hasCause(e, NoSuchTableException.class)) {
        return Recovered.FRESH_START;
      }
      throw e;
    }
    return recoverFrom(table, dest);
  }

  /**
   * Recovers this sink's committed-through-ms token (and corresponding max-committed sequence) by
   * scanning a table's snapshot ancestry and returning the first {@code
   * beam.cdc.committed-through-ms.<sinkId>} found, else {@link Long#MIN_VALUE}.
   */
  Recovered recoverFrom(Table table, String dest) {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return Recovered.FRESH_START;
    }
    String tokenKey = COMMITTED_THROUGH_MS_PREFIX + sinkId;
    String maxSeqKey = MAX_COMMITTED_SEQ_PREFIX + sinkId;
    boolean sawSinkMarker = false;
    for (Snapshot s : SnapshotUtil.ancestorsOf(current.snapshotId(), table::snapshot)) {
      Map<String, String> summary = s.summary();
      if (summary == null) {
        continue;
      }
      if (sinkId.equals(summary.get(SINK_ID_KEY))) {
        sawSinkMarker = true;
      }
      String tokenValue = summary.get(tokenKey);
      if (tokenValue == null) {
        continue;
      }
      long committedThroughMs;
      try {
        committedThroughMs = Long.parseLong(tokenValue);
      } catch (NumberFormatException e) {
        // An older intact token is better than crash-looping
        tokenParseFailures.inc();
        LOG.error(
            "CDC sink '{}' found an unparseable committed-through token '{}' in snapshot {} "
                + "of table '{}'; ignoring it and scanning older ancestors.",
            sinkId,
            tokenValue,
            s.snapshotId(),
            dest);
        continue;
      }
      // Both values come from this snapshot: the pair must describe one commit.
      return new Recovered(committedThroughMs, parseMaxSeq(summary.get(maxSeqKey), s, dest));
    }
    if (sawSinkMarker) {
      // This sink has committed to the table before, yet no token survived the ancestry scan.
      // Rare but can happen if expire_snapshots removes the token-bearing snapshots.
      suspectedTokenExpiry.inc();
      LOG.warn(
          "CDC sink '{}' found its sink-id marker in table '{}' ancestry but no "
              + "committed-through token; the token-bearing snapshot(s) may have been expired. "
              + "Falling back to MIN, which may replay retained windows.",
          sinkId,
          dest);
    }
    return Recovered.FRESH_START;
  }

  /** {@link Long#MIN_VALUE} when the max-committed-seq is absent or unparseable. */
  private long parseMaxSeq(@Nullable String value, Snapshot s, String dest) {
    if (value == null) {
      return Long.MIN_VALUE;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      tokenParseFailures.inc();
      LOG.error(
          "CDC sink '{}' found an unparseable max-committed-seq '{}' in snapshot {} of "
              + "table '{}'; ignoring it.",
          sinkId,
          value,
          s.snapshotId(),
          dest);
      return Long.MIN_VALUE;
    }
  }

  /**
   * Whether an idle destination should emit an empty token-refresh (heartbeat) commit: {@code true}
   * iff the most recent table snapshot bearing this sink's committed-through token is older than
   * {@code intervalMillis} relative to {@code nowMs}.
   */
  boolean shouldHeartbeat(Table table, long intervalMillis, long nowMs) {
    @Nullable Snapshot current = table.currentSnapshot();
    if (current == null) {
      return false;
    }
    String tokenKey = COMMITTED_THROUGH_MS_PREFIX + sinkId;
    for (Snapshot s : SnapshotUtil.ancestorsOf(current.snapshotId(), table::snapshot)) {
      Map<String, String> summary = s.summary();
      if (summary != null && summary.get(tokenKey) != null) {
        return s.timestampMillis() < nowMs - intervalMillis;
      }
    }
    return false;
  }

  /**
   * Finds and returns the snapshot corresponding to a just-committed window by looking for the
   * specified {@code windowEndMs}. Expects that the caller has just committed the window, so throws
   * if no such snapshot exists.
   */
  Snapshot findRecentlyCommittedTokenSnapshot(Table table, String dest, long windowEndMs) {
    table.refresh();
    Snapshot current =
        checkStateNotNull(
            table.currentSnapshot(),
            "table '%s' has no current snapshot right after a commit",
            dest);
    String tokenKey = COMMITTED_THROUGH_MS_PREFIX + sinkId;
    String wanted = Long.toString(windowEndMs);
    for (Snapshot s : SnapshotUtil.ancestorsOf(current.snapshotId(), table::snapshot)) {
      Map<String, String> summary = s.summary();
      if (summary != null && wanted.equals(summary.get(tokenKey))) {
        return s;
      }
    }
    throw new IllegalStateException(
        "CDC sink '"
            + sinkId
            + "' committed window-end "
            + windowEndMs
            + " ms to table '"
            + dest
            + "' but found no snapshot carrying its committed-through token in the refreshed "
            + "ancestry.");
  }

  private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }
}
