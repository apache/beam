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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a batch of {@link ExecutedGroup}s sharing a commit key (assigned in {@link
 * PlanRewriteGroups}) and performs one atomic commit: old data files (plus any dangling,
 * file-scoped deletion vectors) are deleted and the newly written, compacted files are added.
 * Correctness properties:
 *
 * <ul>
 *   <li><b>Conflict validation</b>: {@link RewriteFiles#validateFromSnapshot(long)} pins the commit
 *       to the planning snapshot, so a conflicting concurrent change to the rewritten files fails
 *       the commit.
 *   <li><b>Sequence-number preservation</b>: with {@link
 *       RewriteDataFiles.Configuration#useStartingSequenceNumber} the rewritten files keep the
 *       starting snapshot's data sequence number, so delete files added since planning still apply
 *       to the rewritten data.
 *   <li><b>Idempotency</b>: each commit stamps the snapshot summary with {@code operationId} and
 *       {@code commitKey}; a retried bundle re-emits the stamped snapshot instead of committing
 *       twice.
 *   <li><b>Failure handling</b>: a {@link CommitStateUnknownException} is rethrown WITHOUT deleting
 *       files (the commit may have succeeded server-side). A terminal failure likewise leaves the
 *       outputs in place, as operation-id-tagged orphans for a later remove-orphan-files run;
 *       partial progress charges the commit-failure budget, atomic mode fails the pipeline.
 * </ul>
 */
class CommitRewriteGroups extends DoFn<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(CommitRewriteGroups.class);

  static final String OP_ID_PROP = "beam.rewrite.operation-id";
  static final String COMMIT_KEY_PROP = "beam.rewrite.commit-key";

  private static final int MAX_COMMIT_ATTEMPTS = 4;

  static final TupleTag<SnapshotInfo> COMMITTED = new TupleTag<>() {};
  /** Failed commit keys, counted downstream and compared against {@code maxFailedCommits}. */
  static final TupleTag<Integer> FAILED_COMMITS = new TupleTag<>() {};
  /** One {@link RewriteResult} commit fragment per processed key. */
  static final TupleTag<RewriteResult> COMMIT_SUMMARY = new TupleTag<>() {};

  private static final Counter numFilesCommitted =
      Metrics.counter(CommitRewriteGroups.class, "numFilesCommitted");
  private static final Counter numFilesRemoved =
      Metrics.counter(CommitRewriteGroups.class, "numFilesRemoved");
  private static final Counter numCommitConflicts =
      Metrics.counter(CommitRewriteGroups.class, "numCommitConflicts");
  private static final Counter numFailedCommits =
      Metrics.counter(CommitRewriteGroups.class, "numFailedCommits");

  private final String tableIdentifier;
  private final IcebergCatalogConfig catalogConfig;
  private final RewriteDataFiles.Configuration config;

  CommitRewriteGroups(
      String tableIdentifier,
      IcebergCatalogConfig catalogConfig,
      RewriteDataFiles.Configuration config) {
    this.tableIdentifier = tableIdentifier;
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  @ProcessElement
  public void process(
      @Element KV<Integer, Iterable<ExecutedGroup>> element, MultiOutputReceiver out)
      throws InterruptedException {
    int commitKey = element.getKey();
    List<ExecutedGroup> groups = ImmutableList.copyOf(element.getValue());
    if (groups.isEmpty()) {
      return;
    }

    String operationId = groups.get(0).getOperationId();
    long startingSnapshotId = groups.get(0).getStartingSnapshotId();
    long startingSequenceNumber = groups.get(0).getStartingSequenceNumber();

    groups = completeParentSubgroups(groups);
    if (groups.isEmpty()) {
      // Nothing to commit
      return;
    }

    Table table = TableCache.getRefreshed(catalogConfig, tableIdentifier);

    // (1) Idempotency: did a prior bundle attempt already commit this (operationId, commitKey)?
    @Nullable
    Snapshot alreadyCommitted =
        findCommittedSnapshot(table, commitKey, operationId, startingSequenceNumber);
    if (alreadyCommitted != null) {
      LOG.info(
          RewriteDataFiles.REWRITE_PREFIX
              + "Commit key {} already committed as snapshot (id={}, seq={}); skipping.",
          commitKey,
          alreadyCommitted.snapshotId(),
          alreadyCommitted.sequenceNumber());
      out.get(COMMITTED).output(SnapshotInfo.fromSnapshot(alreadyCommitted));
      out.get(COMMIT_SUMMARY)
          .output(idempotentCommitFragment(alreadyCommitted, rewrittenBytes(groups)));
      return;
    }

    // (2) Commit with bounded retry + revalidation.
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.standardSeconds(2))
            .withMaxRetries(MAX_COMMIT_ATTEMPTS)
            .withMaxBackoff(Duration.standardSeconds(10))
            .backoff();
    for (int attempt = 1; ; attempt++) {
      try {
        Snapshot snap = commitOnce(table, groups, startingSnapshotId, commitKey, operationId);
        out.get(COMMITTED).output(SnapshotInfo.fromSnapshot(snap));
        out.get(COMMIT_SUMMARY)
            .output(
                RewriteResult.builder()
                    .setCommittedSnapshots(1)
                    .setFilesAdded(snapshotSummaryCount(snap, SnapshotSummary.ADDED_FILES_PROP))
                    .setFilesRemoved(snapshotSummaryCount(snap, SnapshotSummary.DELETED_FILES_PROP))
                    .setRewrittenBytes(rewrittenBytes(groups))
                    .build());
        return;
      } catch (CommitStateUnknownException e) {
        // May have succeeded server-side: do NOT clean up files. Rethrow so the bundle retries;
        // the idempotency check above detects a commit that did land.
        throw e;
      } catch (RuntimeException e) {
        if (!(e instanceof CleanableFailure)) {
          // Not cleanable: leave the files in place and let the bundle retry.
          throw e;
        }
        // Cleanable failure (typically a commit conflict): retry, then account for it below.
        numCommitConflicts.inc();
        if (attempt < MAX_COMMIT_ATTEMPTS) {
          LOG.warn(
              RewriteDataFiles.REWRITE_PREFIX
                  + "Commit failed for key {} (attempt {}/{}), retrying",
              commitKey,
              attempt,
              MAX_COMMIT_ATTEMPTS,
              e);
          // Spread out retries: many commit keys can contend on the table's metadata pointer.
          BackOffUtils.next(Sleeper.DEFAULT, backoff);
          table.refresh();
          continue;
        }

        // No attempts left: a concurrent attempt for the same stamp may already have landed.
        table.refresh();
        @Nullable
        Snapshot landed =
            findCommittedSnapshot(table, commitKey, operationId, startingSequenceNumber);
        if (landed != null) {
          out.get(COMMITTED).output(SnapshotInfo.fromSnapshot(landed));
          out.get(COMMIT_SUMMARY).output(idempotentCommitFragment(landed, rewrittenBytes(groups)));
          return;
        }

        // The commit did not land. Don't delete this batch's output files: a retried or zombie
        // attempt could still commit them, and a snapshot would then reference missing data.
        numFailedCommits.inc();
        List<String> orphans = ExecutedGroup.newFilePaths(groups);
        if (config.partialProgressEnabled()) {
          long failedBytes = rewrittenBytes(groups);
          LOG.warn(
              RewriteDataFiles.REWRITE_PREFIX
                  + "Commit for key {} failed after {} attempt(s), leaving {} input byte(s) "
                  + "un-compacted. {} orphan output file(s) left tagged with operation-id {} "
                  + "for a later remove-orphan-files run: {}",
              commitKey,
              MAX_COMMIT_ATTEMPTS,
              failedBytes,
              orphans.size(),
              operationId,
              orphans,
              e);
          out.get(FAILED_COMMITS).output(commitKey);
          out.get(COMMIT_SUMMARY).output(RewriteResult.builder().setFailedCommits(1).build());
          return;
        }
        // Atomic mode: fail the pipeline, again without deleting the outputs.
        LOG.error(
            RewriteDataFiles.REWRITE_PREFIX
                + "Atomic commit for key {} failed after {} attempt(s); failing the pipeline. {} "
                + "orphan output file(s) left tagged with operation-id {} for a later "
                + "remove-orphan-files run: {}",
            commitKey,
            MAX_COMMIT_ATTEMPTS,
            orphans.size(),
            operationId,
            orphans,
            e);
        throw new RuntimeException(commitConflictMessage(commitKey, orphans.size()), e);
      }
    }
  }

  /** Performs a single {@link RewriteFiles} commit and returns the resulting snapshot. */
  @VisibleForTesting
  Snapshot commitOnce(
      Table table,
      List<ExecutedGroup> groups,
      long startingSnapshotId,
      int commitKey,
      String operationId) {
    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(startingSnapshotId);
    @Nullable String branch = config.getBranch();
    if (branch != null) {
      // Write-audit-publish: commit to the branch, leaving main untouched.
      rewrite.toBranch(branch);
    }
    if (config.useStartingSequenceNumber()) {
      // Fail closed: without the starting snapshot we cannot preserve the rewritten files' data
      // sequence number, so late delete files could stop applying to them (MOR data resurrection).
      Snapshot start =
          checkStateNotNull(
              table.snapshot(startingSnapshotId),
              "Cannot preserve the starting sequence number: starting snapshot %s is no longer "
                  + "available (expired?). Set useStartingSequenceNumber=false only if the table has "
                  + "no delete files that must keep applying to the rewritten data.",
              startingSnapshotId);
      rewrite.dataSequenceNumber(start.sequenceNumber());
    }
    // Rebuild the file sets from each group's compact descriptors.
    DataFileSet dataFilesToDelete = DataFileSet.create();
    DeleteFileSet deleteFilesToDelete = DeleteFileSet.create();
    int added = 0;
    for (ExecutedGroup g : groups) {
      for (SerializableDataFile sdf : g.getRewrittenDataFiles()) {
        dataFilesToDelete.add(sdf.createDataFile(table.specs()));
      }
      for (String dvJson : g.getDanglingDeleteFileJsons()) {
        deleteFilesToDelete.add(
            (DeleteFile)
                JsonUtil.parse(dvJson, node -> ContentFileParser.fromJson(node, table.specs())));
      }
      for (SerializableDataFile sdf : g.getNewFiles()) {
        rewrite.addFile(sdf.createDataFile(table.specs()));
        added++;
      }
    }
    dataFilesToDelete.forEach(rewrite::deleteFile);
    deleteFilesToDelete.forEach(rewrite::deleteFile);
    int removed = dataFilesToDelete.size();

    config.snapshotProperties().forEach(rewrite::set);
    rewrite.set(OP_ID_PROP, operationId);
    rewrite.set(COMMIT_KEY_PROP, Integer.toString(commitKey));
    rewrite.commit();
    numFilesCommitted.inc(added);
    numFilesRemoved.inc(removed);
    // Locate OUR snapshot by its stamp rather than reading currentSnapshot(): a concurrent commit
    // may have advanced it.
    table.refresh();
    Snapshot snap =
        checkStateNotNull(
            findCommittedSnapshot(
                table, commitKey, operationId, groups.get(0).getStartingSequenceNumber()),
            "Commit for key %s landed but its stamped snapshot could not be located.",
            commitKey);
    LOG.info(
        RewriteDataFiles.REWRITE_PREFIX
            + "Committed key {} as snapshot(id={}, seq={}) (+{} / -{} files)",
        commitKey,
        snap.snapshotId(),
        snap.sequenceNumber(),
        added,
        removed);
    return snap;
  }

  /**
   * Scans ancestors of the current head for a snapshot already stamped with this {@code
   * operationId} and {@code commitKey}, i.e. one this bundle already committed.
   *
   * <p>The walk stops once a snapshot's sequence number drops to or below the STARTING snapshot's:
   * sequence numbers are monotonic along an ancestry, so nothing at or below that floor can carry
   * our stamp.
   */
  private @Nullable Snapshot findCommittedSnapshot(
      Table table, int commitKey, String operationId, long startingSequenceNumber) {
    long headSnapshotId;
    @Nullable String branch = config.getBranch();
    if (branch != null) {
      SnapshotRef ref = table.refs().get(branch);
      if (ref == null) {
        return null;
      }
      headSnapshotId = ref.snapshotId();
    } else {
      Snapshot current = table.currentSnapshot();
      if (current == null) {
        return null;
      }
      headSnapshotId = current.snapshotId();
    }
    String wantKey = Integer.toString(commitKey);
    for (Snapshot s : SnapshotUtil.ancestorsOf(headSnapshotId, table::snapshot)) {
      if (s.sequenceNumber() <= startingSequenceNumber) {
        break;
      }
      @Nullable Map<String, String> summary = s.summary();
      if (summary != null
          && operationId.equals(summary.get(OP_ID_PROP))
          && wantKey.equals(summary.get(COMMIT_KEY_PROP))) {
        return s;
      }
    }
    return null;
  }

  /**
   * Returns only subgroups whose parent was fully rewritten. Committing a partially rewritten
   * parent would delete its input files while missing a failed subgroup's rows.
   */
  private List<ExecutedGroup> completeParentSubgroups(List<ExecutedGroup> groups) {
    Map<Integer, List<ExecutedGroup>> byParent = new LinkedHashMap<>();
    for (ExecutedGroup g : groups) {
      byParent.computeIfAbsent(g.getParentGroupIndex(), k -> new ArrayList<>()).add(g);
    }
    List<ExecutedGroup> complete = new ArrayList<>();
    for (Map.Entry<Integer, List<ExecutedGroup>> e : byParent.entrySet()) {
      List<ExecutedGroup> subgroups = e.getValue();
      int expected = subgroups.get(0).getParentSubgroupCount();
      if (subgroups.size() >= expected) {
        complete.addAll(subgroups);
      } else {
        List<String> orphans = ExecutedGroup.newFilePaths(subgroups);
        LOG.warn(
            RewriteDataFiles.REWRITE_PREFIX
                + "Parent group {} is incomplete (rewrote only {} of {} subgroups); skipping it so its "
                + "input files are not partially replaced. {} orphan output file(s) left for a "
                + "later remove-orphan-files run: {}",
            e.getKey(),
            subgroups.size(),
            expected,
            orphans.size(),
            orphans);
      }
    }
    return complete;
  }

  /** Commit-summary fragment for a snapshot located by its idempotency stamp. */
  private static RewriteResult idempotentCommitFragment(Snapshot snapshot, long rewrittenBytes) {
    return RewriteResult.builder()
        .setCommittedSnapshots(1)
        .setFilesAdded(snapshotSummaryCount(snapshot, SnapshotSummary.ADDED_FILES_PROP))
        .setFilesRemoved(snapshotSummaryCount(snapshot, SnapshotSummary.DELETED_FILES_PROP))
        .setRewrittenBytes(rewrittenBytes)
        .build();
  }

  /** Null-safe read of a numeric snapshot-summary property (missing / non-numeric -> 0). */
  private static long snapshotSummaryCount(Snapshot snapshot, String key) {
    @Nullable Map<String, String> summary = snapshot.summary();
    @Nullable String value = summary == null ? null : summary.get(key);
    if (value == null) {
      return 0L;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  /** Guidance message for an atomic-mode commit that failed after exhausting its retries. */
  private static String commitConflictMessage(int commitKey, int orphanCount) {
    return String.format(
        "RewriteDataFiles commit for key %s conflicted with a concurrent Iceberg operation and "
            + "could not be committed after retries. %s orphan output file(s) were left on disk "
            + "(tagged with this operation's id) for a later remove-orphan-files run; the input "
            + "files were NOT deleted, so no data was lost. Consider enabling partial progress so "
            + "the rewrite is split into smaller commits, which makes the overall operation more "
            + "resilient to conflicts.",
        commitKey, orphanCount);
  }

  /** Total input bytes this batch attempted to rewrite, used for failed-commit accounting. */
  private static long rewrittenBytes(List<ExecutedGroup> groups) {
    long total = 0;
    for (ExecutedGroup g : groups) {
      total += g.getTotalInputByteSize();
    }
    return total;
  }
}
