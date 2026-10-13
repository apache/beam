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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CommitRewriteGroups}. */
@RunWith(JUnit4.class)
public class CommitRewriteGroupsTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private TableIdentifier tableId;

  /**
   * Creates an unpartitioned table with {@code numFiles} small data files (3 records each), records
   * its {@link TableIdentifier} in {@link #tableId} for later reloads, and returns the table.
   */
  private Table buildTable(int numFiles) throws Exception {
    tableId = TableIdentifier.of("default", "commit_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    AppendFiles append = table.newAppend();
    for (int i = 0; i < numFiles; i++) {
      DataFile df =
          warehouse.writeRecords(
              "f" + i + "_" + System.nanoTime() + ".parquet",
              table.schema(),
              TestFixtures.FILE1SNAPSHOT1);
      append.appendFile(df);
    }
    append.commit();
    table.refresh();
    return table;
  }

  /**
   * Builds an {@link IcebergCatalogConfig} pointed at the same Hadoop warehouse as the test's
   * table, so the DoFn's {@code catalog().loadTable(...)} resolves the SAME physical table. The
   * warehouse root is derived by stripping the namespace and table-name segments off the table's
   * own location, avoiding {@code TestDataWarehouse}'s protected {@code location} field.
   */
  private IcebergCatalogConfig catalogConfig(Table table) {
    String tableLocation = table.location(); // <warehouse>/<namespace>/<table>
    String namespace = tableId.namespace().toString();
    String tableName = tableId.name();
    String suffix = "/" + namespace + "/" + tableName;
    assertTrue("Unexpected table location: " + tableLocation, tableLocation.endsWith(suffix));
    String warehouse = tableLocation.substring(0, tableLocation.length() - suffix.length());
    return IcebergCatalogConfig.builder()
        .setCatalogName("hadoop")
        .setCatalogProperties(
            ImmutableMap.of(
                "type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, "warehouse", warehouse))
        .build();
  }

  /**
   * Plans + rewrites the table into real {@link ExecutedGroup}s by driving the production DoFns
   * through {@link DoFnTester}, then groups them into commit batches keyed by integer commit key.
   */
  private List<KV<Integer, Iterable<ExecutedGroup>>> planAndRewrite(
      Table table, RewriteDataFiles.Configuration config) throws Exception {
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    // 1. Plan groups via the production ScanAndPlan DoFn.
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    List<KV<Integer, RewriteSubGroup>> planned =
        planTester.peekOutputElements(PlanRewriteGroups.GROUPS);

    // 2. Rewrite each planned group via the production RewriteGroupDoFn.
    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> rewriteTester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    rewriteTester.processBundle(planned);
    List<KV<Integer, ExecutedGroup>> executed =
        rewriteTester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN);

    // 3. Group ExecutedGroups by commit key.
    Map<Integer, List<ExecutedGroup>> byKey = new java.util.LinkedHashMap<>();
    for (KV<Integer, ExecutedGroup> kv : executed) {
      byKey.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv.getValue());
    }
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = new ArrayList<>();
    for (Map.Entry<Integer, List<ExecutedGroup>> e : byKey.entrySet()) {
      batches.add(KV.of(e.getKey(), (Iterable<ExecutedGroup>) e.getValue()));
    }
    return batches;
  }

  private static long countRows(Table table) {
    long count = 0;
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record ignored : rows) {
        count++;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return count;
  }

  private static int snapshotCount(Table table) {
    int n = 0;
    for (Snapshot ignored : table.snapshots()) {
      n++;
    }
    return n;
  }

  private static long replaceSnapshots(Table table) {
    long count = 0;
    for (Snapshot s : table.snapshots()) {
      if (DataOperations.REPLACE.equals(s.operation())) {
        count++;
      }
    }
    return count;
  }

  /** Locations of every newly written output file across a commit batch. */
  private static List<String> newFilePaths(KV<Integer, Iterable<ExecutedGroup>> batch) {
    List<String> paths = new ArrayList<>();
    for (ExecutedGroup g : batch.getValue()) {
      for (SerializableDataFile sdf : g.getNewFiles()) {
        paths.add(sdf.getPath());
      }
    }
    return paths;
  }

  /** Locations of every INPUT data file being replaced across a commit batch. */
  private static Set<String> rewrittenInputPaths(KV<Integer, Iterable<ExecutedGroup>> batch) {
    Set<String> paths = new HashSet<>();
    for (ExecutedGroup g : batch.getValue()) {
      for (SerializableDataFile sdf : g.getRewrittenDataFiles()) {
        paths.add(sdf.getPath());
      }
    }
    return paths;
  }

  @Test
  public void atomicCommit_replacesFiles() throws Exception {
    Table table = buildTable(6);
    long recordsBefore = countRows(table);

    // Capture all old data-file paths before the rewrite.
    Set<String> oldPaths = new HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        oldPaths.add(t.file().location().toString());
      }
    }

    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertEquals("Expected a single commit batch", 1, batches.size());

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> commitTester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    commitTester.processBundle(batches.get(0));

    List<SnapshotInfo> committed = commitTester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals("Expected exactly one committed snapshot", 1, committed.size());

    // The fresh commit emits one RewriteResult fragment reporting adds/removes/bytes.
    List<RewriteResult> summaries =
        commitTester.peekOutputElements(CommitRewriteGroups.COMMIT_SUMMARY);
    assertEquals("exactly one commit fragment", 1, summaries.size());
    RewriteResult cs = summaries.get(0);
    assertEquals("one snapshot committed", 1L, cs.getCommittedSnapshots());
    assertEquals("no commit failure", 0L, cs.getFailedCommits());
    assertTrue("some compacted files added", cs.getFilesAdded() > 0);
    assertTrue("the rewritten inputs removed", cs.getFilesRemoved() > 0);
    assertTrue("fresh commit reports rewritten input bytes", cs.getRewrittenBytes() > 0);

    // Re-read the physical table.
    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    Snapshot snap = reloaded.currentSnapshot();

    // (a) operation is "replace"
    assertEquals(DataOperations.REPLACE, snap.operation());

    // (b) no old rewritten file path is present in the table after the rewrite
    Set<String> newPaths = new HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        newPaths.add(t.file().location().toString());
      }
    }
    for (String old : oldPaths) {
      assertFalse("Old rewritten file should no longer be present: " + old, newPaths.contains(old));
    }
    assertTrue("Rewrite should produce fewer files", newPaths.size() < oldPaths.size());

    // (c) total record count is preserved
    assertEquals("Record count must be preserved", recordsBefore, countRows(reloaded));
  }

  @Test
  public void idempotentRetry() throws Exception {
    Table table = buildTable(6);
    long recordsBefore = countRows(table);

    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertEquals(1, batches.size());
    KV<Integer, Iterable<ExecutedGroup>> batch = batches.get(0);
    // Materialize the batch so it can be replayed against two testers.
    List<ExecutedGroup> groupList = ImmutableList.copyOf(batch.getValue());
    KV<Integer, Iterable<ExecutedGroup>> replayable =
        KV.of(batch.getKey(), (Iterable<ExecutedGroup>) groupList);

    int snapsBefore = snapshotCount(table);

    // First commit.
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> firstTester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    firstTester.processBundle(replayable);
    List<SnapshotInfo> firstCommitted =
        firstTester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals(1, firstCommitted.size());
    long committedSnapshotId = firstCommitted.get(0).getSnapshotId();

    Table afterFirst = warehouse.loadTable(tableId);
    afterFirst.refresh();
    int snapsAfterFirst = snapshotCount(afterFirst);
    assertEquals("Exactly one new snapshot after first commit", snapsBefore + 1, snapsAfterFirst);

    // Second commit with a FRESH DoFn, same operationId + batch -> should be a no-op commit-wise.
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> secondTester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    secondTester.processBundle(replayable);
    List<SnapshotInfo> secondCommitted =
        secondTester.peekOutputElements(CommitRewriteGroups.COMMITTED);

    Table afterSecond = warehouse.loadTable(tableId);
    afterSecond.refresh();
    int snapsAfterSecond = snapshotCount(afterSecond);

    assertEquals(
        "Second invocation must NOT create another snapshot", snapsAfterFirst, snapsAfterSecond);
    assertEquals(
        "Second invocation must still emit the existing committed snapshot",
        1,
        secondCommitted.size());
    assertEquals(
        "Second invocation must emit the SAME snapshot id",
        committedSnapshotId,
        secondCommitted.get(0).getSnapshotId());

    // The idempotent re-emit still reports one committed snapshot, with adds/removes read from the
    // already-committed snapshot's summary. rewrittenBytes is a PER-RUN metric: this attempt
    // re-committed the batch via the stamp, so it credits the batch's input bytes.
    long batchBytes = 0L;
    for (ExecutedGroup g : groupList) {
      batchBytes += g.getTotalInputByteSize();
    }
    assertTrue("fixture must have real input bytes", batchBytes > 0);
    List<RewriteResult> secondSummaries =
        secondTester.peekOutputElements(CommitRewriteGroups.COMMIT_SUMMARY);
    assertEquals(1, secondSummaries.size());
    RewriteResult cs2 = secondSummaries.get(0);
    assertEquals(
        "idempotent re-emit reports one committed snapshot", 1L, cs2.getCommittedSnapshots());
    assertTrue("adds read from the committed snapshot summary", cs2.getFilesAdded() > 0);
    assertTrue("removes read from the committed snapshot summary", cs2.getFilesRemoved() > 0);
    assertEquals(
        "per-run metric — the replay credits the batch it re-emits",
        batchBytes,
        cs2.getRewrittenBytes());

    // Record count still preserved.
    assertEquals(recordsBefore, countRows(afterSecond));
  }

  @Test
  public void idempotentReplayFindsStampAfterStartingSnapshotExpired() throws Exception {
    // The idempotency stamp scan bounds its ancestor walk by the starting snapshot's SEQUENCE
    // NUMBER (carried from planning), not its id — so a replay must still find the stamped
    // snapshot even after a concurrent expire-snapshots removed the starting snapshot itself.
    Table table = buildTable(6);
    long recordsBefore = countRows(table);

    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertEquals(1, batches.size());
    KV<Integer, Iterable<ExecutedGroup>> batch = batches.get(0);
    List<ExecutedGroup> groupList = ImmutableList.copyOf(batch.getValue());
    KV<Integer, Iterable<ExecutedGroup>> replayable =
        KV.of(batch.getKey(), (Iterable<ExecutedGroup>) groupList);
    long startingSnapshotId = groupList.get(0).getStartingSnapshotId();

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> firstTester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    firstTester.processBundle(replayable);
    List<SnapshotInfo> firstCommitted =
        firstTester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals(1, firstCommitted.size());
    long committedSnapshotId = firstCommitted.get(0).getSnapshotId();

    // A concurrent expire-snapshots removes the STARTING snapshot during the retry window.
    Table reloaded = warehouse.loadTable(tableId);
    reloaded.expireSnapshots().expireSnapshotId(startingSnapshotId).commit();
    reloaded.refresh();
    assertNull(
        "precondition: the starting snapshot must be gone", reloaded.snapshot(startingSnapshotId));

    // A fresh replay must still find the stamp (sequence-number floor) instead of re-committing.
    int snapsAfterExpire = snapshotCount(reloaded);
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> secondTester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    secondTester.processBundle(replayable);
    List<SnapshotInfo> secondCommitted =
        secondTester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals("the replay must emit the already-committed snapshot", 1, secondCommitted.size());
    assertEquals(committedSnapshotId, secondCommitted.get(0).getSnapshotId());
    assertTrue(
        "no terminal commit failure may be reported",
        secondTester.peekOutputElements(CommitRewriteGroups.FAILED_COMMITS).isEmpty());

    Table afterReplay = warehouse.loadTable(tableId);
    afterReplay.refresh();
    assertEquals("the replay must NOT commit again", snapsAfterExpire, snapshotCount(afterReplay));
    assertEquals(recordsBefore, countRows(afterReplay));
  }

  // Late-delete sequence-number preservation, conflict detection, and deletion-vector cleanup are
  // covered end-to-end in RewriteDataFilesCorrectnessTest.

  /** A cleanable commit failure that is NOT a ValidationException/CommitFailedException. */
  private static class InjectedCleanableFailure extends RuntimeException
      implements CleanableFailure {
    InjectedCleanableFailure(String message) {
      super(message);
    }
  }

  /**
   * A {@link CommitRewriteGroups} whose commit always throws a chosen exception (fault injection).
   * {@link #calls} counts how many times {@code commitOnce} ran (visible only with {@code
   * DO_NOT_CLONE}).
   */
  private static class FailingCommit extends CommitRewriteGroups {
    private final RuntimeException toThrow;
    int calls = 0;

    FailingCommit(
        String tableIdentifier,
        IcebergCatalogConfig catalogConfig,
        RewriteDataFiles.Configuration config,
        RuntimeException toThrow) {
      super(tableIdentifier, catalogConfig, config);
      this.toThrow = toThrow;
    }

    @Override
    Snapshot commitOnce(
        Table table,
        List<ExecutedGroup> groups,
        long startingSnapshotId,
        int commitKey,
        String operationId) {
      calls++;
      throw toThrow;
    }
  }

  /**
   * A {@link CommitRewriteGroups} whose commit throws a real {@link CommitFailedException} on the
   * first {@code failFirstN} calls, then delegates to the real commit — exercises the retry loop in
   * the RECOVER direction.
   */
  private static class FlakyCommit extends CommitRewriteGroups {
    private final int failFirstN;
    int calls = 0;

    FlakyCommit(
        String tableIdentifier,
        IcebergCatalogConfig catalogConfig,
        RewriteDataFiles.Configuration config,
        int failFirstN) {
      super(tableIdentifier, catalogConfig, config);
      this.failFirstN = failFirstN;
    }

    @Override
    Snapshot commitOnce(
        Table table,
        List<ExecutedGroup> groups,
        long startingSnapshotId,
        int commitKey,
        String operationId) {
      if (++calls <= failFirstN) {
        throw new CommitFailedException("injected transient conflict");
      }
      return super.commitOnce(table, groups, startingSnapshotId, commitKey, operationId);
    }
  }

  /**
   * A {@link CommitRewriteGroups} whose commit LANDS (delegates to super, stamping the snapshot)
   * but then always throws, so the retry loop exhausts and the last-chance {@code
   * findCommittedSnapshot} recheck must locate the already-landed stamp instead of reporting
   * failure.
   */
  private static class LandsThenThrows extends CommitRewriteGroups {
    LandsThenThrows(
        String tableIdentifier,
        IcebergCatalogConfig catalogConfig,
        RewriteDataFiles.Configuration config) {
      super(tableIdentifier, catalogConfig, config);
    }

    @Override
    Snapshot commitOnce(
        Table table,
        List<ExecutedGroup> groups,
        long startingSnapshotId,
        int commitKey,
        String operationId) {
      Snapshot landed = super.commitOnce(table, groups, startingSnapshotId, commitKey, operationId);
      throw new CommitFailedException(
          "landed then injected failure (snapshot %s)", landed.snapshotId());
    }
  }

  @Test
  public void cleanableCommitFailureInAtomicModeThrowsWithoutDeleting() throws Exception {
    // Atomic mode: a terminal cleanable commit failure must FAIL the pipeline but must NOT delete
    // this batch's output files. A retried or concurrent (zombie) attempt of the same commit
    // element could still commit them, so deleting addable files risks a snapshot referencing
    // missing data. They are left as operationId-tagged orphans instead.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    List<String> outputs = newFilePaths(batches.get(0));
    assertFalse("sanity: the batch produced output files", outputs.isEmpty());

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(
            new FailingCommit(
                tableId.toString(),
                catalogConfig(table),
                config,
                new InjectedCleanableFailure("injected cleanable failure")));

    assertThrows(Exception.class, () -> tester.processBundle(batches.get(0)));

    assertTrue(
        "a cleanable failure must not commit",
        tester.peekOutputElements(CommitRewriteGroups.COMMITTED).isEmpty());
    Table after = warehouse.loadTable(tableId);
    for (String p : outputs) {
      assertTrue(
          "atomic terminal commit failure must NOT delete output file: " + p,
          after.io().newInputFile(p).exists());
    }
  }

  @Test
  public void partialProgressCommitFailureRoutedToFailedCommits() throws Exception {
    // Under partial progress a terminal cleanable commit failure is TOLERATED: the batch is routed
    // to FAILED_COMMITS (charged to the budget), not rethrown and not routed to atomic cleanup.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setPartialProgressEnabled(true).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    List<String> outputs = newFilePaths(batches.get(0));
    assertFalse("sanity: the batch produced output files", outputs.isEmpty());
    Table loaded = warehouse.loadTable(tableId);
    for (String p : outputs) {
      assertTrue(
          "output should exist before the failed commit", loaded.io().newInputFile(p).exists());
    }

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(
            new FailingCommit(
                tableId.toString(),
                catalogConfig(table),
                config,
                new InjectedCleanableFailure("injected cleanable failure")));
    tester.processBundle(batches.get(0)); // must NOT throw: partial progress tolerates it

    assertTrue(
        "a tolerated failure must not commit",
        tester.peekOutputElements(CommitRewriteGroups.COMMITTED).isEmpty());
    assertEquals(
        "the failed batch must be reported to FAILED_COMMITS",
        1,
        tester.peekOutputElements(CommitRewriteGroups.FAILED_COMMITS).size());
    // The same failure is reported in the result fragment (the reporting channel; FAILED_COMMITS
    // still drives the budget).
    List<RewriteResult> summaries = tester.peekOutputElements(CommitRewriteGroups.COMMIT_SUMMARY);
    assertEquals("one commit fragment", 1, summaries.size());
    assertEquals("reports the failed commit", 1L, summaries.get(0).getFailedCommits());
    assertEquals("nothing committed", 0L, summaries.get(0).getCommittedSnapshots());
    // The batch's output files must be RETAINED, not deleted: a retried or concurrent attempt of
    // this same commit element could still commit them.
    Table after = warehouse.loadTable(tableId);
    for (String p : outputs) {
      assertTrue(
          "a tolerated partial-progress failure must NOT delete this batch's output files: " + p,
          after.io().newInputFile(p).exists());
    }
  }

  @Test
  public void partialToleratedFailureLeavesFilesCommittableByRetry() throws Exception {
    // Data-loss regression: under partial progress a terminal commit failure must NOT delete the
    // batch's output files. Beam can reprocess the same commit element (a sibling key's
    // CommitStateUnknownException fails the bundle, or a zombie attempt runs concurrently) and
    // that retry may commit — landing a snapshot referencing missing files if they were deleted.
    Table table = buildTable(6);
    long recordsBefore = countRows(table);
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setPartialProgressEnabled(true).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    // Materialize so the same batch can be replayed against a second (succeeding) commit DoFn.
    List<ExecutedGroup> groupList = ImmutableList.copyOf(batches.get(0).getValue());
    KV<Integer, Iterable<ExecutedGroup>> batch =
        KV.of(batches.get(0).getKey(), (Iterable<ExecutedGroup>) groupList);
    List<String> outputs = newFilePaths(batch);

    // 1. First attempt fails terminally; tolerated under partial progress (charged to the budget).
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> failing =
        DoFnTester.of(
            new FailingCommit(
                tableId.toString(),
                catalogConfig(table),
                config,
                new InjectedCleanableFailure("injected cleanable failure")));
    failing.processBundle(batch);
    assertEquals(
        "first attempt must be reported as a failed commit",
        1,
        failing.peekOutputElements(CommitRewriteGroups.FAILED_COMMITS).size());

    // 2. Its output files must still exist (not deleted).
    Table afterFail = warehouse.loadTable(tableId);
    for (String p : outputs) {
      assertTrue(
          "a tolerated partial-progress failure must retain its output files: " + p,
          afterFail.io().newInputFile(p).exists());
    }

    // 3. A retry of the SAME batch now commits successfully, and every data file the committed
    // snapshot references must exist on disk (proving the retained outputs were commitable).
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> succeeding =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    succeeding.processBundle(batch);
    assertEquals(
        "the retried commit must land exactly one snapshot",
        1,
        succeeding.peekOutputElements(CommitRewriteGroups.COMMITTED).size());

    Table committed = warehouse.loadTable(tableId);
    committed.refresh();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        committed.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        assertTrue(
            "committed snapshot references a data file that is missing on disk: "
                + t.file().location(),
            committed.io().newInputFile(t.file().location().toString()).exists());
      }
    }
    assertEquals("no rows may be lost by the rewrite", recordsBefore, countRows(committed));
  }

  @Test
  public void commitEmitsItsOwnStampedSnapshot() throws Exception {
    // commitOnce must emit the snapshot IT created — located by its (operationId, commitKey)
    // stamp — not table.currentSnapshot(), which a concurrent commit on the shared cached table
    // could have advanced. The race isn't deterministically reproducible, so this pins the
    // observable guarantee: the emitted snapshot carries this commit's own stamp.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    KV<Integer, Iterable<ExecutedGroup>> batch = batches.get(0);
    int commitKey = batch.getKey();

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batch);
    List<SnapshotInfo> committed = tester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals(1, committed.size());

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    Snapshot emitted = reloaded.snapshot(committed.get(0).getSnapshotId());
    assertEquals(
        "emitted snapshot must carry this commit's own commit-key stamp",
        Integer.toString(commitKey),
        emitted.summary().get(CommitRewriteGroups.COMMIT_KEY_PROP));
  }

  @Test
  public void incompleteParentGroupIsNotCommitted() throws Exception {
    // Parent-group atomicity: if a parent group was split into N subgroups but fewer than N
    // rewrote successfully, NONE of that parent's subgroups may be committed — deleting the
    // parent's byte-range-split input files while missing a subgroup's rewritten range would lose
    // that range's rows. An incomplete parent is simulated by claiming a subgroup count of 2 while
    // supplying only one subgroup; the parent's inputs must stay live.
    Table table = buildTable(6);
    long recordsBefore = countRows(table);
    int snapsBefore = snapshotCount(table);
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setPartialProgressEnabled(true).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    ExecutedGroup real = batches.get(0).getValue().iterator().next();

    Set<String> inputPaths = new HashSet<>();
    for (SerializableDataFile sdf : real.getRewrittenDataFiles()) {
      inputPaths.add(sdf.getPath());
    }
    assertFalse("sanity: the parent has input files", inputPaths.isEmpty());

    ExecutedGroup incomplete =
        ExecutedGroup.builder()
            .setStartingSnapshotId(real.getStartingSnapshotId())
            .setStartingSequenceNumber(real.getStartingSequenceNumber())
            .setOperationId(real.getOperationId())
            .setParentGroupIndex(real.getParentGroupIndex())
            .setParentSubgroupCount(2) // claim 2 expected, supply only this 1 -> incomplete parent
            .setTotalInputByteSize(real.getTotalInputByteSize())
            .setNewFiles(real.getNewFiles())
            .setRewrittenDataFiles(real.getRewrittenDataFiles())
            .setDanglingDeleteFileJsons(real.getDanglingDeleteFileJsons())
            .build();
    KV<Integer, Iterable<ExecutedGroup>> batch =
        KV.of(0, (Iterable<ExecutedGroup>) ImmutableList.of(incomplete));

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batch);

    assertTrue(
        "an incomplete parent group must not be committed",
        tester.peekOutputElements(CommitRewriteGroups.COMMITTED).isEmpty());
    // A batch that is empty after the completeness filter has nothing to report: the rewrite
    // failure is already counted via the failed-parents side output.
    assertTrue(
        "no commit fragment for an all-incomplete batch",
        tester.peekOutputElements(CommitRewriteGroups.COMMIT_SUMMARY).isEmpty());
    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertEquals(
        "no snapshot may be created for an incomplete parent",
        snapsBefore,
        snapshotCount(reloaded));
    Set<String> livePaths = new HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        livePaths.add(t.file().location().toString());
      }
    }
    for (String p : inputPaths) {
      assertTrue("incomplete parent's input file must remain live: " + p, livePaths.contains(p));
    }
    assertEquals("no rows may be lost", recordsBefore, countRows(reloaded));
  }

  @Test
  public void commitFailsClosedWhenStartingSnapshotUnavailable() throws Exception {
    // useStartingSequenceNumber (default true): if the starting snapshot is no longer loadable we
    // cannot preserve the data sequence number, so late deletes could stop applying to the
    // rewritten files. Fail closed rather than silently proceeding with a fresh sequence number.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    List<ExecutedGroup> groups = ImmutableList.copyOf(batches.get(0).getValue());

    CommitRewriteGroups doFn =
        new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config);
    Table loaded = warehouse.loadTable(tableId);
    long bogusSnapshotId = -12345L; // not a snapshot in this table

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                doFn.commitOnce(
                    loaded, groups, bogusSnapshotId, 0, groups.get(0).getOperationId()));
    assertTrue(
        "message should point at the missing starting snapshot: " + ex.getMessage(),
        ex.getMessage().toLowerCase().contains("starting snapshot"));
  }

  @Test
  public void useStartingSequenceNumberFalseAssignsFreshSequenceNumber() throws Exception {
    // With useStartingSequenceNumber=false the rewritten files must get a NEW (fresh) data
    // sequence number — the rewrite's own — not the starting snapshot's, and the fail-closed check
    // on the starting snapshot is skipped entirely. (With the default true they keep the starting
    // number so late deletes still apply — see the fail-closed companion above.)
    Table table = buildTable(6);
    long startingSeq = table.currentSnapshot().sequenceNumber();
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setUseStartingSequenceNumber(false).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batches.get(0)); // must NOT throw / fail closed
    assertEquals(1, tester.peekOutputElements(CommitRewriteGroups.COMMITTED).size());

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    long committedSeq = reloaded.currentSnapshot().sequenceNumber();
    assertTrue(
        "the rewrite must advance to a new, higher snapshot sequence number",
        committedSeq > startingSeq);
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        assertEquals(
            "useStartingSequenceNumber=false must assign the fresh data sequence number",
            Long.valueOf(committedSeq),
            t.file().dataSequenceNumber());
      }
    }
  }

  @Test
  public void nonCleanableCommitFailureRethrown() throws Exception {
    // A non-cleanable failure (outcome not known to be safe to clean) must be rethrown WITHOUT
    // cleanup, not routed aside — the runner may retry, and the idempotency check guards a
    // re-commit.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    List<String> outputs = newFilePaths(batches.get(0));
    assertFalse("sanity: the batch produced output files", outputs.isEmpty());

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(
            new FailingCommit(
                tableId.toString(),
                catalogConfig(table),
                config,
                new IllegalStateException("injected non-cleanable failure")));

    // Pin the ACTUAL propagated type (a non-cleanable failure is rethrown as-is), not merely that
    // something threw.
    assertThrows(IllegalStateException.class, () -> tester.processBundle(batches.get(0)));

    // The outcome is not known to be safe to clean, so the output files must NOT be deleted: a
    // runner retry re-references them and the idempotency check guards a re-commit.
    Table after = warehouse.loadTable(tableId);
    for (String p : outputs) {
      assertTrue(
          "a non-cleanable failure must NOT delete output files",
          after.io().newInputFile(p).exists());
    }
  }

  @Test
  public void commitRetryRecoversFromTransientConflict() throws Exception {
    // The bounded commit retry loop must RECOVER, not just fail: a real CommitFailedException on
    // the first attempt, then the real commit. Exactly one committed snapshot, one new REPLACE
    // snapshot, and nothing routed to FAILED_COMMITS.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(
            new FlakyCommit(tableId.toString(), catalogConfig(table), config, /* failFirstN= */ 1));
    tester.processBundle(batches.get(0));

    assertEquals(
        "the retried commit must land exactly one snapshot",
        1,
        tester.peekOutputElements(CommitRewriteGroups.COMMITTED).size());
    assertTrue(
        "a recovered retry must not report a failed commit",
        tester.peekOutputElements(CommitRewriteGroups.FAILED_COMMITS).isEmpty());
    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertEquals("exactly one new REPLACE snapshot", 1, replaceSnapshots(reloaded));
  }

  @Test
  public void lastChanceRecheckFindsLandedCommit() throws Exception {
    // If a commit LANDS but the attempt then throws (a lost/timed-out response), the retry loop
    // exhausts and the last-chance findCommittedSnapshot recheck must locate the already-stamped
    // snapshot and emit it, rather than reporting a false failure.
    Table table = buildTable(6);
    long recordsBefore = countRows(table);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    KV<Integer, Iterable<ExecutedGroup>> batch = batches.get(0);
    List<String> outputs = newFilePaths(batch);

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new LandsThenThrows(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batch); // must NOT throw: the recheck absorbs the landed-then-threw commit

    List<SnapshotInfo> committed = tester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals("the last-chance recheck must emit the landed snapshot", 1, committed.size());
    assertTrue(
        "a landed commit must not be reported as failed",
        tester.peekOutputElements(CommitRewriteGroups.FAILED_COMMITS).isEmpty());
    // The last-chance-landed path reports the committed snapshot in the result fragment too.
    List<RewriteResult> summaries = tester.peekOutputElements(CommitRewriteGroups.COMMIT_SUMMARY);
    assertEquals("one commit fragment", 1, summaries.size());
    assertEquals("the landed commit is reported", 1L, summaries.get(0).getCommittedSnapshots());
    assertEquals("not reported as a failure", 0L, summaries.get(0).getFailedCommits());
    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    Snapshot emitted = reloaded.snapshot(committed.get(0).getSnapshotId());
    assertEquals(
        "emitted snapshot must carry this commit's own stamp",
        Integer.toString(batch.getKey()),
        emitted.summary().get(CommitRewriteGroups.COMMIT_KEY_PROP));
    for (String p : outputs) {
      assertTrue(
          "the landed commit's output files must remain on disk: " + p,
          reloaded.io().newInputFile(p).exists());
    }
    assertEquals("no rows lost", recordsBefore, countRows(reloaded));
  }

  @Test
  public void commitStateUnknownRethrownWithoutRetryOrCleanup() throws Exception {
    // A CommitStateUnknownException means the commit may have succeeded server-side, so it is
    // rethrown IMMEDIATELY (no internal retry) and this batch's output files are NOT deleted. A
    // Beam bundle retry plus the idempotency stamp check absorb it.
    Table table = buildTable(6);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    List<String> outputs = newFilePaths(batches.get(0));
    assertFalse("sanity: the batch produced output files", outputs.isEmpty());

    FailingCommit failing =
        new FailingCommit(
            tableId.toString(),
            catalogConfig(table),
            config,
            new CommitStateUnknownException(new RuntimeException("boom")));
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester = DoFnTester.of(failing);
    // DO_NOT_CLONE so failing.calls reflects the real invocation count.
    tester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);

    assertThrows(CommitStateUnknownException.class, () -> tester.processBundle(batches.get(0)));
    assertEquals(
        "state-unknown must be rethrown on the first attempt (no internal retries)",
        1,
        failing.calls);
    assertTrue(
        "state-unknown must not route to FAILED_COMMITS",
        tester.peekOutputElements(CommitRewriteGroups.FAILED_COMMITS).isEmpty());
    Table after = warehouse.loadTable(tableId);
    for (String p : outputs) {
      assertTrue(
          "state-unknown must NOT delete this batch's output files: " + p,
          after.io().newInputFile(p).exists());
    }
  }

  @Test
  public void completeParentCommitsWhileIncompleteParentIsExcluded() throws Exception {
    // In a MIXED batch, completeParentSubgroups must filter at PARENT granularity — a fully
    // present parent commits while an incomplete parent (a subgroup missing) is excluded whole,
    // its inputs left live. Two partitions with one file each yield two SEPARATE parent groups
    // (planning is per-partition), one subgroup each; under maxCommits=1 they share commit key 0.
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();
    tableId = TableIdentifier.of("default", "b5_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema, spec);
    AppendFiles append = table.newAppend();
    for (int shard = 0; shard < 2; shard++) {
      Record partition = GenericRecord.create(spec.partitionType());
      partition.setField("shard", shard);
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        Record r = GenericRecord.create(schema);
        r.setField("id", (long) (shard * 100 + i));
        r.setField("shard", shard);
        records.add(r);
      }
      append.appendFile(
          warehouse.writeRecords(
              "b5_" + shard + "_" + System.nanoTime() + ".parquet",
              schema,
              spec,
              partition,
              records));
    }
    append.commit();
    table.refresh();
    long recordsBefore = countRows(table);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(1) // both parent groups share commit key 0
            .setRewriteOptions(ImmutableMap.of("min-input-files", "1", "rewrite-all", "true"))
            .build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertEquals("both parents must share one commit key", 1, batches.size());
    List<ExecutedGroup> parents = ImmutableList.copyOf(batches.get(0).getValue());
    assertEquals("fixture must yield two single-subgroup parents", 2, parents.size());

    ExecutedGroup complete = parents.get(0); // parent A: 1 of 1 subgroups -> complete
    ExecutedGroup real = parents.get(1);
    ExecutedGroup incomplete = // parent B: claim 2 subgroups, supply only 1 -> incomplete
        ExecutedGroup.builder()
            .setStartingSnapshotId(real.getStartingSnapshotId())
            .setStartingSequenceNumber(real.getStartingSequenceNumber())
            .setOperationId(real.getOperationId())
            .setParentGroupIndex(real.getParentGroupIndex())
            .setParentSubgroupCount(2)
            .setTotalInputByteSize(real.getTotalInputByteSize())
            .setNewFiles(real.getNewFiles())
            .setRewrittenDataFiles(real.getRewrittenDataFiles())
            .setDanglingDeleteFileJsons(real.getDanglingDeleteFileJsons())
            .build();

    Set<String> completeInputs = new HashSet<>();
    for (SerializableDataFile sdf : complete.getRewrittenDataFiles()) {
      completeInputs.add(sdf.getPath());
    }
    Set<String> incompleteInputs = new HashSet<>();
    for (SerializableDataFile sdf : incomplete.getRewrittenDataFiles()) {
      incompleteInputs.add(sdf.getPath());
    }
    List<String> completeOutputs = new ArrayList<>();
    for (SerializableDataFile sdf : complete.getNewFiles()) {
      completeOutputs.add(sdf.getPath());
    }

    KV<Integer, Iterable<ExecutedGroup>> batch =
        KV.of(0, (Iterable<ExecutedGroup>) ImmutableList.of(complete, incomplete));
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batch);

    assertEquals(
        "exactly one commit for the complete parent",
        1,
        tester.peekOutputElements(CommitRewriteGroups.COMMITTED).size());
    // The fragment reports the complete parent's commit.
    List<RewriteResult> summaries = tester.peekOutputElements(CommitRewriteGroups.COMMIT_SUMMARY);
    assertEquals("one commit fragment", 1, summaries.size());
    assertEquals("the complete parent committed", 1L, summaries.get(0).getCommittedSnapshots());
    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertEquals("exactly one REPLACE snapshot", 1, replaceSnapshots(reloaded));

    Set<String> live = new HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        live.add(t.file().location().toString());
      }
    }
    // Complete parent A: inputs replaced (gone), output live.
    for (String in : completeInputs) {
      assertFalse("complete parent's input must be deleted: " + in, live.contains(in));
    }
    for (String out : completeOutputs) {
      assertTrue("complete parent's output must be live: " + out, live.contains(out));
    }
    // Incomplete parent B: inputs stay live (never partially replaced).
    for (String in : incompleteInputs) {
      assertTrue("incomplete parent's input must remain live: " + in, live.contains(in));
    }
    assertEquals("no rows lost", recordsBefore, countRows(reloaded));
  }

  @Test
  public void branchCompactionCommitsToBranchWithUserPropsLeavingMainUntouched() throws Exception {
    // With setBranch, planning reads the branch head and the commit lands on the branch; main is
    // untouched. The commit summary carries the user snapshot property AND both beam.rewrite.*
    // idempotency stamps.
    Table table = buildTable(6);
    long mainHead = table.currentSnapshot().snapshotId();
    String branch = "audit";
    table.manageSnapshots().createBranch(branch, mainHead).commit();
    table.refresh();

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setBranch(branch)
            .setSnapshotProperties(ImmutableMap.of("team", "data-platform"))
            .build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertEquals(1, batches.size());

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batches.get(0));
    List<SnapshotInfo> committed = tester.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals(1, committed.size());

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertEquals(
        "main head must be unchanged by a branch compaction",
        mainHead,
        reloaded.currentSnapshot().snapshotId());
    SnapshotRef ref = reloaded.refs().get(branch);
    assertNotNull("branch must still exist", ref);
    Snapshot branchHead = reloaded.snapshot(ref.snapshotId());
    assertEquals(
        "the branch head must be the committed REPLACE snapshot",
        committed.get(0).getSnapshotId(),
        branchHead.snapshotId());
    assertEquals(DataOperations.REPLACE, branchHead.operation());
    assertEquals("data-platform", branchHead.summary().get("team"));
    assertNotNull(
        "operation-id stamp must be present",
        branchHead.summary().get(CommitRewriteGroups.OP_ID_PROP));
    assertEquals("0", branchHead.summary().get(CommitRewriteGroups.COMMIT_KEY_PROP));
  }

  @Test
  public void branchCompactionIdempotentReplay() throws Exception {
    // Replaying a committed branch batch must find the stamp on the BRANCH head (not main's
    // ancestry) and re-emit it, creating no new snapshot.
    Table table = buildTable(6);
    String branch = "audit";
    table.manageSnapshots().createBranch(branch, table.currentSnapshot().snapshotId()).commit();
    table.refresh();

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setBranch(branch).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    List<ExecutedGroup> groupList = ImmutableList.copyOf(batches.get(0).getValue());
    KV<Integer, Iterable<ExecutedGroup>> replayable =
        KV.of(batches.get(0).getKey(), (Iterable<ExecutedGroup>) groupList);

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> first =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    first.processBundle(replayable);
    long firstId = first.peekOutputElements(CommitRewriteGroups.COMMITTED).get(0).getSnapshotId();
    Table afterFirst = warehouse.loadTable(tableId);
    afterFirst.refresh();
    int snapsAfterFirst = snapshotCount(afterFirst);

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> second =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    second.processBundle(replayable);
    List<SnapshotInfo> secondCommitted = second.peekOutputElements(CommitRewriteGroups.COMMITTED);
    assertEquals(1, secondCommitted.size());
    assertEquals(
        "replay must re-emit the same branch snapshot",
        firstId,
        secondCommitted.get(0).getSnapshotId());
    Table afterSecond = warehouse.loadTable(tableId);
    afterSecond.refresh();
    assertEquals(
        "replay must NOT create a new snapshot", snapsAfterFirst, snapshotCount(afterSecond));
  }

  @Test
  public void branchCompactionPlansFromDivergedBranchHeadNotMain() throws Exception {
    // The branch head must DIVERGE from main before planning, otherwise a regression that planned
    // from main's head would still pass the other branch tests (whose branch head == main head).
    // Files appended ONLY to the branch must appear among the rewritten inputs, and main's head
    // must stay untouched.
    Table table = buildTable(3);
    long mainHead = table.currentSnapshot().snapshotId();
    String branch = "diverge";
    table.manageSnapshots().createBranch(branch, mainHead).commit();
    table.refresh();

    // Append 3 more files to the BRANCH only: its head now diverges from main's.
    Set<String> branchOnlyPaths = new HashSet<>();
    AppendFiles branchAppend = table.newAppend();
    for (int i = 0; i < 3; i++) {
      DataFile df =
          warehouse.writeRecords(
              "branchonly" + i + "_" + System.nanoTime() + ".parquet",
              table.schema(),
              TestFixtures.FILE1SNAPSHOT1);
      branchAppend.appendFile(df);
      branchOnlyPaths.add(df.location());
    }
    branchAppend.toBranch(branch).commit();
    table.refresh();
    long branchHeadBefore = table.refs().get(branch).snapshotId();
    assertNotEquals("sanity: the branch must have diverged from main", mainHead, branchHeadBefore);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setBranch(branch)
            .setRewriteOptions(ImmutableMap.of("rewrite-all", "true"))
            .build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertEquals(1, batches.size());

    // The branch-only files must be among the rewritten INPUTS — proof planning read the branch
    // head, not main's (main has only its 3 original files, none of these).
    Set<String> rewrittenInputs = rewrittenInputPaths(batches.get(0));
    assertTrue(
        "branch-only files must be compacted (planned from the branch head): " + rewrittenInputs,
        rewrittenInputs.containsAll(branchOnlyPaths));

    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), catalogConfig(table), config));
    tester.processBundle(batches.get(0));

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertEquals(
        "main head must be untouched by a branch compaction",
        mainHead,
        reloaded.currentSnapshot().snapshotId());
    SnapshotRef ref = reloaded.refs().get(branch);
    assertNotNull("branch must still exist", ref);
    assertEquals(
        "the new branch head must be a REPLACE (compaction) snapshot",
        DataOperations.REPLACE,
        reloaded.snapshot(ref.snapshotId()).operation());
    assertNotEquals(
        "the branch head must advance to the compaction snapshot",
        branchHeadBefore,
        ref.snapshotId());
  }

  /** Concatenated messages of a throwable and its cause chain (pipeline exceptions wrap deeply). */
  private static String causeChainMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null) {
        sb.append(c.getMessage()).append(" | ");
      }
    }
    return sb.toString();
  }
}
