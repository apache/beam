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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests for {@link RewriteDataFiles} driven through {@link IcebergMaintenance}. */
@RunWith(JUnit4.class)
public class RewriteDataFilesTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private TableIdentifier tableId;

  /**
   * Creates an unpartitioned table with {@code numFiles} small data files of DISTINCT rows (id N ->
   * data "row-N", two rows per file). Distinct payloads let {@link #rowMultiset} detect dropped,
   * duplicated, or swapped rows that an identical-record fixture would hide.
   */
  private Table buildTable(int numFiles) throws IOException {
    tableId = TableIdentifier.of("default", "rewrite_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    AppendFiles append = table.newAppend();
    long id = 0;
    for (int f = 0; f < numFiles; f++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < 2; i++, id++) {
        Record r = GenericRecord.create(TestFixtures.SCHEMA);
        r.setField("id", id);
        r.setField("data", "row-" + id);
        records.add(r);
      }
      append.appendFile(
          // Record split offsets so each (single-row-group) file yields exactly ONE range task.
          // Without them a tiny target-file-size drives the fixed-size split fallback to shatter a
          // file into many sub-byte ranges, turning one poisoned file into many subgroup failures
          // and skewing the per-parent failed-rewrite count the result asserts.
          warehouse.writeRecords(
              "f" + f + "_" + System.nanoTime() + ".parquet",
              table.schema(),
              records,
              ImmutableMap.of()));
    }
    append.commit();
    table.refresh();
    return table;
  }

  /**
   * Parquet writer properties that force many small row groups from little data (mirrors {@code
   * PlanRewriteGroupsTest}/{@code RewriteDataFilesCorrectnessTest}): tiny row-group size,
   * dictionary off, tiny pages, capped check interval, uncompressed.
   */
  private static final Map<String, String> MULTI_ROW_GROUP_PROPS =
      ImmutableMap.<String, String>builder()
          .put("write.parquet.row-group-size-bytes", "8192")
          .put("parquet.enable.dictionary", "false")
          .put("write.parquet.page-size-bytes", "1024")
          .put("write.parquet.row-group-check-max-record-count", "100")
          .put("write.parquet.compression-codec", "uncompressed")
          .build();

  /** A table with a single file spanning many row groups (its scan task splits per row group). */
  private Table buildMultiRowGroupTable(int records) throws Exception {
    tableId = TableIdentifier.of("default", "mrg_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    List<Record> rows = new ArrayList<>();
    for (int i = 0; i < records; i++) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "row-" + i + "-" + (i * 2654435761L));
      rows.add(r);
    }
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "mrg_" + System.nanoTime() + ".parquet",
                table.schema(),
                rows,
                MULTI_ROW_GROUP_PROPS))
        .commit();
    table.refresh();
    return table;
  }

  private static String onlyDataFile(Table table) throws Exception {
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(it);
      assertEquals("fixture must have exactly one file", 1, tasks.size());
      return tasks.get(0).file().location();
    }
  }

  private static long totalDataFileBytes(Table table) throws Exception {
    long total = 0;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      for (FileScanTask t : it) {
        total += t.file().fileSizeInBytes();
      }
    }
    return total;
  }

  private static void assertRowGroupsAtLeast(Table table, int min) throws Exception {
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      for (FileScanTask t : it) {
        List<Long> offsets = t.file().splitOffsets();
        int rowGroups = offsets == null ? 1 : offsets.size();
        assertTrue("fixture must have multi-row-group files; got " + rowGroups, rowGroups >= min);
      }
    }
  }

  /** The table's live rows as a sorted (id|data) multiset. */
  private List<String> rowMultiset(Table table) throws IOException {
    List<String> keys = new ArrayList<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        keys.add(r.getField("id") + "|" + r.getField("data"));
      }
    }
    Collections.sort(keys);
    return keys;
  }

  /** Catalog properties pointed at the same Hadoop warehouse the test table lives in. */
  private Map<String, String> catalogProps(Table table) {
    String tableLocation = table.location(); // <warehouse>/<namespace>/<table>
    String suffix = "/" + tableId.namespace().toString() + "/" + tableId.name();
    assertTrue("Unexpected table location: " + tableLocation, tableLocation.endsWith(suffix));
    String warehouseRoot = tableLocation.substring(0, tableLocation.length() - suffix.length());
    return ImmutableMap.of(
        "type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, "warehouse", warehouseRoot);
  }

  private int liveDataFiles(Table table) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask ignored : tasks) {
        count++;
      }
    }
    return count;
  }

  /** The rows live at a specific snapshot (e.g. a branch head) as a sorted (id|data) multiset. */
  private List<String> rowMultisetAt(Table table, long snapshotId) throws IOException {
    List<String> keys = new ArrayList<>();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).useSnapshot(snapshotId).build()) {
      for (Record r : records) {
        keys.add(r.getField("id") + "|" + r.getField("data"));
      }
    }
    Collections.sort(keys);
    return keys;
  }

  /** The data-file count at a specific snapshot (e.g. a branch head). */
  private int liveDataFilesAt(Table table, long snapshotId) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      for (FileScanTask ignored : tasks) {
        count++;
      }
    }
    return count;
  }

  private long replaceSnapshots(Table table) {
    long count = 0;
    for (Snapshot s : table.snapshots()) {
      if (DataOperations.REPLACE.equals(s.operation())) {
        count++;
      }
    }
    return count;
  }

  private static final Map<String, String> REWRITE_ALL = ImmutableMap.of("rewrite-all", "true");

  private void addPositionalDelete(Table table, DataFile dataFile, long position) throws Exception {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec());
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1L).format(FileFormat.PARQUET).build();
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    PositionDeleteWriter<Record> writer =
        appenderFactory.newPosDeleteWriter(outputFile, FileFormat.PARQUET, null);
    PositionDelete<Record> positionDelete = PositionDelete.create();
    try {
      positionDelete.set(dataFile.location().toString(), position, null);
      writer.write(positionDelete);
    } finally {
      writer.close();
    }
    DeleteFile deleteFile = writer.toDeleteFile();
    table.newRowDelta().addDeletes(deleteFile).commit();
    table.refresh();
  }

  @Test
  public void endToEndAtomicCommitConflictFailsWithoutCommitting() throws Exception {
    // Commit-conflict handling through the FULL expand() wiring: a concurrent change to a file
    // being rewritten makes the atomic commit conflict, so after retries the pipeline FAILS having
    // committed nothing. Output files are left as tagged orphans — a retry could still commit them.
    Table table = buildTable(4);
    long startingSnapshot = table.currentSnapshot().snapshotId();
    DataFile victim;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      victim = tasks.iterator().next().file();
    }
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setSnapshotId(startingSnapshot)
            .setRewriteOptions(REWRITE_ALL)
            .build();
    // Conflicting change committed AFTER the starting snapshot: remove a file the rewrite will try
    // to replace, so the atomic commit's validateFromSnapshot detects the conflict.
    table.newDelete().deleteFile(victim).commit();
    table.refresh();

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                    .rewriteDataFiles(config)
                    .run()
                    .waitUntilFinish());
    // Assert the ACTUAL failure cause (a commit conflict), not merely that something threw.
    assertTrue(
        "the failure must describe the commit conflict: " + causeChainMessage(ex),
        causeChainMessage(ex).contains("conflicted with a concurrent"));

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "a conflicted atomic rewrite must commit no REPLACE snapshot",
        0,
        replaceSnapshots(reloaded));
  }

  @Test
  public void rewriteResultReportsThePlannedAndCommittedReality() throws Exception {
    // The RESULT row summarizes the run — planned parents/files, committed snapshot, files
    // added/removed, rewritten bytes — and reports zero rewrite failures on a healthy run.
    Table table = buildTable(6);
    long inputFiles = liveDataFiles(table); // 6 files -> one parent group -> one commit
    Pipeline pipeline = Pipeline.create();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table), pipeline);
    maintenance.rewriteDataFiles(RewriteDataFiles.Configuration.builder().build());
    PAssert.thatSingleton(maintenance.rewriteResult())
        .satisfies(
            r -> {
              assertNotNull("operation id set on a real run", r.getOperationId());
              assertEquals("one parent group planned", 1L, r.getPlannedParentGroups());
              assertEquals("all input files planned", inputFiles, r.getPlannedFiles());
              assertEquals("no rewrite failures", 0L, r.getFailedRewriteParents());
              assertEquals("one snapshot committed", 1L, r.getCommittedSnapshots());
              assertEquals("no commit failures", 0L, r.getFailedCommits());
              assertEquals("the input files removed", inputFiles, r.getFilesRemoved());
              // The fixture deterministically compacts the 6 inputs into ONE output file.
              assertEquals("one compacted file added", 1L, r.getFilesAdded());
              assertTrue("rewritten bytes reported", r.getRewrittenBytes() > 0);
              return null;
            });
    pipeline.run().waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "the rewrite committed exactly one REPLACE snapshot", 1, replaceSnapshots(reloaded));
    // Reality check: the result's filesAdded equals the post-run live (compacted) file count.
    assertEquals(
        "result filesAdded matches the live compacted-file count", 1, liveDataFiles(reloaded));
  }

  @Test
  public void emptyTableRewriteResultIsAllZeros() throws Exception {
    // An empty-table run is a no-op but STILL produces exactly one result row — all zeros with a
    // null operation id (the Combine identity on empty input).
    tableId = TableIdentifier.of("default", "emptyres_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    assertNull("sanity: an empty table has no snapshot", table.currentSnapshot());
    Pipeline pipeline = Pipeline.create();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table), pipeline);
    maintenance.rewriteDataFiles();
    PAssert.thatSingleton(maintenance.rewriteResult())
        .satisfies(
            r -> {
              assertNull("no operation id for an empty run", r.getOperationId());
              assertNull("no starting snapshot for an empty run", r.getStartingSnapshotId());
              assertEquals(0L, r.getPlannedParentGroups());
              assertEquals(0L, r.getPlannedFiles());
              assertEquals(0L, r.getPlannedBytes());
              assertEquals(0L, r.getFailedRewriteParents());
              assertEquals(0L, r.getCommittedSnapshots());
              assertEquals(0L, r.getFailedCommits());
              assertEquals(0L, r.getFilesAdded());
              assertEquals(0L, r.getFilesRemoved());
              assertEquals(0L, r.getRewrittenBytes());
              return null;
            });
    pipeline.run().waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertNull("no snapshot created for an empty table", reloaded.currentSnapshot());
  }

  @Test
  public void emptyTableMaintenanceIsNoOp() throws Exception {
    // A table created but never written (no snapshot) must not crash maintenance — it runs as a
    // graceful no-op, so a scheduled rewrite over an empty table doesn't fail and page.
    tableId = TableIdentifier.of("default", "empty_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    assertNull("sanity: an empty table has no snapshot", table.currentSnapshot());

    // Must not throw at build or run.
    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles()
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertNull(
        "no snapshot should have been created for an empty table", reloaded.currentSnapshot());
  }

  @Test
  public void branchOnlyWapTableCompactsOnBranchLeavingMainNull() throws Exception {
    // A WAP table whose only commits went to a branch has a null MAIN head but a live branch ref.
    // setBranch must resolve the impulse head from that branch, not main — a null main head builds
    // an empty no-op impulse and the branch is then silently never compacted.
    tableId = TableIdentifier.of("default", "wap_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    String branch = "audit";
    long id = 0;
    for (int f = 0; f < 6; f++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < 2; i++, id++) {
        Record r = GenericRecord.create(TestFixtures.SCHEMA);
        r.setField("id", id);
        r.setField("data", "row-" + id);
        records.add(r);
      }
      // Commit ONLY to the branch: main is never created, so currentSnapshot() stays null.
      table
          .newAppend()
          .appendFile(
              warehouse.writeRecords(
                  "wap" + f + "_" + System.nanoTime() + ".parquet", table.schema(), records))
          .toBranch(branch)
          .commit();
    }
    table.refresh();
    assertNull("sanity: a branch-only WAP table has no main snapshot", table.currentSnapshot());
    long branchHeadBefore = table.refs().get(branch).snapshotId();
    List<String> expectedRows = rowMultisetAt(table, branchHeadBefore);
    assertEquals("baseline branch must have 6 files", 6, liveDataFilesAt(table, branchHeadBefore));

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(
            RewriteDataFiles.Configuration.builder()
                .setBranch(branch)
                .setRewriteOptions(REWRITE_ALL)
                .build())
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    reloaded.refresh();
    assertNull(
        "main head must stay null — the compaction landed on the branch",
        reloaded.currentSnapshot());
    SnapshotRef ref = reloaded.refs().get(branch);
    assertNotNull("branch must still exist", ref);
    Snapshot branchHead = reloaded.snapshot(ref.snapshotId());
    assertEquals(
        "the branch head must be a REPLACE (compaction) snapshot",
        DataOperations.REPLACE,
        branchHead.operation());
    assertEquals("branch rows preserved", expectedRows, rowMultisetAt(reloaded, ref.snapshotId()));
    assertTrue(
        "compaction must reduce the branch file count",
        liveDataFilesAt(reloaded, ref.snapshotId()) < 6);
  }

  @Test
  public void setBranchMissingFailsAtTaskAddTime() throws Exception {
    // setBranch naming a branch that does not exist must fail synchronously at
    // rewriteDataFiles(...) time — not silently no-op, nor fail deep inside the running job.
    Table table = buildTable(2);
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                maintenance.rewriteDataFiles(
                    RewriteDataFiles.Configuration.builder().setBranch("no-such-branch").build()));
    assertTrue(
        "message must explain the branch does not exist: " + ex.getMessage(),
        ex.getMessage().contains("does not exist"));
  }

  @Test
  public void explicitSnapshotIdOnMainEmptyTableIsConsultedNotSilentlyIgnored() throws Exception {
    // On a table whose MAIN head is empty, an explicit snapshotId must be resolved BEFORE the
    // branch/main fallbacks; falling back to the null currentSnapshot() makes the run a silent
    // no-op that ignores the pin. An unresolvable pin instead fails loudly at task-add time, which
    // is the observable proof (a main-empty table has no files to commit, so a valid pin cannot
    // round-trip a commit here).
    tableId = TableIdentifier.of("default", "pin_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    assertNull("sanity: an empty table has no main snapshot", table.currentSnapshot());

    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                maintenance.rewriteDataFiles(
                    RewriteDataFiles.Configuration.builder().setSnapshotId(999_999L).build()));
    assertTrue(
        "message must name the missing snapshot: " + ex.getMessage(),
        ex.getMessage().contains("snapshot 999999 not found"));
  }

  @Test
  public void endToEndMorDeletesApplied() throws Exception {
    // MOR positional delete driven through the ASSEMBLED pipeline (Redistribute, coders, GBK,
    // atomic gate, commit stage): the rewrite reads through the delete filter, the new file
    // excludes the deleted row, and no live delete remains afterward.
    tableId = TableIdentifier.of("default", "mor_e2e_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    DataFile dataFile1 =
        warehouse.writeRecords(
            "mor1_" + System.nanoTime() + ".parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1);
    DataFile dataFile2 =
        warehouse.writeRecords(
            "mor2_" + System.nanoTime() + ".parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
    table.refresh();
    List<String> before = rowMultiset(table);
    addPositionalDelete(table, dataFile1, 0L);
    List<String> surviving = rowMultiset(table);
    assertEquals("delete removes exactly one row", before.size() - 1, surviving.size());

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(
            RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build())
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    // Exact multiset, not just count: proves the surviving rows (and only those) are what remain.
    assertEquals("exact surviving rows preserved", surviving, rowMultiset(reloaded));
    assertEquals("rewrite must produce a REPLACE snapshot", 1, replaceSnapshots(reloaded));
  }

  @Test
  public void endToEndPartitionedPreservesPartitions() throws Exception {
    // Partitioned rewrite through the assembled pipeline: rows stay in their partition and none are
    // resurrected into the wrong (or a null) partition.
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();
    tableId = TableIdentifier.of("default", "part_e2e_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema, spec);

    AppendFiles append = table.newAppend();
    int recordsPerFile = 3;
    long expectedRows = 0;
    for (int shard = 0; shard < 2; shard++) {
      Record partition = GenericRecord.create(spec.partitionType());
      partition.setField("shard", shard);
      for (int f = 0; f < 2; f++) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < recordsPerFile; i++) {
          Record r = GenericRecord.create(schema);
          r.setField("id", (long) (shard * 1000 + f * 100 + i));
          r.setField("shard", shard);
          records.add(r);
        }
        expectedRows += records.size();
        append.appendFile(
            warehouse.writeRecords(
                "p" + shard + "_" + f + "_" + System.nanoTime() + ".parquet",
                schema,
                spec,
                partition,
                records));
      }
    }
    append.commit();
    table.refresh();

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(
            RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build())
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    long shard0 = 0;
    long shard1 = 0;
    try (CloseableIterable<Record> rows = IcebergGenerics.read(reloaded).build()) {
      for (Record r : rows) {
        int shard = (Integer) r.getField("shard");
        if (shard == 0) {
          shard0++;
        } else if (shard == 1) {
          shard1++;
        } else {
          throw new AssertionError("unexpected shard: " + shard);
        }
      }
    }
    assertEquals("total rows preserved", expectedRows, shard0 + shard1);
    assertEquals("shard 0 rows preserved", expectedRows / 2, shard0);
    assertEquals("shard 1 rows preserved", expectedRows / 2, shard1);
    assertTrue("rewrite must reduce the file count", liveDataFiles(reloaded) < 4);

    // File-level partition metadata: each rewritten file must carry the table's current spec and
    // sit in exactly one shard partition (0 or 1) — one compacted file per shard, no null/mixed
    // partitions. A full-scan row check alone wouldn't catch a mis-stamped file partition.
    Map<Integer, Integer> filesPerShard = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = reloaded.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        assertEquals(
            "file must use the table's partition spec",
            reloaded.spec().specId(),
            t.file().specId());
        int shard = t.file().partition().get(0, Integer.class);
        filesPerShard.merge(shard, 1, Integer::sum);
      }
    }
    assertEquals("one compacted file per shard", ImmutableMap.of(0, 1, 1, 1), filesPerShard);
  }

  @Test
  public void endToEndAtomic() throws Exception {
    Table table = buildTable(8);
    List<String> expectedRows = rowMultiset(table);
    assertEquals("baseline should have 8 files", 8, liveDataFiles(table));

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(RewriteDataFiles.Configuration.builder().build())
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("row multiset must be preserved", expectedRows, rowMultiset(reloaded));
    assertTrue("compaction must reduce the number of data files", liveDataFiles(reloaded) < 8);
    assertEquals(
        "the new snapshot must be a replace",
        DataOperations.REPLACE,
        reloaded.currentSnapshot().operation());
    assertEquals(
        "atomic mode must create exactly one replace snapshot", 1, replaceSnapshots(reloaded));
  }

  @Test
  public void embedsInCallerSuppliedPipeline() throws Exception {
    // The create(tableId, catalogConfig, Pipeline) overload must attach the maintenance graph to
    // the CALLER's own pipeline. Running THAT pipeline directly (not maintenance.run()) means the
    // rewrite only lands if the transform was wired into it, not into an internally-owned pipeline.
    Table table = buildTable(6);
    List<String> expectedRows = rowMultiset(table);

    Pipeline pipeline = Pipeline.create();
    IcebergMaintenance.create(tableId.toString(), catalogProps(table), pipeline)
        .rewriteDataFiles(RewriteDataFiles.Configuration.builder().build());
    pipeline.run().waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("row multiset must be preserved", expectedRows, rowMultiset(reloaded));
    assertTrue("compaction must reduce the file count", liveDataFiles(reloaded) < 6);
    assertEquals(
        "the rewrite must commit exactly one REPLACE snapshot", 1, replaceSnapshots(reloaded));
  }

  @Test
  public void writePropertiesOverrideOutputCompressionCodec() throws Exception {
    // setWriteProperties must reach the output writer: the table's own parquet codec is ZSTD, the
    // rewrite overrides it with gzip, and a committed output file's Parquet footer must show GZIP.
    // A broken wiring silently leaves the table's zstd default.
    tableId = TableIdentifier.of("default", "wp_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            tableId,
            TestFixtures.SCHEMA,
            null,
            ImmutableMap.of("write.parquet.compression-codec", "zstd"));
    AppendFiles append = table.newAppend();
    long id = 0;
    for (int f = 0; f < 4; f++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < 2; i++, id++) {
        Record r = GenericRecord.create(TestFixtures.SCHEMA);
        r.setField("id", id);
        r.setField("data", "row-" + id);
        records.add(r);
      }
      append.appendFile(
          warehouse.writeRecords(
              "wp" + f + "_" + System.nanoTime() + ".parquet", table.schema(), records));
    }
    append.commit();
    table.refresh();

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(REWRITE_ALL)
                .setWriteProperties(ImmutableMap.of("write.parquet.compression-codec", "gzip"))
                .build())
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "write property must override the table's zstd default",
        CompressionCodecName.GZIP,
        firstColumnCodec(onlyDataFile(reloaded)));
  }

  /** The Parquet compression codec of the first column chunk of the file at {@code location}. */
  private static CompressionCodecName firstColumnCodec(String location) throws IOException {
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(java.net.URI.create(location));
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                path, new org.apache.hadoop.conf.Configuration()))) {
      return reader.getFooter().getBlocks().get(0).getColumns().get(0).getCodec();
    }
  }

  @Test
  public void unknownRewriteOptionRejectedFailFast() throws Exception {
    Table table = buildTable(2);
    // An action-level option the bin-pack planner does not recognize must be rejected up front, not
    // silently ignored.
    RewriteDataFiles.Configuration cfg =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(ImmutableMap.of("remove-dangling-deletes", "true"))
            .build();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table));
    assertThrows(IllegalArgumentException.class, () -> maintenance.rewriteDataFiles(cfg));
  }

  @Test
  public void outputSpecIdOptionAcceptedThroughPublicPath() throws Exception {
    // output-spec-id is a real planner option — SizeBasedFileRewritePlanner.init() reads it — but
    // Iceberg 1.10 omits it from validOptions(), so fail-fast validation must allow it explicitly
    // or the public rewriteDataFiles(...) path throws before planning.
    Table table = buildTable(2);
    RewriteDataFiles.Configuration cfg =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of("output-spec-id", String.valueOf(table.spec().specId())))
            .build();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table));
    // Must NOT throw: output-spec-id is a valid planner option.
    maintenance.rewriteDataFiles(cfg);
  }

  @Test
  public void invalidRewriteOptionValuesRejectedFailFast() throws Exception {
    // Bad option VALUES (not just unknown keys) must be rejected at rewriteDataFiles(...) time
    // rather than deep inside the running job. Each case uses a FRESH maintenance instance:
    // validation runs during apply, so a rejected apply leaves a half-built transform on the
    // pipeline and a reused builder could not add a second task.
    Table table = buildTable(2);

    // Invalid rewrite-job-order name (RewriteJobOrder.fromName rejects it in planner.init).
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                .rewriteDataFiles(
                    RewriteDataFiles.Configuration.builder()
                        .setRewriteOptions(ImmutableMap.of("rewrite-job-order", "sideways"))
                        .build()));

    // output-spec-id that does not exist on the table.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                .rewriteDataFiles(
                    RewriteDataFiles.Configuration.builder()
                        .setRewriteOptions(ImmutableMap.of("output-spec-id", "999"))
                        .build()));

    // Invalid size (target-file-size-bytes must be > 0).
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                .rewriteDataFiles(
                    RewriteDataFiles.Configuration.builder()
                        .setRewriteOptions(ImmutableMap.of("target-file-size-bytes", "0"))
                        .build()));
  }

  @Test
  public void rewrittenFilesInheritTableMetricsProperties() throws Exception {
    // Table configured to collect NO column metrics.
    tableId = TableIdentifier.of("default", "metrics_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            tableId,
            TestFixtures.SCHEMA,
            null,
            ImmutableMap.of("write.metadata.metrics.default", "none"));
    AppendFiles append = table.newAppend();
    long id = 0;
    for (int f = 0; f < 10; f++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < 2; i++, id++) {
        Record r = GenericRecord.create(TestFixtures.SCHEMA);
        r.setField("id", id);
        r.setField("data", "row-" + id);
        records.add(r);
      }
      append.appendFile(
          warehouse.writeRecords(
              "m" + f + "_" + System.nanoTime() + ".parquet", table.schema(), records));
    }
    append.commit();
    table.refresh();

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(RewriteDataFiles.Configuration.builder().build())
        .run()
        .waitUntilFinish();

    // Every rewritten (live) data file must honor metrics=none -> no column bounds collected.
    Table reloaded = warehouse.loadTable(tableId);
    int rewrittenFiles = 0;
    try (CloseableIterable<FileScanTask> tasks = reloaded.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        rewrittenFiles++;
        assertTrue(
            "metrics=none must propagate to rewritten files (no lower bounds)",
            t.file().lowerBounds() == null || t.file().lowerBounds().isEmpty());
      }
    }
    assertTrue("expected at least one rewritten file to inspect", rewrittenFiles > 0);
  }

  @Test
  public void partialProgressMultipleSnapshots() throws Exception {
    Table table = buildTable(6);
    List<String> expectedRows = rowMultiset(table);

    // Force one-file-per-group so 6 groups form; with maxCommits=3 the round-robin key
    // (keptIndex % 3) spreads them over exactly 3 independent commits (2 parents each).
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(3)
            // Happy path: tolerate no failures so the commit count is deterministic (exactly 3).
            .setMaxFailedCommits(0)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "max-file-group-size-bytes", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(config)
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("row multiset must be preserved", expectedRows, rowMultiset(reloaded));
    long commits = replaceSnapshots(reloaded);
    // 6 single-file groups with maxCommits=3 => round-robin keys 0,1,2,0,1,2 => exactly 3 commits.
    assertEquals("partial progress must produce exactly 3 commits", 3, commits);
  }

  @Test
  public void rewriteFailuresDoNotCountAsCommitFailures() throws Exception {
    // A failed REWRITE is reported in failedRewriteParents, NEVER charged to the commit budget.
    // Poison one file so its parent fails to rewrite: even with maxFailedCommits=0 the run
    // SUCCEEDS, the healthy files compact, and the result separates the rewrite failure from the
    // zero commit failures.
    Table table = buildTable(6);
    String poisoned;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      poisoned = tasks.iterator().next().file().location().toString();
    }
    table.io().deleteFile(poisoned);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(6)
            .setMaxFailedCommits(0) // no commit-stage failures tolerated
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "max-file-group-size-bytes", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();

    Pipeline pipeline = Pipeline.create();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table), pipeline);
    maintenance.rewriteDataFiles(config);
    PAssert.thatSingleton(maintenance.rewriteResult())
        .satisfies(
            r -> {
              assertEquals("one rewrite failure reported", 1L, r.getFailedRewriteParents());
              assertEquals("no commit failures", 0L, r.getFailedCommits());
              assertTrue("healthy files still committed", r.getCommittedSnapshots() >= 1);
              return null;
            });
    pipeline.run().waitUntilFinish(); // must NOT throw: a rewrite failure is not a commit failure

    // The healthy single-file groups still committed (the poisoned group was skipped). We avoid
    // reading row data here because the poisoned file's bytes are gone and it is still referenced.
    Table reloaded = warehouse.loadTable(tableId);
    assertTrue(
        "healthy groups must still commit despite one rewrite-group failure",
        replaceSnapshots(reloaded) >= 1);
  }

  @Test
  public void atomicRewriteFailureCleansUpSiblingsAndFails() throws Exception {
    // Atomic mode is all-or-nothing: if one group fails to rewrite, the successful siblings' output
    // files must be DELETED, not leaked as orphans, and the job must fail. Poison one file so its
    // single-file group fails while the others succeed.
    Table table = buildTable(6);
    String poisoned;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      poisoned = tasks.iterator().next().file().location().toString();
    }
    table.io().deleteFile(poisoned);
    int parquetBefore = countParquetFilesOnDisk(table);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "max-file-group-size-bytes", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build(); // atomic (default)

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                    .rewriteDataFiles(config)
                    .run()
                    .waitUntilFinish());
    // Pin the ACTUAL rewrite-group abort cause, not merely that something threw.
    assertTrue(
        "the failure must describe the rewrite-group abort: " + causeChainMessage(ex),
        causeChainMessage(ex).contains("could not be rewritten"));

    // The successful siblings' freshly written outputs were cleaned up: no new parquet files.
    assertEquals(
        "an aborted atomic rewrite must leave no orphan output files on disk",
        parquetBefore,
        countParquetFilesOnDisk(table));
  }

  @Test
  public void atomicRewriteAllGroupsFailingFails() throws Exception {
    // If EVERY group fails to rewrite, no commit batch exists for the gate to abort, so a
    // safety-net assert must still fail the job rather than silently succeed.
    Table table = buildTable(3);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        table.io().deleteFile(t.file().location().toString());
      }
    }
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(ImmutableMap.of("min-input-files", "1"))
            .build();

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                    .rewriteDataFiles(config)
                    .run()
                    .waitUntilFinish());
    // Pin the all-groups-failed safety net (AssertAtomicRewriteProgressed), not any crash.
    assertTrue(
        "the failure must be the all-groups-failed safety net: " + causeChainMessage(ex),
        causeChainMessage(ex).contains("failed to rewrite and nothing was committed"));
  }

  @Test
  public void rewriteFailuresAreToleratedAndReportedInResult() throws Exception {
    // Partial-progress runs tolerate ANY number of rewrite-group failures (no budget) and report
    // them in the result — Spark parity. Poison 2 single-file parents: the run SUCCEEDS, healthy
    // parents still commit, the poisoned inputs stay live, and the result counts 2 failed parents.
    Table table = buildTable(6);
    List<String> poisoned = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      java.util.Iterator<FileScanTask> it = tasks.iterator();
      for (int i = 0; i < 2 && it.hasNext(); i++) {
        poisoned.add(it.next().file().location().toString());
      }
    }
    for (String p : poisoned) {
      table.io().deleteFile(p); // unreadable input -> that parent fails to rewrite
    }

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(1)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "max-file-group-size-bytes", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();

    Pipeline pipeline = Pipeline.create();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table), pipeline);
    maintenance.rewriteDataFiles(config);
    PAssert.thatSingleton(maintenance.rewriteResult())
        .satisfies(
            r -> {
              assertEquals(
                  "both poisoned parents reported failed", 2L, r.getFailedRewriteParents());
              assertTrue("the healthy parents committed", r.getCommittedSnapshots() >= 1);
              return null;
            });
    pipeline
        .run()
        .waitUntilFinish(); // must NOT throw: rewrite failures are tolerated, not budgeted

    Table reloaded = warehouse.loadTable(tableId);
    assertTrue("healthy parents committed a REPLACE", replaceSnapshots(reloaded) >= 1);
    Set<String> live = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = reloaded.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        live.add(t.file().location().toString());
      }
    }
    for (String p : poisoned) {
      assertTrue("a poisoned parent's input stays live: " + p, live.contains(p));
    }
  }

  @Test
  public void commitFailureWithinBudgetTolerated() throws Exception {
    // A terminal commit failure under partial progress is charged to maxFailedCommits (rewrite
    // failures are tolerated separately, never budgeted). With maxCommits=6 the injected failure
    // isolates to one key, so with maxFailedCommits=1 the run must SUCCEED and the other five keys
    // must still commit a REPLACE snapshot.
    Table table = buildTable(6);
    long startingSnapshot = table.currentSnapshot().snapshotId();
    DataFile victim;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      victim = tasks.iterator().next().file();
    }
    // Metadata-only delete AFTER the pinned planning snapshot: still planned + rewritten (bytes
    // present), but its commit can no longer delete it.
    table.newDelete().deleteFile(victim).commit();
    table.refresh();

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setSnapshotId(startingSnapshot)
            .setPartialProgressEnabled(true)
            .setMaxCommits(6)
            .setMaxFailedCommits(1)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "max-file-group-size-bytes", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();

    IcebergMaintenance.create(tableId.toString(), catalogProps(table))
        .rewriteDataFiles(config)
        .run()
        .waitUntilFinish();

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "the five healthy keys must each commit a REPLACE snapshot", 5, replaceSnapshots(reloaded));
  }

  @Test
  public void commitFailureExceedingBudgetFailsPipeline() throws Exception {
    // The same single commit failure with maxFailedCommits=0 must FAIL the pipeline with a message
    // naming the commit budget.
    Table table = buildTable(6);
    long startingSnapshot = table.currentSnapshot().snapshotId();
    DataFile victim;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      victim = tasks.iterator().next().file();
    }
    table.newDelete().deleteFile(victim).commit();
    table.refresh();

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setSnapshotId(startingSnapshot)
            .setPartialProgressEnabled(true)
            .setMaxCommits(6)
            .setMaxFailedCommits(0)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "max-file-group-size-bytes", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                IcebergMaintenance.create(tableId.toString(), catalogProps(table))
                    .rewriteDataFiles(config)
                    .run()
                    .waitUntilFinish());
    assertTrue(
        "failure must name the exceeded commit budget: " + causeChainMessage(ex),
        causeChainMessage(ex).contains("commit"));
  }

  @Test
  public void multiSubgroupParentFailureCountsOnceInResult() throws Exception {
    // The result counts PLANNED PARENT GROUPS, not subgroups: one multi-row-group file split into
    // several range bins is ONE parent, so poisoning its bytes must report failedRewriteParents=1
    // (a per-subgroup count would report >=2 here) while the partial-progress run still SUCCEEDS.
    Table table = buildMultiRowGroupTable(1500);
    assertRowGroupsAtLeast(table, 3);
    long target = totalDataFileBytes(table) / 3; // split the single file into several bins
    String theFile = onlyDataFile(table);
    // Poison the file's bytes so every subgroup's read fails; metadata stays intact so planning
    // still splits it into multiple bins.
    table.io().deleteFile(theFile);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(1)
            .setMaxFailedCommits(0)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1", "target-file-size-bytes", String.valueOf(target)))
            .build();

    // Fixture precondition: the parent must actually split into >=2 subgroup bins, else the
    // per-parent (vs per-subgroup) assertion below passes vacuously if the packer regresses to one
    // bin. Plan only (metadata survives the poisoning) and count this parent's emitted subgroups.
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    assertTrue(
        "fixture must split the parent into >=2 subgroups (else the per-parent count is vacuous)",
        planTester.peekOutputElements(PlanRewriteGroups.GROUPS).size() >= 2);

    Pipeline pipeline = Pipeline.create();
    IcebergMaintenance maintenance =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table), pipeline);
    maintenance.rewriteDataFiles(config);
    PAssert.thatSingleton(maintenance.rewriteResult())
        .satisfies(
            r -> {
              assertEquals(
                  "all bins of one parent count as ONE failed parent",
                  1L,
                  r.getFailedRewriteParents());
              return null;
            });
    pipeline.run().waitUntilFinish(); // must NOT throw
  }

  @Test
  public void unboundedInputRejectedAtConstruction() throws Exception {
    // The transform's global-window GroupByKey + singleton side input never fire on unbounded
    // input, so the pipeline would hang committing nothing. Reject unbounded input at construction
    // time with a clear message instead.
    Table table = buildTable(2);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder().setCatalogProperties(catalogProps(table)).build();
    SnapshotInfo snapshotInfo = SnapshotInfo.fromSnapshot(table.currentSnapshot());

    Pipeline pipeline = Pipeline.create();
    PCollection<SnapshotInfo> unbounded =
        pipeline
            .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
            .apply(
                MapElements.into(TypeDescriptor.of(SnapshotInfo.class))
                    .via((Long x) -> snapshotInfo))
            .setCoder(RewriteDataFiles.SNAPSHOT_CODER);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                unbounded.apply(
                    RewriteDataFiles.create(
                        tableId.toString(),
                        st,
                        catalogConfig,
                        RewriteDataFiles.Configuration.builder().build())));
    assertTrue(
        "message must explain the bounded-only restriction: " + ex.getMessage(),
        ex.getMessage().contains("bounded"));
  }

  @Test
  public void sameSpecPartitionedCompactionOpensOneWriterPerSubgroup() throws Exception {
    // Planning groups files by partition, so a same-spec subgroup only ever sees one partition
    // value and holds a single open appender. Two shards, one subgroup each -> counter == 2.
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();
    tableId = TableIdentifier.of("default", "onewriter_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema, spec);
    AppendFiles append = table.newAppend();
    for (int shard = 0; shard < 2; shard++) {
      Record partition = GenericRecord.create(spec.partitionType());
      partition.setField("shard", shard);
      for (int f = 0; f < 2; f++) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
          Record r = GenericRecord.create(schema);
          r.setField("id", (long) (shard * 100 + f * 10 + i));
          r.setField("shard", shard);
          records.add(r);
        }
        append.appendFile(
            warehouse.writeRecords(
                "ow" + shard + "_" + f + "_" + System.nanoTime() + ".parquet",
                schema,
                spec,
                partition,
                records));
      }
    }
    append.commit();
    table.refresh();

    PipelineResult result =
        IcebergMaintenance.create(tableId.toString(), catalogProps(table))
            .rewriteDataFiles(
                RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build())
            .run();
    result.waitUntilFinish();

    long openWriters = 0;
    for (MetricResult<Long> c :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(WriterFactory.class, "openFanoutWriters"))
                    .build())
            .getCounters()) {
      openWriters += c.getAttempted();
    }
    assertEquals(
        "each same-spec subgroup opens exactly one writer (2 shards -> 2)", 2L, openWriters);
  }

  @Test
  public void deleteOrphansUsesOneBulkCallWhenSupported() {
    // An atomic abort can face tens of thousands of orphans; when the FileIO supports bulk ops,
    // delete them in ONE call, not N serial round-trips.
    List<String> paths = Arrays.asList("a.parquet", "b.parquet", "c.parquet");
    BulkFakeIO io = new BulkFakeIO();
    assertEquals(3, RewriteDataFiles.deleteOrphans(io, paths));
    assertEquals("exactly one bulk delete call", 1, io.bulkCalls);
    assertEquals(new HashSet<>(paths), io.deleted);
  }

  @Test
  public void deleteOrphansFallsBackToPerFileWhenNotBulk() {
    // A FileIO without bulk support falls back to per-file deletes.
    List<String> paths = Arrays.asList("a.parquet", "b.parquet", "c.parquet");
    PlainFakeIO io = new PlainFakeIO();
    assertEquals(3, RewriteDataFiles.deleteOrphans(io, paths));
    assertEquals(3, io.singleDeletes);
  }

  @Test
  public void deleteOrphansBulkFailureReportsPartialCountAndDoesNotThrow() {
    // A partial bulk failure must NOT throw — the caller fails the pipeline regardless and leaves
    // the rest as tagged orphans — but it reports how many were deleted.
    List<String> paths = Arrays.asList("a.parquet", "b.parquet", "c.parquet");
    assertEquals(1, RewriteDataFiles.deleteOrphans(new BulkFailIO(2), paths));
  }

  @Test
  public void deleteOrphansNonBulkFailureFallsBackToPerFile() {
    // A bulk delete failure that is NOT a BulkDeletionFailureException (an unwrapped S3 SDK
    // auth/throttle/shutdown RuntimeException) must fall back to per-file deletes, not propagate a
    // raw IO stack out of CleanupAndFail.
    List<String> paths = Arrays.asList("a.parquet", "b.parquet", "c.parquet");
    BulkRuntimeFailIO io = new BulkRuntimeFailIO();
    assertEquals(3, RewriteDataFiles.deleteOrphans(io, paths));
    assertEquals("must fall back to per-file deletes", 3, io.singleDeletes);
  }

  private static class PlainFakeIO implements FileIO {
    int singleDeletes = 0;

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      singleDeletes++;
    }
  }

  private static class BulkFakeIO implements FileIO, SupportsBulkOperations {
    int bulkCalls = 0;
    final Set<String> deleted = new HashSet<>();

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new AssertionError("bulk-capable FileIO must not use per-file delete");
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) {
      bulkCalls++;
      pathsToDelete.forEach(deleted::add);
    }
  }

  private static class BulkFailIO implements FileIO, SupportsBulkOperations {
    private final int failCount;

    BulkFailIO(int failCount) {
      this.failCount = failCount;
    }

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new AssertionError("bulk-capable FileIO must not use per-file delete");
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) {
      throw new BulkDeletionFailureException(failCount);
    }
  }

  private static class BulkRuntimeFailIO implements FileIO, SupportsBulkOperations {
    int singleDeletes = 0;

    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      singleDeletes++;
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) {
      throw new RuntimeException("simulated non-Bulk S3 client failure");
    }
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

  /** Counts {@code .parquet} files physically present under the table's location. */
  private int countParquetFilesOnDisk(Table table) throws Exception {
    String loc = table.location();
    java.nio.file.Path root =
        loc.startsWith("file:")
            ? java.nio.file.Paths.get(java.net.URI.create(loc))
            : java.nio.file.Paths.get(loc);
    try (java.util.stream.Stream<java.nio.file.Path> walk = java.nio.file.Files.walk(root)) {
      return (int) walk.filter(p -> p.toString().endsWith(".parquet")).count();
    }
  }
}
