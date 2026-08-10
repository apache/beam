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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end correctness tests for the rewrite that need fine-grained control over the table state
 * between planning and committing (MOR deletes, concurrency, conflicts). These drive the production
 * DoFns through {@link DoFnTester} so a conflicting operation can be injected after the rewrite is
 * planned but before it is committed.
 */
@RunWith(JUnit4.class)
public class RewriteDataFilesCorrectnessTest {
  private static final Map<String, String> REWRITE_ALL = ImmutableMap.of("min-input-files", "1");

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private TableIdentifier tableId;

  private Table buildTable(int numFiles) throws Exception {
    tableId = TableIdentifier.of("default", "correctness_" + System.nanoTime());
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

  private IcebergCatalogConfig catalogConfig(Table table) {
    String tableLocation = table.location();
    String suffix = "/" + tableId.namespace().toString() + "/" + tableId.name();
    assertTrue("Unexpected table location: " + tableLocation, tableLocation.endsWith(suffix));
    String warehouseRoot = tableLocation.substring(0, tableLocation.length() - suffix.length());
    return IcebergCatalogConfig.builder()
        .setCatalogName("hadoop")
        .setCatalogProperties(
            ImmutableMap.of(
                "type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, "warehouse", warehouseRoot))
        .build();
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

  /** The table's live rows as a sorted (id|data) multiset, so drop/duplicate/swap is detectable. */
  private static List<String> rowMultiset(Table table) {
    List<String> keys = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record r : rows) {
        keys.add(r.getField("id") + "|" + r.getField("data"));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Collections.sort(keys);
    return keys;
  }

  /**
   * The live rows of the (id, shard) partitioned fixture as a sorted (id|shard) multiset — the
   * shard-schema analogue of {@link #rowMultiset}, so a dropped/duplicated/misplaced row is caught
   * (not just a count change).
   */
  private static List<String> idShardMultiset(Table table) {
    List<String> keys = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record r : rows) {
        keys.add(r.getField("id") + "|" + r.getField("shard"));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Collections.sort(keys);
    return keys;
  }

  /** Live data-file paths in the table. */
  private static Set<String> liveDataFilePaths(Table table) throws Exception {
    Set<String> paths = new HashSet<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        paths.add(t.file().location().toString());
      }
    }
    return paths;
  }

  /**
   * Creates a table whose rows are all distinct (id N -> data "row-N"), with {@code recordsPerFile}
   * rows per file across {@code numFiles} files. Distinct payloads let the multiset comparison
   * catch dropped/duplicated/swapped rows that an identical-record fixture would hide.
   */
  private Table buildDistinctTable(int numFiles, int recordsPerFile) throws Exception {
    tableId = TableIdentifier.of("default", "distinct_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    AppendFiles append = table.newAppend();
    long id = 0;
    for (int f = 0; f < numFiles; f++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < recordsPerFile; i++, id++) {
        Record r = GenericRecord.create(TestFixtures.SCHEMA);
        r.setField("id", id);
        r.setField("data", "row-" + id);
        records.add(r);
      }
      append.appendFile(
          warehouse.writeRecords(
              "d" + f + "_" + System.nanoTime() + ".parquet", table.schema(), records));
    }
    append.commit();
    table.refresh();
    return table;
  }

  /**
   * Parquet writer properties that force many small row groups from little data, so a file records
   * several {@code splitOffsets} and {@code task.split()} yields one range task per row group.
   * Three things fight this at unit scale: dictionary encoding buffers values so the writer's
   * getBufferedSize() under-reports; the default 1 MB page never completes so getBufferedSize()
   * stays ~0; and the row-group-size check interval otherwise jumps past the whole file. Also
   * writes uncompressed so on-disk sizes (what the planner selects on) match the uncompressed sizes
   * the rolling writer measures when deciding to roll.
   */
  private static Map<String, String> multiRowGroupWriterProperties(long rowGroupSizeBytes) {
    return ImmutableMap.<String, String>builder()
        .put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(rowGroupSizeBytes))
        .put("parquet.enable.dictionary", "false")
        .put(TableProperties.PARQUET_PAGE_SIZE_BYTES, "1024")
        .put(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "100")
        .put(TableProperties.PARQUET_COMPRESSION, "uncompressed")
        .build();
  }

  /**
   * Builds a table of {@code numFiles} files, each holding {@code recordsPerFile} all-distinct rows
   * and written with a tiny row-group size so every file spans multiple row groups. Distinct
   * payloads let the multiset comparison catch dropped/duplicated/swapped rows.
   */
  private Table buildDistinctMultiRowGroupTable(
      int numFiles, int recordsPerFile, long rowGroupSizeBytes) throws Exception {
    tableId = TableIdentifier.of("default", "mrg_" + System.nanoTime());
    // Write uncompressed so on-disk (planner-visible) byte sizes match the uncompressed sizes the
    // rolling writer measures when deciding to roll. Under the default codec the writer rolls on
    // uncompressed buffered size (~writeMax) but the planner selects on the much smaller compressed
    // size, so outputs would land far below minFileSize and never converge — an artifact of the
    // fixture, not the packer. The rewrite output inherits this table property.
    Table table =
        warehouse.createTable(
            tableId,
            TestFixtures.SCHEMA,
            null,
            ImmutableMap.of(TableProperties.PARQUET_COMPRESSION, "uncompressed"));
    AppendFiles append = table.newAppend();
    long id = 0;
    for (int f = 0; f < numFiles; f++) {
      List<Record> records = new ArrayList<>(recordsPerFile);
      for (int i = 0; i < recordsPerFile; i++, id++) {
        Record r = GenericRecord.create(TestFixtures.SCHEMA);
        r.setField("id", id);
        // A wide, per-row-distinct payload so files reach a size where the 5 KB planner split
        // overhead is negligible (needed for the convergence math) and rows stay distinguishable.
        r.setField("data", "compaction-payload-row-" + id + "-" + (id * 2654435761L));
        records.add(r);
      }
      append.appendFile(
          warehouse.writeRecords(
              "mrg" + f + "_" + System.nanoTime() + ".parquet",
              table.schema(),
              records,
              multiRowGroupWriterProperties(rowGroupSizeBytes)));
    }
    append.commit();
    table.refresh();
    return table;
  }

  /** Sum of the byte sizes of the table's live data files. */
  private static long totalDataFileBytes(Table table) throws Exception {
    long total = 0;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        total += t.file().fileSizeInBytes();
      }
    }
    return total;
  }

  /**
   * Asserts every live data file spans at least {@code min} row groups (has >= min splitOffsets).
   */
  private static void assertRowGroupsAtLeast(Table table, int min) throws Exception {
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        List<Long> offsets = t.file().splitOffsets();
        int rowGroups = offsets == null ? 1 : offsets.size();
        assertTrue(
            "fixture must have multi-row-group files (else nothing splits); got "
                + rowGroups
                + " row group(s) for "
                + t.file().location(),
            rowGroups >= min);
      }
    }
  }

  /** Runs only the planning DoFn and returns the emitted rewrite groups (for fixpoint checks). */
  private List<KV<Integer, RewriteSubGroup>> planOnly(
      Table table, RewriteDataFiles.Configuration config) throws Exception {
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    return planTester.peekOutputElements(PlanRewriteGroups.GROUPS);
  }

  /** Plans + rewrites the table into ExecutedGroup batches, keyed by commit key. */
  private List<KV<Integer, Iterable<ExecutedGroup>>> planAndRewrite(
      Table table, RewriteDataFiles.Configuration config) throws Exception {
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    List<KV<Integer, RewriteSubGroup>> planned =
        planTester.peekOutputElements(PlanRewriteGroups.GROUPS);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> rewriteTester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    rewriteTester.processBundle(planned);
    List<KV<Integer, ExecutedGroup>> executed =
        rewriteTester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN);

    Map<Integer, List<ExecutedGroup>> byKey = new LinkedHashMap<>();
    for (KV<Integer, ExecutedGroup> kv : executed) {
      byKey.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv.getValue());
    }
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = new ArrayList<>();
    for (Map.Entry<Integer, List<ExecutedGroup>> e : byKey.entrySet()) {
      batches.add(KV.of(e.getKey(), (Iterable<ExecutedGroup>) e.getValue()));
    }
    return batches;
  }

  private List<SnapshotInfo> commit(
      Table table,
      List<KV<Integer, Iterable<ExecutedGroup>>> batches,
      RewriteDataFiles.Configuration config)
      throws Exception {
    return commit(table, batches, config, /* requireCommit= */ true);
  }

  private List<SnapshotInfo> commit(
      Table table,
      List<KV<Integer, Iterable<ExecutedGroup>>> batches,
      RewriteDataFiles.Configuration config,
      boolean requireCommit)
      throws Exception {
    IcebergCatalogConfig cc = catalogConfig(table);
    List<SnapshotInfo> committed = new ArrayList<>();
    for (KV<Integer, Iterable<ExecutedGroup>> batch : batches) {
      DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
          DoFnTester.of(new CommitRewriteGroups(tableId.toString(), cc, config));
      tester.processBundle(batch);
      committed.addAll(tester.peekOutputElements(CommitRewriteGroups.COMMITTED));
    }
    // F23: guard against a vacuous pass. These tests assert row multisets that hold with OR without
    // the rewrite, so if the commit silently fails (e.g. validation rejects it because sequence-
    // number preservation regressed, and partial progress routes it aside) the leftover pre-rewrite
    // table would satisfy them. CommitRewriteGroups only emits COMMITTED on a successful
    // RewriteFiles
    // (replace) commit, so requiring a non-empty result proves the rewrite actually landed. (Tests
    // that deliberately exercise a NOT-committed case — e.g. an incomplete parent group — pass
    // requireCommit=false.)
    if (requireCommit) {
      assertFalse(
          "the rewrite must actually commit — a silently-failed commit leaves the pre-rewrite "
              + "table, which passes the same row assertions",
          committed.isEmpty());
    }
    return committed;
  }

  /** Live data files in partition {@code shard} as a {@code location -> fileSizeInBytes} map. */
  private static Map<String, Long> filesInShard(Table table, int shard) throws Exception {
    Map<String, Long> files = new LinkedHashMap<>();
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        if (((Integer) t.file().partition().get(0, Integer.class)) == shard) {
          files.put(t.file().location().toString(), t.file().fileSizeInBytes());
        }
      }
    }
    return files;
  }

  /** Writes a positional delete for {@code position} in {@code dataFile} and commits it. */
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

  /**
   * A merge-on-read table whose data file has a positional delete must, after rewrite, contain only
   * the surviving rows: the rewrite reads through the delete filter and the new file excludes the
   * deleted row.
   */
  @Test
  public void morDeletesAppliedOnRewrite() throws Exception {
    tableId = TableIdentifier.of("default", "mor_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    // Two data files so the bin-pack group qualifies for rewrite (Iceberg requires >1 input file);
    // the positional delete targets the first file.
    DataFile dataFile1 =
        warehouse.writeRecords("mor1.parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1);
    DataFile dataFile2 =
        warehouse.writeRecords("mor2.parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
    table.refresh();
    long rowsBefore = countRows(table);
    assertTrue("fixture needs at least 2 rows", rowsBefore >= 2);

    // Delete the first row of the first file via a positional delete (MOR), then rewrite.
    addPositionalDelete(table, dataFile1, 0L);
    assertEquals("delete must remove exactly one row", rowsBefore - 1, countRows(table));

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertTrue("file with a delete must be planned for rewrite", !batches.isEmpty());
    commit(table, batches, config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "rewritten table must contain only the surviving rows",
        rowsBefore - 1,
        countRows(reloaded));
  }

  /**
   * An append committed to the table after the rewrite was planned (but before it is committed) is
   * a non-conflicting concurrent operation: its rows must survive the rewrite commit.
   */
  @Test
  public void concurrentAppendNotClobbered() throws Exception {
    Table table = buildTable(4);
    long rowsBefore = countRows(table);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    // Concurrent, non-conflicting append between plan and commit.
    DataFile extra =
        warehouse.writeRecords("extra.parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1);
    table.newAppend().appendFile(extra).commit();
    table.refresh();
    long appendedRows = countRows(table) - rowsBefore;
    assertTrue("concurrent append must add rows", appendedRows > 0);

    commit(table, batches, config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "rewritten rows plus concurrently-appended rows must all survive",
        rowsBefore + appendedRows,
        countRows(reloaded));
  }

  /**
   * If a concurrent operation removes one of the data files the rewrite is replacing, the validated
   * atomic commit must detect the conflict, FAIL (throw) after its bounded retries, and commit
   * nothing. Crucially it must NOT delete this batch's output files: a retried or concurrent
   * (zombie) attempt of the same commit element could still commit them, so deleting addable files
   * would risk a snapshot referencing missing data. The orphaned outputs are left tagged for a
   * later remove-orphan-files run.
   */
  @Test
  public void atomicConflictThrowsAndRetainsOutputFiles() throws Exception {
    Table table = buildTable(4);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    // Collect this batch's output file paths before the (failing) commit.
    List<String> outputs = new ArrayList<>();
    for (ExecutedGroup g : batches.get(0).getValue()) {
      for (SerializableDataFile sdf : g.getNewFiles()) {
        outputs.add(sdf.getPath());
      }
    }
    assertFalse("sanity: the batch produced output files", outputs.isEmpty());

    // Concurrently delete one of the data files the rewrite is about to replace -> hard conflict.
    DataFile victim =
        batches
            .get(0)
            .getValue()
            .iterator()
            .next()
            .getRewrittenDataFiles()
            .get(0)
            .createDataFile(table.specs());
    table.newDelete().deleteFile(victim).commit();
    table.refresh();

    IcebergCatalogConfig cc = catalogConfig(table);
    DoFnTester<KV<Integer, Iterable<ExecutedGroup>>, SnapshotInfo> tester =
        DoFnTester.of(new CommitRewriteGroups(tableId.toString(), cc, config));

    // The atomic conflict must FAIL the pipeline (throw) after retries and commit nothing.
    assertThrows(Exception.class, () -> tester.processBundle(batches.get(0)));
    assertTrue(
        "a conflicting atomic commit must not produce a committed snapshot",
        tester.peekOutputElements(CommitRewriteGroups.COMMITTED).isEmpty());

    // The output files must be retained (not deleted), so a retry can still commit them.
    for (String p : outputs) {
      assertTrue(
          "a conflicted atomic commit must NOT delete its output files: " + p,
          table.io().newInputFile(p).exists());
    }
  }

  /**
   * The rewrite-failure cleanup stage must delete the orphan output files it is handed (the
   * successful siblings' outputs of an atomic rewrite where another group failed to rewrite) and
   * then fail the pipeline. Deleting here is safe: those files were never handed to a commit.
   * Re-deleting an already-deleted path is a no-op, so the stage is safe to retry.
   */
  @Test
  public void cleanupAndFailDeletesOrphansAndFails() throws Exception {
    Table table = buildTable(2);
    // Two real files on disk that are not referenced by the table — i.e. orphans.
    DataFile o1 =
        warehouse.writeRecords(
            "orphan1_" + System.nanoTime() + ".parquet",
            table.schema(),
            TestFixtures.FILE1SNAPSHOT1);
    DataFile o2 =
        warehouse.writeRecords(
            "orphan2_" + System.nanoTime() + ".parquet",
            table.schema(),
            TestFixtures.FILE1SNAPSHOT1);
    List<String> orphans = Arrays.asList(o1.location().toString(), o2.location().toString());
    for (String p : orphans) {
      assertTrue("orphan must exist before cleanup: " + p, table.io().newInputFile(p).exists());
    }

    IcebergCatalogConfig cc = catalogConfig(table);
    DoFnTester<KV<Integer, List<String>>, Void> tester =
        DoFnTester.of(new RewriteDataFiles.CleanupAndFail(tableId.toString(), cc));

    assertThrows(RuntimeException.class, () -> tester.processBundle(KV.of(0, orphans)));

    for (String p : orphans) {
      assertFalse(
          "orphan must be deleted by the cleanup stage: " + p, table.io().newInputFile(p).exists());
    }
  }

  /**
   * The rewrite-failure cleanup stage's failure message must describe a rewrite failure (not a
   * commit conflict), so operators are pointed at the right root cause.
   */
  @Test
  public void cleanupErrorMessageDescribesRewriteFailure() throws Exception {
    Table table = buildTable(2);
    IcebergCatalogConfig cc = catalogConfig(table);

    DoFnTester<KV<Integer, List<String>>, Void> rewriteTester =
        DoFnTester.of(new RewriteDataFiles.CleanupAndFail(tableId.toString(), cc));
    RuntimeException rewriteEx =
        assertThrows(
            RuntimeException.class,
            () -> rewriteTester.processBundle(KV.of(0, Collections.<String>emptyList())));
    assertTrue(
        "rewrite-stage error must describe the rewrite failure: " + rewriteEx.getMessage(),
        rewriteEx.getMessage().contains("could not be rewritten"));
    assertFalse(
        "rewrite-stage error must NOT blame a commit conflict: " + rewriteEx.getMessage(),
        rewriteEx.getMessage().contains("conflicted with a concurrent"));
  }

  /**
   * End-to-end guard for the original data-loss bug: with {@code max-files-to-rewrite=3} on a
   * 5-file table, the commit must delete EXACTLY the 3 rewritten files (not all 5), keep the other
   * 2 untouched, and preserve the full row multiset. The old bug deleted files it never rewrote — a
   * planner-only assertion (only 3 planned) would not have caught the bad delete-set.
   */
  @Test
  public void maxFilesToRewriteDeletesOnlyRewrittenFiles() throws Exception {
    Table table = buildDistinctTable(5, 2); // 5 files, 10 distinct rows
    List<String> expectedRows = rowMultiset(table);
    Set<String> originalPaths = liveDataFilePaths(table);
    assertEquals(5, originalPaths.size());

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(ImmutableMap.of("min-input-files", "1", "max-files-to-rewrite", "3"))
            .build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    // Capture exactly which input files the plan scheduled for rewrite (and thus deletion).
    Set<String> scheduledForDeletion = new HashSet<>();
    for (KV<Integer, Iterable<ExecutedGroup>> batch : batches) {
      for (ExecutedGroup eg : batch.getValue()) {
        eg.getRewrittenDataFiles()
            .forEach(sdf -> scheduledForDeletion.add(sdf.createDataFile(table.specs()).location()));
      }
    }
    assertEquals("exactly 3 files must be scheduled for rewrite", 3, scheduledForDeletion.size());

    commit(table, batches, config);
    Table reloaded = warehouse.loadTable(tableId);

    // No rows lost, duplicated, or swapped.
    assertEquals("full row multiset must be preserved", expectedRows, rowMultiset(reloaded));

    Set<String> livePaths = liveDataFilePaths(reloaded);
    // The 3 rewritten files are gone; the 2 un-rewritten originals are still present verbatim.
    for (String p : scheduledForDeletion) {
      assertTrue("rewritten file must be removed: " + p, !livePaths.contains(p));
    }
    for (String p : originalPaths) {
      if (!scheduledForDeletion.contains(p)) {
        assertTrue("un-rewritten original file must remain: " + p, livePaths.contains(p));
      }
    }
  }

  /**
   * When a parent group is packed into several sub-groups (tiny target makes each whole file its
   * own sub-group), the rewrite across those sub-groups must still preserve every row and commit
   * cleanly as a single batch.
   */
  @Test
  public void splitRewritePreservesRowsAndDedupsDeletes() throws Exception {
    Table table = buildDistinctTable(2, 3); // 2 files, 6 distinct rows
    List<String> expected = rowMultiset(table);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();
    commit(table, planAndRewrite(table, config), config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "row multiset must be preserved through a split rewrite", expected, rowMultiset(reloaded));
  }

  /**
   * F4-C R3 — convergence + fixpoint. This is the end-to-end regression for the non-convergence bug
   * that whole-file subgroup packing causes and that row-group range packing fixes.
   *
   * <p>The fixture is the F4 band: files each below {@code min-file-size} (so they are re-selected
   * for rewrite) yet larger than half the input split size (so <b>whole-file</b> packing can never
   * combine two of them into one bin). Whole-file packing therefore rewrites each file 1:1 into
   * another below-min file — the file count never drops and a second planning pass keeps selecting
   * the same files forever. Row-group range packing splits each multi-row-group file into its row
   * groups and repacks those ranges <b>across file boundaries</b> into target-sized bins, so the
   * files compact into fewer, in-band outputs.
   *
   * <p>Converged is defined operationally: after one rewrite the outputs land within {@code
   * [minFileSize, writeMaxFileSize]} and a SECOND planning pass over the compacted table plans
   * nothing. The whole-file packer fails both halves; the range packer satisfies both.
   */
  @Test
  public void byteRangePackingConvergesAndReachesFixpoint() throws Exception {
    int numFiles = 5;
    int recordsPerFile = 1500;
    long rowGroupSizeBytes = 8192;
    Table table = buildDistinctMultiRowGroupTable(numFiles, recordsPerFile, rowGroupSizeBytes);
    List<String> expectedRows = rowMultiset(table);
    Set<String> originalPaths = liveDataFilePaths(table);
    assertEquals(numFiles, originalPaths.size());
    // Every file must genuinely span several row groups, otherwise nothing splits and the test
    // could not tell whole-file packing from range packing.
    assertRowGroupsAtLeast(table, 3);

    // target = totalBytes / 3 puts each of the 5 (roughly equal) files at ~0.2*total = 0.6*target,
    // which is (a) below minFileSize = 0.75*target -> selected for rewrite, and (b) above
    // inputSplitSize/2 (~0.5*target) -> two files never share a whole-file bin -> whole-file
    // packing
    // is 1:1 and non-convergent. Range packing repacks the row-group ranges into ~3 target-sized
    // files.
    long totalBytes = totalDataFileBytes(table);
    long target = totalBytes / 3;
    // Guard the convergence math: the 5 KB planner split overhead must be negligible vs the target,
    // else inputSplitSize inflates toward writeMaxFileSize and two files could pair under
    // whole-file
    // packing (which would make this test pass even for the buggy packer).
    assertTrue(
        "fixture too small; grow recordsPerFile so target dominates the 5 KB split overhead "
            + "(target="
            + target
            + ")",
        target > 60_000);

    long minFileSize = (long) (target * 0.75); // Iceberg MIN_FILE_SIZE_DEFAULT_RATIO
    long maxFileSize = (long) (target * 1.80); // Iceberg MAX_FILE_SIZE_DEFAULT_RATIO
    long writeMaxFileSize = target + (maxFileSize - target) / 2; // Iceberg writeMaxFileSize()

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(ImmutableMap.of("target-file-size-bytes", String.valueOf(target)))
            .build();

    commit(table, planAndRewrite(table, config), config);
    Table reloaded = warehouse.loadTable(tableId);

    // No data loss, duplication, or swap.
    assertEquals(
        "row multiset must be preserved through the range-packed rewrite",
        expectedRows,
        rowMultiset(reloaded));

    // Convergence: strictly fewer files than we started with. Whole-file 1:1 packing keeps all 5.
    Set<String> livePaths = liveDataFilePaths(reloaded);
    assertTrue(
        "range packing must reduce the file count below " + numFiles + " (got " + livePaths.size(),
        livePaths.size() < numFiles);

    // Every output lands in [minFileSize, writeMaxFileSize] — the target band.
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        long size = t.file().fileSizeInBytes();
        assertTrue(
            "compacted output " + size + " must be >= minFileSize " + minFileSize,
            size >= minFileSize);
        assertTrue(
            "compacted output " + size + " must be <= writeMaxFileSize " + writeMaxFileSize,
            size <= writeMaxFileSize);
      }
    }

    // Fixpoint — the actual definition of converged: a second planning pass over the compacted
    // table plans ZERO groups. The whole-file packer's 1:1 below-min outputs are re-selected here.
    assertEquals(
        "a second planning pass over the converged table must plan nothing",
        0,
        planOnly(reloaded, config).size());
  }

  /**
   * CRITICAL regression for partial-progress data loss: when one sub-group of a parent group fails
   * (routed to REWRITE_FAILURES and excluded from the committed batch), no rows may be lost.
   *
   * <p>Parent-group atomicity (F4) guarantees this: an incomplete parent (missing any sub-group) is
   * committed as a whole or not at all, so a sub-group's input files are never partially replaced.
   * Here we simulate the failure by excluding one planned sub-group from the committed batch; the
   * commit then lands NOTHING for that parent (requireCommit=false), every input file stays live,
   * and the full row multiset is intact.
   */
  @Test
  public void droppedSubGroupKeepsInputsLiveAndLosesNoRows() throws Exception {
    Table table = buildDistinctTable(3, 2); // 3 files, 6 distinct rows
    List<String> expected = rowMultiset(table);
    assertEquals(3, liveDataFilePaths(table).size());

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(1)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1",
                    "min-file-size-bytes", "0",
                    "target-file-size-bytes", "2",
                    "max-file-size-bytes", "3"))
            .build();

    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    List<KV<Integer, RewriteSubGroup>> planned =
        planTester.peekOutputElements(PlanRewriteGroups.GROUPS);
    assertTrue("config must split the parent group into >1 sub-group", planned.size() > 1);

    // Simulate one sub-group failing under partial progress: it never reaches commit. Capture the
    // input files it would have rewritten — these must remain untouched (live).
    KV<Integer, RewriteSubGroup> dropped = planned.get(0);
    Set<String> droppedInputs = new HashSet<>();
    RewriteGroupTestHelpers.rewrittenDataFiles(dropped.getValue(), table)
        .forEach(f -> droppedInputs.add(f.location()));
    List<KV<Integer, RewriteSubGroup>> survivors = planned.subList(1, planned.size());
    assertTrue("there must be surviving sub-groups to commit", !survivors.isEmpty());

    // Rewrite + commit only the survivors.
    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> rewriteTester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    rewriteTester.processBundle(survivors);
    List<KV<Integer, ExecutedGroup>> executed =
        rewriteTester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN);

    Map<Integer, List<ExecutedGroup>> byKey = new LinkedHashMap<>();
    for (KV<Integer, ExecutedGroup> kv : executed) {
      byKey.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv.getValue());
    }
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = new ArrayList<>();
    for (Map.Entry<Integer, List<ExecutedGroup>> e : byKey.entrySet()) {
      batches.add(KV.of(e.getKey(), (Iterable<ExecutedGroup>) e.getValue()));
    }
    // The dropped sub-group makes its parent incomplete, so parent-group atomicity commits NOTHING
    // for it — requireCommit=false (the point is precisely that no data lands, and none is lost).
    commit(table, batches, config, false);

    Table reloaded = warehouse.loadTable(tableId);
    Set<String> livePaths = liveDataFilePaths(reloaded);
    // The dropped sub-group's input files were never committed, so they must remain live...
    for (String p : droppedInputs) {
      assertTrue("dropped sub-group's input file must remain live: " + p, livePaths.contains(p));
    }
    // ...and no row is lost, duplicated, or swapped.
    assertEquals(
        "full row multiset must survive a dropped sub-group", expected, rowMultiset(reloaded));
  }

  /** Writes a v3 deletion vector deleting {@code position} in {@code dataFile} and commits it. */
  private void addDeletionVector(Table table, DataFile dataFile, long position) throws Exception {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 3, 3L).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try {
      writer.delete(dataFile.location().toString(), position, table.spec(), null);
    } finally {
      writer.close();
    }
    RowDelta rowDelta = table.newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();
    table.refresh();
  }

  private static boolean hasDeleteFiles(Table table) throws Exception {
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        if (!t.deletes().isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * A file-scoped deletion vector (Iceberg v3) over a rewritten data file must be applied on read
   * (the deleted row is gone) and removed by the rewrite commit (no delete files dangle afterward).
   */
  @Test
  public void danglingDeletionVectorRemovedOnRewrite() throws Exception {
    tableId = TableIdentifier.of("default", "dv_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            tableId, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    DataFile f1 =
        warehouse.writeRecords("dv1.parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1);
    DataFile f2 =
        warehouse.writeRecords("dv2.parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1);
    table.newAppend().appendFile(f1).appendFile(f2).commit();
    table.refresh();
    long rowsBefore = countRows(table);

    addDeletionVector(table, f1, 0L);
    assertEquals("the DV must remove exactly one row", rowsBefore - 1, countRows(table));
    assertTrue("table must have a delete file before rewrite", hasDeleteFiles(table));

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    commit(table, planAndRewrite(table, config), config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("surviving rows must be preserved", rowsBefore - 1, countRows(reloaded));
    assertTrue(
        "the dangling deletion vector must be removed by the rewrite commit",
        !hasDeleteFiles(reloaded));
  }

  /**
   * Multiple deletion vectors can live in ONE Puffin file at distinct {@code contentOffset}s, each
   * a separate {@link DeleteFile} sharing the same {@link DeleteFile#location()}. Deduping the
   * dangling-DV delete-set by {@code location()} alone collapses them and leaves some DVs undeleted
   * after rewrite. Rewriting two data files whose DVs share a Puffin must remove BOTH DVs.
   */
  @Test
  public void danglingDeletionVectorsSharingOnePuffinAreAllRemoved() throws Exception {
    tableId = TableIdentifier.of("default", "dv2_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            tableId, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    DataFile f1 =
        warehouse.writeRecords("dv2a.parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1);
    DataFile f2 =
        warehouse.writeRecords("dv2b.parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1);
    table.newAppend().appendFile(f1).appendFile(f2).commit();
    table.refresh();
    long rowsBefore = countRows(table);

    // One Puffin, two DVs (one per data file) at distinct offsets but the SAME location.
    List<DeleteFile> dvs = addDeletionVectorsInOnePuffin(table, f1, f2);
    assertEquals(
        "the two DVs must share one Puffin file",
        1,
        dvs.stream().map(d -> d.location().toString()).distinct().count());
    assertEquals("there must be two distinct DV delete entries", 2, dvs.size());
    assertEquals("each DV must remove one row", rowsBefore - 2, countRows(table));

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    commit(table, planAndRewrite(table, config), config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("surviving rows must be preserved", rowsBefore - 2, countRows(reloaded));
    assertEquals(
        "both DVs sharing the Puffin must be removed (none left in the table)",
        "0",
        reloaded.currentSnapshot().summary().get("total-delete-files"));
  }

  /** Writes ONE Puffin holding a DV (deleting position 0) for EACH given data file; commits it. */
  private List<DeleteFile> addDeletionVectorsInOnePuffin(Table table, DataFile... dataFiles)
      throws Exception {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 3, 3L).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try {
      for (DataFile df : dataFiles) {
        writer.delete(df.location().toString(), 0L, table.spec(), null);
      }
    } finally {
      writer.close();
    }
    List<DeleteFile> deleteFiles = new ArrayList<>();
    RowDelta rowDelta = table.newRowDelta();
    for (DeleteFile df : writer.result().deleteFiles()) {
      rowDelta.addDeletes(df);
      deleteFiles.add(df);
    }
    rowDelta.commit();
    table.refresh();
    return deleteFiles;
  }

  /** Writes an equality delete on {@code id == deleteId} and commits it via RowDelta. */
  private void addEqualityDelete(Table table, long deleteId) throws Exception {
    Schema eqSchema = table.schema().select("id");
    int eqFieldId = table.schema().findField("id").fieldId();
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            table.schema(), table.spec(), new int[] {eqFieldId}, eqSchema, null);
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 2, 2L).format(FileFormat.PARQUET).build();
    EqualityDeleteWriter<Record> writer =
        appenderFactory.newEqDeleteWriter(fileFactory.newOutputFile(), FileFormat.PARQUET, null);
    Record delete = GenericRecord.create(eqSchema);
    delete.setField("id", deleteId);
    try {
      writer.write(delete);
    } finally {
      writer.close();
    }
    table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    table.refresh();
  }

  /**
   * Sequence-number preservation: an equality delete committed AFTER the rewrite is planned but
   * BEFORE it is committed must still apply to the rewritten data. With {@code
   * useStartingSequenceNumber=true} (default), the new file is pinned to the starting snapshot's
   * (lower) data sequence number, so the later delete (higher sequence number) still removes the
   * row. If {@code dataSequenceNumber(...)} were dropped, the row would resurrect and this fails.
   */
  @Test
  public void lateEqualityDeleteStillAppliesAfterRewrite() throws Exception {
    Table table = buildDistinctTable(2, 3); // ids 0..5
    long deletedId = 1L;

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    // Plan + rewrite against the current (delete-free) snapshot.
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);

    // Concurrently delete a row via an equality delete, THEN commit the rewrite.
    addEqualityDelete(table, deletedId);
    commit(table, batches, config);

    Table reloaded = warehouse.loadTable(tableId);
    List<String> rows = rowMultiset(reloaded);
    assertTrue(
        "late equality delete must still apply to rewritten data (sequence number preserved)",
        rows.stream().noneMatch(k -> k.startsWith(deletedId + "|")));
    assertEquals("exactly one row removed", 5, rows.size());
  }

  /**
   * The rewrite must write with the configured {@code output-spec-id}, not just the table's current
   * default spec. Here the table default is evolved to a partitioned spec, but the rewrite is told
   * to output the original unpartitioned spec (id 0) — the rewritten files must use spec 0.
   */
  @Test
  public void honorsOutputSpecIdOverDefault() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    tableId = TableIdentifier.of("default", "outspec_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema); // unpartitioned, spec id 0
    int unpartitionedSpecId = table.spec().specId();

    AppendFiles append = table.newAppend();
    long id = 0;
    for (int f = 0; f < 2; f++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < 4; i++, id++) {
        Record r = GenericRecord.create(schema);
        r.setField("id", id);
        r.setField("shard", (int) (id % 2));
        records.add(r);
      }
      append.appendFile(
          warehouse.writeRecords("o" + f + "_" + System.nanoTime() + ".parquet", schema, records));
    }
    append.commit();

    // Evolve the default spec to partition by shard (new, non-zero spec id).
    table.updateSpec().addField("shard").commit();
    table.refresh();
    assertTrue("spec must have evolved", table.spec().specId() != unpartitionedSpecId);
    long expectedRows = countRows(table);
    Set<String> originalPaths = liveDataFilePaths(table);

    // Rewrite, explicitly targeting the ORIGINAL unpartitioned spec.
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1", "output-spec-id", String.valueOf(unpartitionedSpecId)))
            .build();
    commit(table, planAndRewrite(table, config), config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("rows preserved", expectedRows, countRows(reloaded));

    // Prove a rewrite actually happened: the original files are gone, replaced by fewer new files.
    // Without this, the spec assertion below would pass trivially (the originals already had spec
    // 0).
    Set<String> livePaths = liveDataFilePaths(reloaded);
    assertTrue(
        "rewrite must replace the input files (no original file remains live)",
        livePaths.stream().noneMatch(originalPaths::contains));
    assertTrue("rewrite must reduce the file count", livePaths.size() < originalPaths.size());

    // And the rewritten files use the configured output spec (0), not the evolved default spec.
    // Had output-spec-id been ignored, these new files would carry the partitioned default spec id.
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        assertEquals(
            "rewritten file must use the configured output spec, not the table default",
            unpartitionedSpecId,
            t.file().specId());
      }
    }
  }

  /**
   * R11-1 (P0): when {@code output-spec-id} targets a NON-current-default spec, Iceberg's planner
   * puts every such file into ONE {@code emptyStruct} group regardless of its actual partition
   * value, so the C5 single-partition fast path (which pins the first row's partition) would write
   * rows of one partition into and under another — silent corruption. The fast path must therefore
   * also require the output spec to be the current default; otherwise the fanout writer computes
   * per-row keys. Every committed output file must contain only its registered partition's rows.
   */
  @Test
  public void outputSpecIdToOldPartitionedSpecKeepsRowsInTheirPartitions() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec0 = PartitionSpec.builderFor(schema).identity("shard").build();
    tableId = TableIdentifier.of("default", "outspecpart_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema, spec0);
    int oldSpecId = table.spec().specId();

    // A small file for shard=0 AND shard=1, both under spec 0.
    AppendFiles append = table.newAppend();
    for (int shard = 0; shard < 2; shard++) {
      Record partition = GenericRecord.create(spec0.partitionType());
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
              "os" + shard + "_" + System.nanoTime() + ".parquet",
              schema,
              spec0,
              partition,
              records));
    }
    append.commit();

    // Evolve the spec so spec 0 (identity shard) is no longer the default -> the planner will group
    // the old spec-0 files into one emptyStruct group with MIXED shard values.
    table.updateSpec().removeField("shard").addField(Expressions.bucket("id", 4)).commit();
    table.refresh();
    assertNotEquals("spec must have evolved off the default", oldSpecId, table.spec().specId());
    List<String> expectedRows = idShardMultiset(table);

    // Compact the old data, writing outputs back with the OLD spec, with a target big enough to bin
    // both files together.
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of(
                    "output-spec-id", String.valueOf(oldSpecId),
                    "min-input-files", "1",
                    "rewrite-all", "true"))
            .build();
    commit(table, planAndRewrite(table, config), config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "full (id, shard) row multiset preserved", expectedRows, idShardMultiset(reloaded));

    // Every committed spec-0 output file must contain ONLY the rows of its registered shard.
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
        reloaded.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) {
        if (t.file().specId() != oldSpecId) {
          continue;
        }
        int registeredShard = t.file().partition().get(0, Integer.class);
        Set<Integer> rowShards = new HashSet<>();
        // Read PHYSICAL shard values (empty idToConstants): the default reader folds an identity-
        // partition column to the file's registered partition value, which would MASK the
        // mis-partitioning this test hunts for.
        try (CloseableIterable<Record> rows =
            ReadUtils.createReader(
                t, reloaded, reloaded.schema(), Collections.<Integer, Object>emptyMap())) {
          for (Record r : rows) {
            rowShards.add((Integer) r.getField("shard"));
          }
        }
        assertEquals(
            "output file registered under shard "
                + registeredShard
                + " must contain only that "
                + "shard's rows",
            ImmutableSet.of(registeredShard),
            rowShards);
      }
    }
  }

  /**
   * Rewriting a partitioned table must preserve each row's partition: rewritten files carry the
   * correct partition metadata (regression test for the writer omitting the partition spec, which
   * produced empty-partition files that fail to commit) and the row count is preserved.
   */
  @Test
  public void partitionedRewritePreservesPartitions() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();
    tableId = TableIdentifier.of("default", "partitioned_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema, spec);

    // Two files per partition for shards 0 and 1, so each partition's group qualifies for rewrite.
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

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertTrue("partitioned table must produce rewrite groups", !batches.isEmpty());
    commit(table, batches, config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("row count preserved", expectedRows, countRows(reloaded));
    // Each rewritten file lands in exactly one partition; reading per-partition returns the right
    // rows and never resurrects rows into the wrong (or a null) partition.
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
          throw new AssertionError("unexpected shard value: " + shard);
        }
      }
    }
    assertEquals("shard 0 rows preserved", expectedRows / 2, shard0);
    assertEquals("shard 1 rows preserved", expectedRows / 2, shard1);
  }

  /**
   * B6 e2e twin: a rewrite with {@code setFilter("shard = 0")} must compact ONLY shard 0. Shard 1's
   * data files must be byte-identical (same locations AND sizes) after the run — proving the filter
   * is honored end to end, not just at planning.
   */
  @Test
  public void filterCompactsOnlyMatchingPartitionEndToEnd() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();
    tableId = TableIdentifier.of("default", "b6filter_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, schema, spec);
    AppendFiles append = table.newAppend();
    for (int shard = 0; shard < 2; shard++) {
      Record partition = GenericRecord.create(spec.partitionType());
      partition.setField("shard", shard);
      for (int f = 0; f < 2; f++) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
          Record r = GenericRecord.create(schema);
          r.setField("id", (long) (shard * 1000 + f * 100 + i));
          r.setField("shard", shard);
          records.add(r);
        }
        append.appendFile(
            warehouse.writeRecords(
                "b6_" + shard + "_" + f + "_" + System.nanoTime() + ".parquet",
                schema,
                spec,
                partition,
                records));
      }
    }
    append.commit();
    table.refresh();

    Map<String, Long> shard0Before = filesInShard(table, 0);
    Map<String, Long> shard1Before = filesInShard(table, 1);
    assertEquals("fixture: shard 0 starts with 2 files", 2, shard0Before.size());
    assertEquals("fixture: shard 1 starts with 2 files", 2, shard1Before.size());

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setFilter("shard = 0")
            .setRewriteOptions(REWRITE_ALL)
            .build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    commit(table, batches, config);

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals(
        "shard 1 files must be byte-identical (untouched by a shard=0 filter)",
        shard1Before,
        filesInShard(reloaded, 1));
    assertTrue(
        "shard 0 must be compacted to fewer files",
        filesInShard(reloaded, 0).size() < shard0Before.size());
  }

  /**
   * R4: a file whose row-group ranges span several subgroups is a "spanning file". Under partial
   * progress, excluding ONE of its bins from the commit batch must exclude the WHOLE parent
   * (parent-group atomicity), so the shared file is never partially replaced — it stays live and no
   * rows are lost. Extends {@link #droppedSubGroupKeepsInputsLiveAndLosesNoRows} to ranged bins.
   */
  @Test
  public void droppedBinOfSpanningFileKeepsItLiveAndLosesNoRows() throws Exception {
    Table table = buildDistinctMultiRowGroupTable(1, 1500, 8192);
    assertRowGroupsAtLeast(table, 3);
    List<String> expected = rowMultiset(table);
    Set<String> originalPaths = liveDataFilePaths(table);
    assertEquals(1, originalPaths.size());
    String theFile = originalPaths.iterator().next();

    // A target below the file size splits its row-group ranges across several bins (one parent).
    long target = totalDataFileBytes(table) / 3;
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(1)
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1", "target-file-size-bytes", String.valueOf(target)))
            .build();

    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    List<KV<Integer, RewriteSubGroup>> planned =
        planTester.peekOutputElements(PlanRewriteGroups.GROUPS);
    assertTrue("the spanning file must split into >1 bin", planned.size() > 1);

    // Drop one bin of the parent (simulate its rewrite failing under partial progress).
    List<KV<Integer, RewriteSubGroup>> survivors = planned.subList(1, planned.size());
    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> rewriteTester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    rewriteTester.processBundle(survivors);
    List<KV<Integer, ExecutedGroup>> executed =
        rewriteTester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN);
    Map<Integer, List<ExecutedGroup>> byKey = new LinkedHashMap<>();
    for (KV<Integer, ExecutedGroup> kv : executed) {
      byKey.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv.getValue());
    }
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = new ArrayList<>();
    for (Map.Entry<Integer, List<ExecutedGroup>> e : byKey.entrySet()) {
      batches.add(KV.of(e.getKey(), (Iterable<ExecutedGroup>) e.getValue()));
    }
    // The incomplete parent commits nothing (parent-group atomicity), so requireCommit=false.
    commit(table, batches, config, false);

    Table reloaded = warehouse.loadTable(tableId);
    assertTrue(
        "the spanning file must remain live because its parent was excluded",
        liveDataFilePaths(reloaded).contains(theFile));
    assertEquals(
        "no rows lost when a spanning parent is excluded", expected, rowMultiset(reloaded));
  }

  /**
   * R4: when every row of a parent's inputs is deleted (here by full-file deletion vectors), each
   * bin's writer produces zero output files, so the parent commits DELETE-ONLY — the input data
   * files and their dangling DVs are removed and nothing is added.
   */
  @Test
  public void allRowsDeletedParentCommitsDeleteOnly() throws Exception {
    tableId = TableIdentifier.of("default", "delonly_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            tableId, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    DataFile f1 =
        warehouse.writeRecords(
            "do1_" + System.nanoTime() + ".parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1);
    DataFile f2 =
        warehouse.writeRecords(
            "do2_" + System.nanoTime() + ".parquet", table.schema(), TestFixtures.FILE2SNAPSHOT1);
    table.newAppend().appendFile(f1).appendFile(f2).commit();
    table.refresh();
    assertTrue("fixture needs rows", countRows(table) > 0);

    // Delete EVERY row of both files via deletion vectors.
    addFullFileDeletionVector(table, f1, f1.recordCount());
    addFullFileDeletionVector(table, f2, f2.recordCount());
    assertEquals("all rows must be deleted before rewrite", 0, countRows(table));
    assertTrue("table must carry delete files before rewrite", hasDeleteFiles(table));

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder().setRewriteOptions(REWRITE_ALL).build();
    List<KV<Integer, Iterable<ExecutedGroup>>> batches = planAndRewrite(table, config);
    assertTrue("fully-deleted files must still be planned for rewrite", !batches.isEmpty());
    for (KV<Integer, Iterable<ExecutedGroup>> b : batches) {
      for (ExecutedGroup eg : b.getValue()) {
        assertTrue("a fully-deleted group must produce no new files", eg.getNewFiles().isEmpty());
      }
    }
    commit(table, batches, config); // the delete-only commit still lands

    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("no rows remain", 0, countRows(reloaded));
    assertFalse("dangling delete files must be removed by the rewrite", hasDeleteFiles(reloaded));
    assertTrue("a delete-only commit adds no data files", liveDataFilePaths(reloaded).isEmpty());
  }

  /**
   * R4 edge: with a target larger than the whole group, every row-group range packs into ONE bin
   * (N=1). The single subgroup's adjacent ranges merge back and the parent commits cleanly as one
   * unit — the N=1 case collapses without special handling.
   */
  @Test
  public void singleBinGroupCombinesRangesAndCommits() throws Exception {
    Table table = buildDistinctMultiRowGroupTable(3, 800, 8192);
    List<String> expected = rowMultiset(table);
    assertEquals(3, liveDataFilePaths(table).size());
    long total = totalDataFileBytes(table);

    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1", "target-file-size-bytes", String.valueOf(total * 2)))
            .build();
    assertEquals("all ranges must pack into a single subgroup", 1, planOnly(table, config).size());

    commit(table, planAndRewrite(table, config), config);
    Table reloaded = warehouse.loadTable(tableId);
    assertEquals("rows preserved through a single-bin rewrite", expected, rowMultiset(reloaded));
    assertTrue(
        "a single bin must combine the inputs into fewer files",
        liveDataFilePaths(reloaded).size() < 3);
  }

  /** Writes a deletion vector deleting EVERY position [0, recordCount) of {@code dataFile}. */
  private void addFullFileDeletionVector(Table table, DataFile dataFile, long recordCount)
      throws Exception {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 3, 3L).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try {
      for (long pos = 0; pos < recordCount; pos++) {
        writer.delete(dataFile.location().toString(), pos, table.spec(), null);
      }
    } finally {
      writer.close();
    }
    RowDelta rowDelta = table.newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();
    table.refresh();
  }
}
