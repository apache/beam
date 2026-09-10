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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExpireSnapshotsTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testStandardSnapshotExpiration() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "standard_expire_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, tableId);

    List<Snapshot> originalSnapshots = Lists.newArrayList(table.snapshots());
    assertEquals(3, originalSnapshots.size());

    // Record data file paths from snapshot 1, 2, and 3
    String s1Path = getDataFilePath(table, originalSnapshots.get(0));
    String s2Path = getDataFilePath(table, originalSnapshots.get(1));
    String s3Path = getDataFilePath(table, originalSnapshots.get(2));

    assertTrue("S1 file must exist", fileExists(s1Path));
    assertTrue("S2 file must exist", fileExists(s2Path));
    assertTrue("S3 file must exist", fileExists(s3Path));

    long cutoff = originalSnapshots.get(2).timestampMillis();
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(1)
            .setCleanFiles(true)
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              assertEquals(2L, r.getExpiredSnapshotsCount());
              assertEquals(0L, r.getDeletedDataFilesCount());
              assertTrue(r.getDeletedManifestListsCount() >= 2L);
              return null;
            });

    pipeline.run();

    table.refresh();
    List<Snapshot> remainingSnapshots = Lists.newArrayList(table.snapshots());
    assertEquals(1, remainingSnapshots.size());
    assertEquals(originalSnapshots.get(2).snapshotId(), remainingSnapshots.get(0).snapshotId());

    // In an append-only sequence, S3 still references files from S1 and S2; all data files must be
    // preserved!
    assertTrue("S1 file must still exist as it is referenced in S3", fileExists(s1Path));
    assertTrue("S2 file must still exist as it is referenced in S3", fileExists(s2Path));
    assertTrue("S3 file must still exist as it is referenced in S3", fileExists(s3Path));
  }

  @Test
  public void testSharedFilesPreservedAcrossSnapshots() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "shared_files_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithOverwrites(warehouse, tableId);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    assertEquals(3, snapshots.size());

    // In createTableWithOverwrites:
    // S1: File A
    // S2: File B
    // S3: File A overwritten with File C (active: B, C; obsolete: A)
    long cutoff = snapshots.get(2).timestampMillis();
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(1)
            .setCleanFiles(true)
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              assertEquals(2L, r.getExpiredSnapshotsCount());
              // Only File A is deleted; File B was retained into S3 and File C was added in S3
              assertEquals(1L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    table.refresh();
    assertEquals(1, Lists.newArrayList(table.snapshots()).size());

    // Verify active files in current snapshot
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      int activeFileCount = 0;
      for (FileScanTask task : tasks) {
        activeFileCount++;
        assertTrue(
            "Active file must exist on disk: " + task.file().path(),
            fileExists(task.file().path().toString()));
      }
      assertEquals(2, activeFileCount);
    }
  }

  @Test
  public void testBranchAndTagProtection() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "branch_protect_" + System.nanoTime());
    String branchName = "test_branch";
    Table table = ExpireSnapshotsTestFixtures.createTableWithBranch(warehouse, tableId, branchName);

    table.refresh();
    assertEquals(3, Lists.newArrayList(table.snapshots()).size());

    Snapshot branchHead = table.snapshot(table.refs().get(branchName).snapshotId());
    String branchFilePath = getDataFilePath(table, branchHead);
    assertTrue("Branch data file must exist", fileExists(branchFilePath));

    // Expire on main branch older than current time, retaining 1
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(System.currentTimeMillis() + 100_000L)
            .setRetainLast(1)
            .setCleanFiles(true)
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              assertTrue(r.getExpiredSnapshotsCount() >= 1);
              return null;
            });

    pipeline.run();

    table.refresh();
    // Branch file must be completely untouched and preserved!
    assertTrue("Branch data file must NOT be deleted", fileExists(branchFilePath));
    assertTrue("Branch ref must still exist", table.refs().containsKey(branchName));
  }

  @Test
  public void testRetainLastSafetyFloor() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "retain_floor_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, tableId);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    assertEquals(3, snapshots.size());
    long cutoff = snapshots.get(2).timestampMillis();

    // Snapshots 0 and 1 are older than cutoff, but retainLast = 2 preserves Snapshot 1
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(2)
            .setCleanFiles(true)
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              assertEquals(1L, r.getExpiredSnapshotsCount());
              assertEquals(0L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    table.refresh();
    assertEquals(2, Lists.newArrayList(table.snapshots()).size());
  }

  @Test
  public void testIdempotencyRepeatedExecutions() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "idempotent_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, tableId);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    long cutoff = snapshots.get(2).timestampMillis();
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(1)
            .setCleanFiles(true)
            .build();

    // Run 1: expires 2 snapshots
    pipeline
        .apply("Input 1", Create.of(tableId.toString()))
        .apply("Expire 1", ExpireSnapshots.create(getCatalogConfig(), config));
    pipeline.run();

    table.refresh();
    assertEquals(1, Lists.newArrayList(table.snapshots()).size());

    // Run 2: pipeline runs again on the same table
    Pipeline pipeline2 = Pipeline.create();
    PCollection<ExpireSnapshotsResult> result2 =
        pipeline2
            .apply("Input 2", Create.of(tableId.toString()))
            .apply("Expire 2", ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result2).containsInAnyOrder(ExpireSnapshotsResult.zeros());
    pipeline2.run().waitUntilFinish();

    table.refresh();
    assertEquals(1, Lists.newArrayList(table.snapshots()).size());
  }

  @Test
  public void testDryRunDoesNotDeleteFromStorage() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "dry_run_" + System.nanoTime());
    Table table = ExpireSnapshotsTestFixtures.createTableWithOverwrites(warehouse, tableId);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    String fileAPath = getDataFilePath(table, snapshots.get(0));

    long cutoff = snapshots.get(2).timestampMillis();
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(1)
            .setCleanFiles(false) // Dry run
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              assertEquals(2L, r.getExpiredSnapshotsCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              return null;
            });

    pipeline.run();

    // Files must still physically exist on storage after dry run!
    assertTrue("File A must still exist after dry run", fileExists(fileAPath));
  }

  @Test
  public void testEmptyTableAndSingleSnapshot() {
    TableIdentifier tableId = TableIdentifier.of("default", "empty_table_" + System.nanoTime());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);

    ExpireSnapshots.Configuration config = ExpireSnapshots.Configuration.builder().build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result).containsInAnyOrder(ExpireSnapshotsResult.zeros());
    pipeline.run();
  }

  @Test
  public void testMultiTablePipeline() throws IOException {
    TableIdentifier table1 = TableIdentifier.of("default", "multi_table_1_" + System.nanoTime());
    TableIdentifier table2 = TableIdentifier.of("default", "multi_table_2_" + System.nanoTime());

    Table t1 = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, table1);
    Table t2 = ExpireSnapshotsTestFixtures.createTableWithMultiSnapshots(warehouse, table2);

    List<Snapshot> s1 = Lists.newArrayList(t1.snapshots());
    List<Snapshot> s2 = Lists.newArrayList(t2.snapshots());
    long cutoff = Math.max(s1.get(2).timestampMillis(), s2.get(2).timestampMillis());

    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(1)
            .setCleanFiles(true)
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(table1.toString(), table2.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              // 2 snapshots expired per table * 2 tables = 4
              assertEquals(4L, r.getExpiredSnapshotsCount());
              assertTrue(r.getDeletedManifestListsCount() >= 4L);
              return null;
            });

    pipeline.run();

    t1.refresh();
    t2.refresh();
    assertEquals(1, Lists.newArrayList(t1.snapshots()).size());
    assertEquals(1, Lists.newArrayList(t2.snapshots()).size());
  }

  @Test
  public void testExpireSnapshotsWithDeleteFiles() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", "expire_deletes_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    table.refresh();

    // S1: Data file A
    DataFile fileA =
        warehouse.writeRecords(
            "file_a_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(ExpireSnapshotsTestFixtures.createRecord(0L, "val-0")));
    table.newAppend().appendFile(fileA).commit();
    table.refresh();

    // S2: Add position delete file
    File posDeleteDiskFile = TEMPORARY_FOLDER.newFile("pos_del_" + System.nanoTime() + ".parquet");
    assertTrue(posDeleteDiskFile.exists());

    DeleteFile posDelete =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath(posDeleteDiskFile.getAbsolutePath())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(1L)
            .build();

    ExpireSnapshotsTestFixtures.waitUntilAfter(table.currentSnapshot().timestampMillis());
    table.newRowDelta().addDeletes(posDelete).commit();
    table.refresh();

    // S3: Overwrite / rewrite data file (clean up / replace)
    DataFile fileB =
        warehouse.writeRecords(
            "file_b_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(ExpireSnapshotsTestFixtures.createRecord(1L, "val-1")));
    ExpireSnapshotsTestFixtures.waitUntilAfter(table.currentSnapshot().timestampMillis());
    table
        .newRewrite()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .deleteFile(fileA)
        .deleteFile(posDelete)
        .addFile(fileB)
        .commit();
    table.refresh();

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    assertEquals(3, snapshots.size());
    long cutoff = snapshots.get(2).timestampMillis();

    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder()
            .setExpireOlderThan(cutoff)
            .setRetainLast(1)
            .setCleanFiles(true)
            .build();

    PCollection<ExpireSnapshotsResult> result =
        pipeline
            .apply(Create.of(tableId.toString()))
            .apply(ExpireSnapshots.create(getCatalogConfig(), config));

    PAssert.that(result)
        .satisfies(
            results -> {
              ExpireSnapshotsResult r = results.iterator().next();
              assertEquals(2L, r.getExpiredSnapshotsCount());
              assertEquals(1L, r.getDeletedDataFilesCount());
              assertEquals(1L, r.getDeletedPositionDeleteFilesCount());
              return null;
            });

    pipeline.run();

    assertFalse(
        "Position delete file must be physically deleted from storage", posDeleteDiskFile.exists());
  }

  private static boolean fileExists(String path) {
    return new File(new Path(path).toUri()).exists();
  }

  private static String getDataFilePath(Table table, Snapshot snapshot) throws IOException {
    List<DataFile> files = Lists.newArrayList(snapshot.addedDataFiles(table.io()));
    if (!files.isEmpty()) {
      return files.get(0).path().toString();
    }
    return null;
  }
}
