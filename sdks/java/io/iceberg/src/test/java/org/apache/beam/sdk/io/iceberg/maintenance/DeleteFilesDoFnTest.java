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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DeleteFilesDoFnTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig getCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
        .build();
  }

  @Test
  public void testDeletesFilesPhysicallyWhenCleanFilesTrue()
      throws IOException, NoSuchSchemaException {
    TableIdentifier tableId = TableIdentifier.of("default", "delete_phys_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    DataFile file =
        warehouse.writeRecords(
            "delete_target_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(ExpireSnapshotsTestFixtures.createRecord(1L, "val-1")));

    File diskFile = new File(new Path(file.path().toString()).toUri());
    assertTrue("Target file must exist prior to deletion", diskFile.exists());

    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder().setCleanFiles(true).build();

    SchemaCoder<FileInfo> fileInfoCoder =
        SchemaRegistry.createDefault().getSchemaCoder(FileInfo.class);

    PCollection<ExpireSnapshotsResult> output =
        pipeline
            .apply(
                Create.of(
                        KV.of(
                            ShardedKey.of(tableId.toString(), new byte[0]),
                            (Iterable<FileInfo>)
                                Collections.singletonList(
                                    FileInfo.of(
                                        file.path().toString(),
                                        FileCategory.DATA,
                                        false,
                                        tableId.toString()))))
                    .withCoder(
                        KvCoder.of(
                            ShardedKey.Coder.of(StringUtf8Coder.of()),
                            IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteFilesDoFn(getCatalogConfig(), config)));

    PAssert.that(output)
        .containsInAnyOrder(ExpireSnapshotsResult.builder().setDeletedDataFilesCount(1L).build());

    pipeline.run();

    assertFalse("File must be physically deleted from storage", diskFile.exists());
  }

  @Test
  public void testDryRunDoesNotDeleteFromStorage() throws IOException, NoSuchSchemaException {
    TableIdentifier tableId = TableIdentifier.of("default", "dry_run_" + System.nanoTime());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    DataFile file =
        warehouse.writeRecords(
            "dry_target_" + System.nanoTime() + ".parquet",
            table.schema(),
            Collections.singletonList(ExpireSnapshotsTestFixtures.createRecord(1L, "val-1")));

    File diskFile = new File(new Path(file.path().toString()).toUri());
    assertTrue("Target file must exist prior to deletion", diskFile.exists());

    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder().setCleanFiles(false).build();

    SchemaCoder<FileInfo> fileInfoCoder =
        SchemaRegistry.createDefault().getSchemaCoder(FileInfo.class);

    PCollection<ExpireSnapshotsResult> output =
        pipeline
            .apply(
                Create.of(
                        KV.of(
                            ShardedKey.of(tableId.toString(), new byte[0]),
                            (Iterable<FileInfo>)
                                Collections.singletonList(
                                    FileInfo.of(
                                        file.path().toString(),
                                        FileCategory.DATA,
                                        false,
                                        tableId.toString()))))
                    .withCoder(
                        KvCoder.of(
                            ShardedKey.Coder.of(StringUtf8Coder.of()),
                            IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteFilesDoFn(getCatalogConfig(), config)));

    PAssert.that(output)
        .containsInAnyOrder(ExpireSnapshotsResult.builder().setDeletedDataFilesCount(1L).build());

    pipeline.run();

    assertTrue("File must still exist on storage during dry run", diskFile.exists());
  }

  @Test
  public void testIgnoresNotFoundException() throws NoSuchSchemaException {
    TableIdentifier tableId = TableIdentifier.of("default", "missing_file_" + System.nanoTime());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);

    String missingPath = warehouse.location + "/non_existent.parquet";
    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder().setCleanFiles(true).build();

    SchemaCoder<FileInfo> fileInfoCoder =
        SchemaRegistry.createDefault().getSchemaCoder(FileInfo.class);

    PCollection<ExpireSnapshotsResult> output =
        pipeline
            .apply(
                Create.of(
                        KV.of(
                            ShardedKey.of(tableId.toString(), new byte[0]),
                            (Iterable<FileInfo>)
                                Collections.singletonList(
                                    FileInfo.of(
                                        missingPath,
                                        FileCategory.DATA,
                                        false,
                                        tableId.toString()))))
                    .withCoder(
                        KvCoder.of(
                            ShardedKey.Coder.of(StringUtf8Coder.of()),
                            IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteFilesDoFn(getCatalogConfig(), config)));

    PAssert.that(output)
        .containsInAnyOrder(ExpireSnapshotsResult.builder().setDeletedDataFilesCount(1L).build());

    // Should complete cleanly without exception
    pipeline.run();
  }

  @Test
  public void testDeletesPositionAndEqualityDeleteFiles()
      throws IOException, NoSuchSchemaException {
    TableIdentifier tableId =
        TableIdentifier.of("default", "delete_row_level_" + System.nanoTime());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);

    File posDiskFile = TEMPORARY_FOLDER.newFile("pos_del_" + System.nanoTime() + ".parquet");
    File eqDiskFile = TEMPORARY_FOLDER.newFile("eq_del_" + System.nanoTime() + ".parquet");
    assertTrue(posDiskFile.exists());
    assertTrue(eqDiskFile.exists());

    ExpireSnapshots.Configuration config =
        ExpireSnapshots.Configuration.builder().setCleanFiles(true).build();

    SchemaCoder<FileInfo> fileInfoCoder =
        SchemaRegistry.createDefault().getSchemaCoder(FileInfo.class);

    List<FileInfo> files =
        Arrays.asList(
            FileInfo.of(
                posDiskFile.getAbsolutePath(),
                FileCategory.POSITION_DELETES,
                false,
                tableId.toString()),
            FileInfo.of(
                eqDiskFile.getAbsolutePath(),
                FileCategory.EQUALITY_DELETES,
                false,
                tableId.toString()));

    PCollection<ExpireSnapshotsResult> output =
        pipeline
            .apply(
                Create.of(
                        KV.of(
                            ShardedKey.of(tableId.toString(), new byte[0]),
                            (Iterable<FileInfo>) files))
                    .withCoder(
                        KvCoder.of(
                            ShardedKey.Coder.of(StringUtf8Coder.of()),
                            IterableCoder.of(fileInfoCoder))))
            .apply(ParDo.of(new DeleteFilesDoFn(getCatalogConfig(), config)));

    PAssert.that(output)
        .containsInAnyOrder(
            ExpireSnapshotsResult.builder()
                .setDeletedPositionDeleteFilesCount(1L)
                .setDeletedEqualityDeleteFilesCount(1L)
                .build());

    pipeline.run();

    assertFalse("Position delete file must be deleted", posDiskFile.exists());
    assertFalse("Equality delete file must be deleted", eqDiskFile.exists());
  }

  @Test
  public void testDeletePathsBulkFallbackAndPartialFailure() {
    Set<String> deleted = new HashSet<>();
    FileIO io =
        new FileIO() {
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
            if ("fail.parquet".equals(path)) {
              throw new RuntimeException("permission denied");
            } else if ("missing.parquet".equals(path)) {
              throw new NotFoundException("not found");
            } else {
              deleted.add(path);
            }
          }
        };

    // Standard FileIO without SupportsBulkOperations
    Set<String> failed =
        DeleteFilesDoFn.deletePaths(
            io, Arrays.asList("ok.parquet", "missing.parquet", "fail.parquet"));
    assertEquals(Collections.singleton("fail.parquet"), failed);
    assertTrue(deleted.contains("ok.parquet"));
  }

  @Test
  public void testDeletePathsBulkOperationsWithFailure() {
    Set<String> deleted = new HashSet<>();
    class BulkFileIO implements FileIO, SupportsBulkOperations {
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
        if ("fail.parquet".equals(path)) {
          throw new RuntimeException("permission denied");
        }
        deleted.add(path);
      }

      @Override
      public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
        throw new BulkDeletionFailureException(1);
      }
    }

    BulkFileIO io = new BulkFileIO();
    Set<String> failed =
        DeleteFilesDoFn.deletePaths(io, Arrays.asList("ok.parquet", "fail.parquet"));
    assertEquals(Collections.singleton("fail.parquet"), failed);
    assertTrue(deleted.contains("ok.parquet"));
  }
}
