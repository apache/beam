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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchDeleteFilesTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  // ---------------------------------------------------------------------------------------------
  // deletePaths, against fake FileIOs.
  // ---------------------------------------------------------------------------------------------

  /** A {@link FileIO} that records per-file deletes and fails for configured paths. */
  private static class FakeFileIO implements FileIO {
    final List<String> deleted = new ArrayList<>();
    final Map<String, RuntimeException> failures = new HashMap<>();

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
      RuntimeException failure = failures.get(path);
      if (failure != null) {
        throw failure;
      }
      deleted.add(path);
    }
  }

  /** A {@link FakeFileIO} that also supports bulk deletes, which throw a configured exception. */
  private static class FakeBulkFileIO extends FakeFileIO implements SupportsBulkOperations {
    final List<List<String>> bulkCalls = new ArrayList<>();
    @Nullable RuntimeException bulkFailure = null;

    @Override
    public void deleteFiles(Iterable<String> paths) {
      bulkCalls.add(ImmutableList.copyOf(paths));
      RuntimeException failure = bulkFailure;
      if (failure != null) {
        throw failure;
      }
    }
  }

  private static final List<String> PATHS = ImmutableList.of("a", "b", "c");

  @Test
  public void testDeletePathsBulkSuccess() {
    FakeBulkFileIO io = new FakeBulkFileIO();
    assertTrue(BatchDeleteFiles.deletePaths(io, PATHS).isEmpty());
    assertEquals(ImmutableList.of(PATHS), io.bulkCalls);
    assertTrue(io.deleted.isEmpty());
  }

  @Test
  public void testDeletePathsFallsBackAfterBulkDeletionFailure() {
    FakeBulkFileIO io = new FakeBulkFileIO();
    io.bulkFailure = new BulkDeletionFailureException(1);
    io.failures.put("b", new RuntimeException("permission denied"));

    assertEquals(ImmutableSet.of("b"), BatchDeleteFiles.deletePaths(io, PATHS));
    assertEquals(1, io.bulkCalls.size());
    assertEquals(ImmutableList.of("a", "c"), io.deleted);
  }

  @Test
  public void testDeletePathsFallsBackAfterOtherBulkFailure() {
    FakeBulkFileIO io = new FakeBulkFileIO();
    io.bulkFailure = new IllegalStateException("bulk endpoint unavailable");

    assertTrue(BatchDeleteFiles.deletePaths(io, PATHS).isEmpty());
    assertEquals(PATHS, io.deleted);
  }

  @Test
  public void testDeletePathsWithoutBulkSupport() {
    FakeFileIO io = new FakeFileIO();
    io.failures.put("c", new RuntimeException("permission denied"));

    assertEquals(ImmutableSet.of("c"), BatchDeleteFiles.deletePaths(io, PATHS));
    assertEquals(ImmutableList.of("a", "b"), io.deleted);
  }

  @Test
  public void testDeletePathsCountsNotFoundAsDeleted() {
    FakeBulkFileIO io = new FakeBulkFileIO();
    io.bulkFailure = new BulkDeletionFailureException(1);
    io.failures.put("a", new NotFoundException("File does not exist: %s", "a"));

    assertTrue(BatchDeleteFiles.deletePaths(io, PATHS).isEmpty());
    assertEquals(ImmutableList.of("b", "c"), io.deleted);
  }

  @Test
  public void testDeletePathsRethrowsInterruptedBulkDeletion() {
    FakeBulkFileIO io = new FakeBulkFileIO();
    RuntimeException interrupted = new RuntimeException(new InterruptedException());
    io.bulkFailure = interrupted;
    try {
      assertSame(
          interrupted,
          assertThrows(RuntimeException.class, () -> BatchDeleteFiles.deletePaths(io, PATHS)));
      assertTrue(Thread.currentThread().isInterrupted());
      assertTrue(io.deleted.isEmpty());
    } finally {
      Thread.interrupted(); // Clear the interrupt status for later tests.
    }
  }

  @Test
  public void testDeletePathsStopsWhenInterruptedDuringFallback() {
    FakeFileIO io =
        new FakeFileIO() {
          @Override
          public void deleteFile(String path) {
            if (path.equals("b")) {
              Thread.currentThread().interrupt();
              throw new RuntimeException("interrupted while deleting " + path);
            }
            super.deleteFile(path);
          }
        };
    try {
      assertThrows(RuntimeException.class, () -> BatchDeleteFiles.deletePaths(io, PATHS));
      assertEquals(ImmutableList.of("a"), io.deleted);
    } finally {
      Thread.interrupted(); // Clear the interrupt status for later tests.
    }
  }

  @Test
  public void testDeletePathsStopsWhenInterruptWasSwallowed() {
    // Models a FileIO that logs errors instead of throwing them.
    FakeFileIO io =
        new FakeFileIO() {
          @Override
          public void deleteFile(String path) {
            super.deleteFile(path);
            if (path.equals("a")) {
              Thread.currentThread().interrupt();
            }
          }
        };
    try {
      RuntimeException e =
          assertThrows(RuntimeException.class, () -> BatchDeleteFiles.deletePaths(io, PATHS));
      assertTrue(e.getCause() instanceof InterruptedException);
      assertEquals(ImmutableList.of("a"), io.deleted);
    } finally {
      Thread.interrupted(); // Clear the interrupt status for later tests.
    }
  }

  @Test
  public void testDeletePathsReturnsAllFailuresBeyondLoggingLimit() {
    FakeFileIO io = new FakeFileIO();
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < BatchDeleteFiles.MAX_LOGGED_FAILURES + 5; i++) {
      String path = "file-" + i;
      paths.add(path);
      io.failures.put(path, new RuntimeException("permission denied"));
    }
    assertEquals(ImmutableSet.copyOf(paths), BatchDeleteFiles.deletePaths(io, paths));
  }

  @Test
  public void testDeletePathsEmptyInput() {
    FakeBulkFileIO io = new FakeBulkFileIO();
    assertTrue(BatchDeleteFiles.deletePaths(io, ImmutableList.of()).isEmpty());
    assertTrue(io.bulkCalls.isEmpty());
    assertTrue(io.deleted.isEmpty());
  }

  // ---------------------------------------------------------------------------------------------
  // The transform, against tables in a local Hadoop catalog.
  // ---------------------------------------------------------------------------------------------

  private IcebergCatalogConfig catalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogName("name")
        .setCatalogProperties(
            ImmutableMap.of(
                "type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, "warehouse", warehouse.location))
        .build();
  }

  /** Returns a catalog config whose tables use {@link NoRetryHadoopFileIO}. */
  private IcebergCatalogConfig noRetryCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogName("no-retry")
        .setCatalogProperties(
            ImmutableMap.of(
                "type",
                CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
                "warehouse",
                warehouse.location,
                CatalogProperties.FILE_IO_IMPL,
                NoRetryHadoopFileIO.class.getName()))
        .build();
  }

  /**
   * A {@link HadoopFileIO} whose bulk deletes do not retry. {@link HadoopFileIO#deleteFiles}
   * retries each failed file three times with a fixed 1s/2s/4s backoff, which would make a failure
   * test take about seven seconds.
   */
  public static class NoRetryHadoopFileIO extends HadoopFileIO {
    @Override
    public void deleteFiles(Iterable<String> paths) {
      int failures = 0;
      for (String path : paths) {
        try {
          deleteFile(path);
        } catch (RuntimeException e) {
          failures++;
        }
      }
      if (failures > 0) {
        throw new BulkDeletionFailureException(failures);
      }
    }
  }

  private String createTable(String name) {
    warehouse.createTable(TableIdentifier.of("default", name), TestFixtures.SCHEMA);
    return "default." + name;
  }

  /** Creates an empty file and returns its location. */
  private String createFile(String name) throws IOException {
    String location = warehouse.location + "/files/" + name;
    Path path = localPath(location);
    Files.createDirectories(path.getParent());
    Files.createFile(path);
    return location;
  }

  private static Path localPath(String location) {
    return Paths.get(URI.create(location));
  }

  private static long counter(PipelineResult result, String name) {
    long total = 0;
    for (MetricResult<Long> counter :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(BatchDeleteFiles.class, name))
                    .build())
            .getCounters()) {
      total += counter.getAttempted();
    }
    return total;
  }

  private static PCollection<FileCategoryCounts> sum(PCollection<FileCategoryCounts> counts) {
    return counts.apply("Sum", Combine.globally(FileCategoryCounts.sum()));
  }

  @Test
  public void testDeletesFilesAndCountsByCategory() throws IOException {
    String table = createTable("by_category");
    String data = createFile("data.parquet");
    String deletes = createFile("deletes.parquet");
    String manifest = createFile("manifest.avro");
    String manifestList = createFile("snap-1.avro");

    PCollection<FileCategoryCounts> counts =
        pipeline
            .apply(
                Create.of(
                    FileInfo.of(data, FileCategory.DATA, false, table),
                    FileInfo.of(deletes, FileCategory.POSITION_DELETES, false, table),
                    FileInfo.of(manifest, FileCategory.MANIFEST, false, table),
                    FileInfo.of(manifestList, FileCategory.MANIFEST_LIST, false, table)))
            .apply(BatchDeleteFiles.of(catalogConfig()));
    PAssert.thatSingleton(sum(counts))
        .isEqualTo(
            FileCategoryCounts.of(FileCategory.DATA, 1)
                .plus(FileCategoryCounts.of(FileCategory.POSITION_DELETES, 1))
                .plus(FileCategoryCounts.of(FileCategory.MANIFEST, 1))
                .plus(FileCategoryCounts.of(FileCategory.MANIFEST_LIST, 1)));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    for (String location : ImmutableList.of(data, deletes, manifest, manifestList)) {
      assertFalse(location, Files.exists(localPath(location)));
    }
    assertEquals(4, counter(result, BatchDeleteFiles.DELETED_FILES));
    assertEquals(0, counter(result, BatchDeleteFiles.FAILED_FILE_DELETIONS));
  }

  @Test
  public void testSplitsInputIntoBatches() throws IOException {
    String table = createTable("batches");
    List<FileInfo> files = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      files.add(FileInfo.of(createFile("data-" + i + ".parquet"), FileCategory.DATA, false, table));
    }

    PCollection<FileCategoryCounts> counts =
        pipeline
            .apply(Create.of(files))
            .apply(BatchDeleteFiles.of(catalogConfig()).withBatchSize(2));
    PAssert.that(counts)
        .satisfies(
            batches -> {
              int numBatches = 0;
              for (FileCategoryCounts batch : batches) {
                assertTrue(batch.total() <= 2);
                numBatches++;
              }
              assertTrue(numBatches >= 3);
              return null;
            });
    PAssert.thatSingleton(sum(counts)).isEqualTo(FileCategoryCounts.of(FileCategory.DATA, 5));
    pipeline.run().waitUntilFinish();

    for (FileInfo file : files) {
      assertFalse(Files.exists(localPath(file.getPath())));
    }
  }

  @Test
  public void testDeletesFilesOfMultipleTables() throws IOException {
    String table1 = createTable("table1");
    String table2 = createTable("table2");
    String file1 = createFile("table1-data.parquet");
    String file2 = createFile("table2-manifest.avro");

    PCollection<FileCategoryCounts> counts =
        pipeline
            .apply(
                Create.of(
                    FileInfo.of(file1, FileCategory.DATA, false, table1),
                    FileInfo.of(file2, FileCategory.MANIFEST, false, table2)))
            .apply(BatchDeleteFiles.of(catalogConfig()));
    // Batches are per table, so neither batch mixes the two tables' files. (Both tables use the
    // same HadoopFileIO here, so this checks the batching, not which FileIO deletes each file.)
    PAssert.that(counts)
        .containsInAnyOrder(
            FileCategoryCounts.of(FileCategory.DATA, 1),
            FileCategoryCounts.of(FileCategory.MANIFEST, 1));
    pipeline.run().waitUntilFinish();

    assertFalse(Files.exists(localPath(file1)));
    assertFalse(Files.exists(localPath(file2)));
  }

  @Test
  public void testDeleteDisabledReportsWithoutDeleting() throws IOException {
    // The table does not exist, which shows that the catalog is not contacted.
    String file = createFile("data.parquet");

    PCollection<FileCategoryCounts> counts =
        pipeline
            .apply(Create.of(FileInfo.of(file, FileCategory.DATA, false, "default.missing")))
            .apply(BatchDeleteFiles.of(catalogConfig()).withDeleteEnabled(false));
    PAssert.thatSingleton(counts).isEqualTo(FileCategoryCounts.of(FileCategory.DATA, 1));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    assertTrue(Files.exists(localPath(file)));
    assertEquals(0, counter(result, BatchDeleteFiles.DELETED_FILES));
    assertEquals(0, counter(result, BatchDeleteFiles.FAILED_FILE_DELETIONS));
  }

  @Test
  public void testRejectsReachableFile() throws IOException {
    String table = createTable("reachable");
    String file = createFile("live-data.parquet");

    pipeline
        .apply(Create.of(FileInfo.of(file, FileCategory.DATA, true, table)))
        .apply(BatchDeleteFiles.of(catalogConfig()));
    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run().waitUntilFinish());
    assertTrue(e.getCause() instanceof IllegalArgumentException);
    assertTrue(e.getCause().getMessage().contains(file));
    assertTrue(Files.exists(localPath(file)));
  }

  @Test
  public void testDuplicatePathIsCountedPerAppearance() throws IOException {
    String table = createTable("duplicates");
    String file = createFile("data.parquet");
    FileInfo info = FileInfo.of(file, FileCategory.DATA, false, table);

    PCollection<FileCategoryCounts> counts =
        pipeline.apply(Create.of(info, info)).apply(BatchDeleteFiles.of(catalogConfig()));
    PAssert.thatSingleton(sum(counts)).isEqualTo(FileCategoryCounts.of(FileCategory.DATA, 2));
    pipeline.run().waitUntilFinish();

    assertFalse(Files.exists(localPath(file)));
  }

  /**
   * HadoopFileIO succeeds silently on a missing file. The {@link NotFoundException} path is covered
   * by {@link #testDeletePathsCountsNotFoundAsDeleted}.
   */
  @Test
  public void testMissingFileCountsAsDeleted() throws IOException {
    String table = createTable("missing_file");
    String existing = createFile("existing.parquet");
    String missing = warehouse.location + "/files/missing.parquet";

    PCollection<FileCategoryCounts> counts =
        pipeline
            .apply(
                Create.of(
                    FileInfo.of(existing, FileCategory.DATA, false, table),
                    FileInfo.of(missing, FileCategory.DATA, false, table)))
            .apply(BatchDeleteFiles.of(catalogConfig()));
    PAssert.thatSingleton(sum(counts)).isEqualTo(FileCategoryCounts.of(FileCategory.DATA, 2));
    pipeline.run().waitUntilFinish();

    assertFalse(Files.exists(localPath(existing)));
  }

  @Test
  public void testFailedDeletionIsExcludedFromCounts() throws IOException {
    String table = createTable("failed_delete");
    String deletable = createFile("deletable.parquet");
    // HadoopFileIO deletes non-recursively, so deleting a non-empty directory fails.
    String nonEmptyDirectory = warehouse.location + "/files/directory";
    createFile("directory/child.parquet");

    PCollection<FileCategoryCounts> counts =
        pipeline
            .apply(
                Create.of(
                    FileInfo.of(deletable, FileCategory.DATA, false, table),
                    FileInfo.of(nonEmptyDirectory, FileCategory.MANIFEST, false, table)))
            // The bulk call still fails and falls back to per-file deletes, without Hadoop's
            // retry backoff.
            .apply(BatchDeleteFiles.of(noRetryCatalogConfig()));
    // Sharded keys may split even two files into separate batches, so assert on the sum.
    PAssert.thatSingleton(sum(counts)).isEqualTo(FileCategoryCounts.of(FileCategory.DATA, 1));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    assertFalse(Files.exists(localPath(deletable)));
    assertTrue(Files.exists(localPath(nonEmptyDirectory)));
    assertEquals(1, counter(result, BatchDeleteFiles.DELETED_FILES));
    assertEquals(1, counter(result, BatchDeleteFiles.FAILED_FILE_DELETIONS));
  }

  @Test
  public void testRejectsNonPositiveBatchSize() {
    BatchDeleteFiles transform = BatchDeleteFiles.of(catalogConfig());
    assertThrows(IllegalArgumentException.class, () -> transform.withBatchSize(0));
    assertThrows(IllegalArgumentException.class, () -> transform.withBatchSize(-1));
  }
}
