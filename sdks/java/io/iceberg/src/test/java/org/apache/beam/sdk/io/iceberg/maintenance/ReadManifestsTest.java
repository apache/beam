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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReadManifestsTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private IcebergCatalogConfig catalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogName("name")
        .setCatalogProperties(
            ImmutableMap.of(
                "type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, "warehouse", warehouse.location))
        .build();
  }

  private Table createTable(String name, int formatVersion) {
    return createTable(name, PartitionSpec.unpartitioned(), formatVersion);
  }

  private Table createTable(String name, PartitionSpec spec, int formatVersion) {
    return warehouse.createTable(
        TableIdentifier.of("default", name),
        TestFixtures.SCHEMA,
        spec,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)));
  }

  private DataFile dataFile(PartitionSpec spec, String name) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath(warehouse.location + "/data/" + name)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10L)
            .withRecordCount(1L);
    if (spec.isPartitioned()) {
      builder.withPartitionPath("data=a");
    }
    return builder.build();
  }

  private DeleteFile positionDeletes(Table table, String name) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath(warehouse.location + "/data/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10L)
        .withRecordCount(1L)
        .build();
  }

  private DeleteFile equalityDeletes(Table table, String name) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofEqualityDeletes(1)
        .withPath(warehouse.location + "/data/" + name)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10L)
        .withRecordCount(1L)
        .build();
  }

  /** Describes every manifest of the table's current snapshot. */
  private static List<ManifestDescriptor> describeCurrent(
      String tableId, Table table, boolean reachable) {
    List<ManifestDescriptor> descriptors = new ArrayList<>();
    for (ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
      descriptors.add(ManifestDescriptor.from(tableId, manifest, reachable));
    }
    return descriptors;
  }

  private PCollection<FileInfo> read(List<ManifestDescriptor> descriptors) {
    return pipeline.apply(Create.of(descriptors)).apply(ReadManifests.of(catalogConfig()));
  }

  private static long counter(PipelineResult result, String name) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(ReadManifests.class, name))
                    .build());
    long total = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      total += counter.getAttempted();
    }
    return total;
  }

  @Test
  public void testReadsDataManifests() {
    Table table = createTable("data_manifests", 2);
    DataFile file1 = dataFile(table.spec(), "file1.parquet");
    DataFile file2 = dataFile(table.spec(), "file2.parquet");
    DataFile file3 = dataFile(table.spec(), "file3.parquet");
    table.newAppend().appendFile(file1).appendFile(file2).commit();
    table.newAppend().appendFile(file3).commit();
    List<ManifestDescriptor> descriptors = describeCurrent("default.data_manifests", table, true);
    assertEquals(2, descriptors.size());

    PAssert.that(read(descriptors))
        .containsInAnyOrder(
            FileInfo.of(file1.location(), FileCategory.DATA, true, "default.data_manifests"),
            FileInfo.of(file2.location(), FileCategory.DATA, true, "default.data_manifests"),
            FileInfo.of(file3.location(), FileCategory.DATA, true, "default.data_manifests"));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    assertEquals(2, counter(result, ReadManifests.MANIFESTS_READ));
    assertEquals(3, counter(result, ReadManifests.MANIFEST_ENTRIES_READ));
  }

  @Test
  public void testReadsDeleteManifests() {
    Table table = createTable("delete_manifests", 2);
    DataFile data = dataFile(table.spec(), "data.parquet");
    table.newAppend().appendFile(data).commit();
    DeleteFile positionDeletes = positionDeletes(table, "pos-deletes.parquet");
    DeleteFile equalityDeletes = equalityDeletes(table, "eq-deletes.parquet");
    table.newRowDelta().addDeletes(positionDeletes).addDeletes(equalityDeletes).commit();
    List<ManifestDescriptor> descriptors =
        describeCurrent("default.delete_manifests", table, false);
    assertTrue(descriptors.stream().anyMatch(d -> d.getContent() == ManifestContent.DELETES));

    PAssert.that(read(descriptors))
        .containsInAnyOrder(
            FileInfo.of(data.location(), FileCategory.DATA, false, "default.delete_manifests"),
            FileInfo.of(
                positionDeletes.location(),
                FileCategory.POSITION_DELETES,
                false,
                "default.delete_manifests"),
            FileInfo.of(
                equalityDeletes.location(),
                FileCategory.EQUALITY_DELETES,
                false,
                "default.delete_manifests"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testEmitsLiveEntriesOnly() {
    Table table = createTable("live_entries", 2);
    DataFile removed = dataFile(table.spec(), "removed.parquet");
    DataFile kept = dataFile(table.spec(), "kept.parquet");
    table.newAppend().appendFile(removed).appendFile(kept).commit();
    table.newDelete().deleteFile(removed).commit();
    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    // The rewritten manifest still records the removed file, as a DELETED entry.
    assertTrue(manifests.stream().anyMatch(ManifestFile::hasDeletedFiles));

    PAssert.that(read(describeCurrent("default.live_entries", table, true)))
        .containsInAnyOrder(
            FileInfo.of(kept.location(), FileCategory.DATA, true, "default.live_entries"));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    assertEquals(1, counter(result, ReadManifests.MANIFEST_ENTRIES_READ));
  }

  /**
   * Writes a manifest under a partition spec that is not in the table's metadata, as happens after
   * unused specs are removed.
   */
  private ManifestFile writeManifestWithRemovedSpec(Table table, DataFile file, PartitionSpec spec)
      throws IOException {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            2,
            spec,
            table.io().newOutputFile(warehouse.location + "/metadata/removed-spec-m0.avro"),
            table.currentSnapshot().snapshotId());
    try {
      writer.add(file);
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private static final PartitionSpec REMOVED_SPEC =
      PartitionSpec.builderFor(TestFixtures.SCHEMA).withSpecId(7).identity("data").build();

  @Test
  public void testRemovedSpecWithoutCarriedSpecFails() throws IOException {
    Table table = createTable("removed_spec_fails", 2);
    table.newAppend().appendFile(dataFile(table.spec(), "existing.parquet")).commit();
    ManifestFile manifest =
        writeManifestWithRemovedSpec(
            table, dataFile(REMOVED_SPEC, "removed-spec.parquet"), REMOVED_SPEC);
    assertFalse(table.specs().containsKey(REMOVED_SPEC.specId()));

    read(
        Collections.singletonList(
            ManifestDescriptor.from("default.removed_spec_fails", manifest, false)));
    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run().waitUntilFinish());
    assertTrue(e.getCause() instanceof IllegalStateException);
    assertTrue(e.getCause().getMessage().contains(manifest.path()));
    assertTrue(e.getCause().getMessage().contains("partition spec 7"));
  }

  @Test
  public void testRemovedSpecWithCarriedSpecSucceeds() throws IOException {
    Table table = createTable("removed_spec_carried", 2);
    table.newAppend().appendFile(dataFile(table.spec(), "existing.parquet")).commit();
    DataFile file = dataFile(REMOVED_SPEC, "removed-spec.parquet");
    ManifestFile manifest = writeManifestWithRemovedSpec(table, file, REMOVED_SPEC);

    PAssert.that(
            read(
                Collections.singletonList(
                    ManifestDescriptor.from("default.removed_spec_carried", manifest, false)
                        .withPartitionSpec(REMOVED_SPEC))))
        .containsInAnyOrder(
            FileInfo.of(file.location(), FileCategory.DATA, false, "default.removed_spec_carried"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testRefreshesStaleCachedTableForNewSpec() {
    createTable("stale_cache", 2);
    // Cache the table before its spec evolves.
    Table cached = TableCache.get(catalogConfig(), "default.stale_cache");
    assertEquals(1, cached.specs().size());

    // Evolve the spec and append through a separate table instance.
    Table table = warehouse.loadTable(TableIdentifier.of("default", "stale_cache"));
    table.updateSpec().addField("data").commit();
    int newSpecId = table.spec().specId();
    DataFile file = dataFile(table.spec(), "partitioned.parquet");
    table.newAppend().appendFile(file).commit();
    assertFalse(cached.specs().containsKey(newSpecId));

    PAssert.that(read(describeCurrent("default.stale_cache", table, true)))
        .containsInAnyOrder(
            FileInfo.of(file.location(), FileCategory.DATA, true, "default.stale_cache"));
    pipeline.run().waitUntilFinish();

    // The reader refreshed the cached table to find the new spec.
    assertTrue(cached.specs().containsKey(newSpecId));
  }

  @Test
  public void testReadsMultipleTablesAndFormatVersions() {
    Table v1 = createTable("format_v1", 1);
    DataFile v1File = dataFile(v1.spec(), "v1.parquet");
    v1.newAppend().appendFile(v1File).commit();

    Table v3 = createTable("format_v3", 3);
    DataFile v3File = dataFile(v3.spec(), "v3.parquet");
    v3.newAppend().appendFile(v3File).commit();
    List<ManifestDescriptor> v3Descriptors = describeCurrent("default.format_v3", v3, false);
    assertNotNull(v3Descriptors.get(0).getFirstRowId());

    List<ManifestDescriptor> descriptors = new ArrayList<>();
    descriptors.addAll(describeCurrent("default.format_v1", v1, true));
    descriptors.addAll(v3Descriptors);

    PAssert.that(read(descriptors))
        .containsInAnyOrder(
            FileInfo.of(v1File.location(), FileCategory.DATA, true, "default.format_v1"),
            FileInfo.of(v3File.location(), FileCategory.DATA, false, "default.format_v3"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadsDeletionVectorsSharingPuffinFile() {
    Table table = createTable("deletion_vectors", 3);
    DataFile data1 = dataFile(table.spec(), "data1.parquet");
    DataFile data2 = dataFile(table.spec(), "data2.parquet");
    table.newAppend().appendFile(data1).appendFile(data2).commit();
    String puffin = warehouse.location + "/data/deletes.puffin";
    table
        .newRowDelta()
        .addDeletes(deletionVector(table, puffin, data1, 4L))
        .addDeletes(deletionVector(table, puffin, data2, 44L))
        .commit();

    // Both deletion vectors are stored in one Puffin file, so its path is emitted twice.
    FileInfo puffinInfo =
        FileInfo.of(puffin, FileCategory.POSITION_DELETES, true, "default.deletion_vectors");
    PAssert.that(read(describeCurrent("default.deletion_vectors", table, true)))
        .containsInAnyOrder(
            FileInfo.of(data1.location(), FileCategory.DATA, true, "default.deletion_vectors"),
            FileInfo.of(data2.location(), FileCategory.DATA, true, "default.deletion_vectors"),
            puffinInfo,
            puffinInfo);
    pipeline.run().waitUntilFinish();
  }

  private static DeleteFile deletionVector(
      Table table, String puffinPath, DataFile referenced, long offset) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(puffinPath)
        .withFileSizeInBytes(100L)
        .withRecordCount(1L)
        .withReferencedDataFile(referenced.location())
        .withContentOffset(offset)
        .withContentSizeInBytes(40L)
        .build();
  }

  @Test
  public void testRejectsEntriesReferencingManifests() {
    ManifestDescriptor descriptor =
        ManifestDescriptor.builder()
            .setTableIdentifier("default.nested")
            .setPath("/tmp/root-manifest.avro")
            .setLength(10L)
            .setPartitionSpecId(0)
            .setContent(ManifestContent.DATA)
            .setSnapshotId(1L)
            .setSequenceNumber(1L)
            .setReachable(true)
            .build();
    assertEquals(FileCategory.DATA, ReadManifests.category(FileContent.DATA, descriptor));
    for (FileContent content :
        new FileContent[] {FileContent.DATA_MANIFEST, FileContent.DELETE_MANIFEST}) {
      UnsupportedOperationException e =
          assertThrows(
              UnsupportedOperationException.class,
              () -> ReadManifests.category(content, descriptor));
      assertTrue(e.getMessage().contains("/tmp/root-manifest.avro"));
    }
  }

  @Test
  public void testRejectsFormatV4Table() {
    createTable("format_v4", 4);
    ManifestDescriptor descriptor =
        ManifestDescriptor.builder()
            .setTableIdentifier("default.format_v4")
            .setPath(warehouse.location + "/metadata/missing.avro")
            .setLength(10L)
            .setPartitionSpecId(0)
            .setContent(ManifestContent.DATA)
            .setSnapshotId(1L)
            .setSequenceNumber(1L)
            .setReachable(true)
            .build();

    read(Collections.singletonList(descriptor));
    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run().waitUntilFinish());
    assertTrue(e.getCause() instanceof UnsupportedOperationException);
    assertTrue(e.getCause().getMessage().contains("format version 4"));
    assertTrue(
        e.getCause()
            .getMessage()
            .contains(
                "highest supported format version is "
                    + ReadManifests.MAX_SUPPORTED_FORMAT_VERSION));
  }

  @Test
  public void testEmptyInput() throws Exception {
    PCollection<FileInfo> output =
        pipeline
            .apply(
                Create.empty(
                    SchemaRegistry.createDefault().getSchemaCoder(ManifestDescriptor.class)))
            .apply(ReadManifests.of(catalogConfig()));
    PAssert.that(output).empty();
    pipeline.run().waitUntilFinish();
  }
}
