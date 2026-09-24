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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.PREFIX_ERROR;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.UNKNOWN_PARTITION_ERROR;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.getPartitionFromMetrics;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.UnverifiableFileHandling;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AddFilesTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private String root;
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private HadoopCatalog catalog;
  private TableIdentifier tableId;
  private final Schema icebergSchema =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.required(3, "age", Types.IntegerType.get()));
  private final List<String> partitionFields = Arrays.asList("age", "truncate(name, 3)");
  private final PartitionSpec spec = PartitionUtils.toPartitionSpec(partitionFields, icebergSchema);
  private final PartitionKey wrapper = new PartitionKey(spec, icebergSchema);
  private final Map<String, String> tableProps =
      ImmutableMap.of("write.metadata.metrics.default", "full", "foo", "bar");
  private IcebergCatalogConfig catalogConfig;
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestName testName = new TestName();
  @Rule public ExpectedLogs logs = ExpectedLogs.none(AddFiles.class);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Before
  public void setup() throws Exception {
    // Root for existing data files:
    root = temp.getRoot().getAbsolutePath() + "/";

    // Set up a local Hadoop Catalog
    catalog = new HadoopCatalog(new Configuration(), warehouse.location);
    tableId = TableIdentifier.of("default", testName.getMethodName());

    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
  }

  @Test
  public void testAddPartitionedFiles() throws Exception {
    testAddFilesWithPartitionPath(true);
  }

  @Test
  public void testAddUnPartitionedFiles() throws Exception {
    testAddFilesWithPartitionPath(false);
  }

  public void testAddFilesWithPartitionPath(boolean isPartitioned) throws Exception {
    // 1. Generate two local Parquet file.
    // Include Hive-like partition path if testing partition case
    String partitionPath1 = isPartitioned ? "age=20/name_trunc=Mar/" : "";
    String file1 = root + partitionPath1 + "data1.parquet";
    wrapper.wrap(record(-1, "Mar", 20));
    DataWriter<Record> writer = createWriter(file1, isPartitioned ? wrapper.copy() : null);
    writer.write(record(1, "Mark", 20));
    writer.write(record(2, "Martin", 20));
    writer.close();

    String partitionPath2 = isPartitioned ? "age=25/name_trunc=Sam/" : "";
    String file2 = root + partitionPath2 + "data2.parquet";
    wrapper.wrap(record(-1, "Sam", 25));
    DataWriter<Record> writer2 = createWriter(file2, isPartitioned ? wrapper.copy() : null);
    writer2.write(record(3, "Samantha", 25));
    writer2.write(record(4, "Sammy", 25));
    writer2.close();

    // 2. Setup the input PCollection
    PCollection<String> inputFiles = pipeline.apply("Create Input", Create.of(file1, file2));

    // 3. Apply the transform (Trigger aggressively for testing)
    PCollectionRowTuple output =
        inputFiles.apply(
            new AddFiles(
                catalogConfig,
                tableId.toString(),
                isPartitioned ? root : null,
                isPartitioned ? partitionFields : null,
                null,
                tableProps,
                null,
                null));

    // 4. Validate PCollection Outputs
    PAssert.that(output.get("errors")).empty();

    // 5. Run the pipeline
    pipeline.run().waitUntilFinish();

    // 6. Validate the Iceberg Table was created with the correct spec and properties
    Table table = catalog.loadTable(tableId);
    tableProps.forEach((key, value) -> assertThat(table.properties(), hasEntry(key, value)));
    assertEquals(isPartitioned ? spec : PartitionSpec.unpartitioned(), table.spec());

    // Check that we have exactly 1 snapshot with 2 files
    assertEquals(1, Iterables.size(table.snapshots()));

    List<DataFile> addedFiles =
        Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
    assertEquals(2, addedFiles.size());

    // Verify file paths
    assertTrue(addedFiles.stream().anyMatch(df -> df.location().contains("data1.parquet")));
    assertTrue(addedFiles.stream().anyMatch(df -> df.location().contains("data2.parquet")));

    // check metrics metadata is preserved
    DataFile writtenDf1 = writer.toDataFile();
    DataFile writtenDf2 = writer2.toDataFile();
    DataFile addedDf1 =
        Iterables.getOnlyElement(
            addedFiles.stream()
                .filter(df -> df.location().contains("data1.parquet"))
                .collect(Collectors.toList()));
    DataFile addedDf2 =
        Iterables.getOnlyElement(
            addedFiles.stream()
                .filter(df -> df.location().contains("data2.parquet"))
                .collect(Collectors.toList()));

    assertEquals(writtenDf1.lowerBounds(), addedDf1.lowerBounds());
    assertEquals(writtenDf1.upperBounds(), addedDf1.upperBounds());
    assertEquals(writtenDf2.lowerBounds(), addedDf2.lowerBounds());
    assertEquals(writtenDf2.upperBounds(), addedDf2.upperBounds());

    // check partition metadata is preserved
    assertEquals(writtenDf1.partition(), addedDf1.partition());
    assertEquals(writtenDf2.partition(), addedDf2.partition());

    // check that mapping util was added
    assertEquals(
        MappingUtil.create(icebergSchema).asMappedFields(),
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING))
            .asMappedFields());
  }

  @Test
  public void testAddFilesWithPartitionFromMetrics() throws IOException {
    // 1. Generate local Parquet files with no directory structure.
    String file1 = root + "data1.parquet";
    DataWriter<Record> writer = createWriter(file1);
    writer.write(record(1, "Mark", 20));
    writer.write(record(2, "Martin", 20));
    writer.close();
    PartitionData expectedPartition1 = new PartitionData(spec.partitionType());
    expectedPartition1.set(0, 20);
    expectedPartition1.set(1, "Mar");

    String file2 = root + "data2.parquet";
    DataWriter<Record> writer2 = createWriter(file2);
    writer2.write(record(3, "Samantha", 25));
    writer2.write(record(4, "Sammy", 25));
    writer2.close();
    PartitionData expectedPartition2 = new PartitionData(spec.partitionType());
    expectedPartition2.set(0, 25);
    expectedPartition2.set(1, "Sam");

    // Also create a "bad" DataFile, containing values that correspond to different partitions
    // This file should get output to the DLQ, because we cannot determine its partition
    String file3 = root + "data3.parquet";
    DataWriter<Record> writer3 = createWriter(file3);
    writer3.write(record(5, "Johnny", 25));
    writer3.write(record(6, "Yaseen", 32));
    writer3.close();

    // 2. Setup the input PCollection
    PCollection<String> inputFiles = pipeline.apply("Create Input", Create.of(file1, file2, file3));

    // 3. Apply the transform (Trigger aggressively for testing)
    PCollectionRowTuple output =
        inputFiles.apply(
            new AddFiles(
                catalogConfig,
                tableId.toString(),
                null, // no prefix, so determine partition from DF metrics
                partitionFields,
                null,
                tableProps,
                null,
                null));

    // 4. There should be an error for File3, because its partition could not be determined
    PAssert.that(output.get("errors"))
        .satisfies(
            errorRows -> {
              Row errorRow = Iterables.getOnlyElement(errorRows);
              checkState(
                  errorRow.getSchema().equals(AddFiles.ERROR_SCHEMA)
                      && file3.equals(errorRow.getString(0))
                      && checkStateNotNull(errorRow.getString(1))
                          .startsWith(UNKNOWN_PARTITION_ERROR));
              return null;
            });

    // 5. Run the pipeline
    pipeline.run().waitUntilFinish();

    // 6. Validate the Iceberg Table was created with the correct spec and properties
    Table table = catalog.loadTable(tableId);
    tableProps.forEach((key, value) -> assertThat(table.properties(), hasEntry(key, value)));
    assertEquals(spec, table.spec());

    // Check that we have exactly 1 snapshot with 2 files
    assertEquals(1, Iterables.size(table.snapshots()));

    List<DataFile> addedFiles =
        Lists.newArrayList(table.currentSnapshot().addedDataFiles(table.io()));
    assertEquals(2, addedFiles.size());

    // Verify file paths
    assertTrue(addedFiles.stream().anyMatch(df -> df.location().contains("data1.parquet")));
    assertTrue(addedFiles.stream().anyMatch(df -> df.location().contains("data2.parquet")));

    // check metrics metadata is preserved
    DataFile writtenDf1 = writer.toDataFile();
    DataFile writtenDf2 = writer2.toDataFile();
    DataFile addedDf1 =
        Iterables.getOnlyElement(
            addedFiles.stream()
                .filter(df -> df.location().contains("data1.parquet"))
                .collect(Collectors.toList()));
    DataFile addedDf2 =
        Iterables.getOnlyElement(
            addedFiles.stream()
                .filter(df -> df.location().contains("data2.parquet"))
                .collect(Collectors.toList()));

    assertEquals(writtenDf1.lowerBounds(), addedDf1.lowerBounds());
    assertEquals(writtenDf1.upperBounds(), addedDf1.upperBounds());
    assertEquals(writtenDf2.lowerBounds(), addedDf2.lowerBounds());
    assertEquals(writtenDf2.upperBounds(), addedDf2.upperBounds());

    // check partition metadata is preserved
    assertEquals(expectedPartition1, addedDf1.partition());
    assertEquals(expectedPartition2, addedDf2.partition());

    assertEquals(
        MappingUtil.create(icebergSchema).asMappedFields(),
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING))
            .asMappedFields());
  }

  @Test
  public void testStreamingAdds() throws IOException {
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String file = String.format("%sdata_%s.parquet", root, i);
      DataWriter<Record> writer = createWriter(file);
      writer.write(record(1, "SomeName", 30));
      writer.close();
      paths.add(file);
    }

    PCollection<String> files =
        pipeline.apply(
            TestStream.create(StringUtf8Coder.of())
                .addElements(
                    paths.get(0),
                    paths.subList(1, 15).toArray(new String[] {})) // should add one manifest file
                .advanceProcessingTime(Duration.standardSeconds(10))
                .addElements(
                    paths.get(15),
                    paths.subList(16, 40).toArray(new String[] {})) // should add 3 manifest files
                .advanceProcessingTime(Duration.standardSeconds(10))
                .addElements(
                    paths.get(40),
                    paths.subList(41, 45).toArray(new String[] {})) // should add one manifest file
                .advanceWatermarkToInfinity());

    files.apply(
        new AddFiles(
            catalogConfig,
            tableId.toString(),
            null,
            null,
            null,
            null,
            10, // trigger at 10 files
            Duration.standardSeconds(5)));
    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(tableId);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis));
    List<ManifestFile> manifests = Iterables.getLast(snapshots).allManifests(table.io());
    manifests.sort(Comparator.comparingLong(ManifestFile::sequenceNumber));

    assertEquals(6, manifests.size());
    assertEquals(10, (int) manifests.get(0).addedFilesCount());
    assertEquals(5, (int) manifests.get(1).addedFilesCount());
    assertEquals(10, (int) manifests.get(2).addedFilesCount());
    assertEquals(10, (int) manifests.get(3).addedFilesCount());
    assertEquals(5, (int) manifests.get(4).addedFilesCount());
    assertEquals(5, (int) manifests.get(5).addedFilesCount());
  }

  @Test
  public void testUnknownFormatErrors() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    // Create a dummy text file (unsupported extension)
    File txtFile = temp.newFile("unsupported.txt");
    txtFile.createNewFile();

    PCollection<String> inputFiles =
        pipeline.apply("Create Input", Create.of(txtFile.getAbsolutePath()));

    AddFiles addFiles =
        new AddFiles(catalogConfig, tableId.toString(), null, null, null, null, null, null);
    PCollectionRowTuple outputTuple = inputFiles.apply(addFiles);

    // Validate the file ended up in the errors PCollection with the correct schema
    PAssert.that(outputTuple.get("errors"))
        .containsInAnyOrder(
            Row.withSchema(AddFiles.ERROR_SCHEMA)
                .addValues(txtFile.getAbsolutePath(), "Could not determine the file's format")
                .build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPartitionPrefixErrors() throws Exception {
    // Drop unpartitioned table and create a partitioned one
    catalog.dropTable(tableId);
    PartitionSpec spec = PartitionSpec.builderFor(icebergSchema).identity("name").build();
    catalog.createTable(tableId, icebergSchema, spec);

    String file1 = root + "data1.parquet";
    wrapper.wrap(record(-1, "And", 30));
    DataWriter<Record> writer = createWriter(file1, wrapper.copy());
    writer.write(record(1, "Andrew", 30));
    writer.close();

    PCollection<String> inputFiles = pipeline.apply("Create Input", Create.of(file1));

    // Notice locationPrefix is "some/prefix/" but the absolute path doesn't start with it
    AddFiles addFiles =
        new AddFiles(
            catalogConfig, tableId.toString(), "some/prefix/", null, null, null, null, null);
    PCollectionRowTuple outputTuple = inputFiles.apply(addFiles);

    PAssert.that(outputTuple.get("errors"))
        .containsInAnyOrder(
            Row.withSchema(AddFiles.ERROR_SCHEMA).addValues(file1, PREFIX_ERROR).build());

    pipeline.run().waitUntilFinish();
  }

  /**
   * We reverted the in-depth bucket-partition validation in
   * https://github.com/apache/beam/pull/38039, partly because it was too resource intensive, and
   * also because the Spark AddFiles equivalent performs zero validation.
   */
  @Ignore
  @Test
  public void testRecognizesBucketPartitionMismatch() throws IOException {
    String file1 = root + "data1.parquet";
    wrapper.wrap(record(-1, "And", 30));
    DataWriter<Record> writer = createWriter(file1, wrapper.copy());
    writer.write(record(1, "Andrew", 30));
    writer.write(record(5, "Sally", 30));
    writer.write(record(10, "Ahmed", 30));
    writer.close();

    // 1 (min) and 10 (max) will transform to bucket=0
    // 5 (some middle value) transforms to bucket=1
    // To prove this transform value mapping^, below is a sanity check.
    // We should recognize that we cannot assign a partition to such a file, and pass it to DLQ.
    List<String> partitionFields = Arrays.asList("bucket(id, 2)", "age");
    PartitionSpec spec = PartitionUtils.toPartitionSpec(partitionFields, icebergSchema);
    PartitionField bucketPartition = spec.fields().get(0);
    assertEquals("id_bucket", bucketPartition.name());
    assertTrue(bucketPartition.transform().toString().contains("bucket["));
    SerializableFunction<Long, Integer> transformFunc =
        (SerializableFunction<Long, Integer>)
            bucketPartition.transform().bind(Types.LongType.get());
    assertEquals(0, (int) transformFunc.apply(1L));
    assertEquals(1, (int) transformFunc.apply(5L));
    assertEquals(0, (int) transformFunc.apply(10L));

    AddFiles addFiles =
        new AddFiles(
            catalogConfig, tableId.toString(), null, partitionFields, null, null, null, null);
    PCollection<String> inputFiles = pipeline.apply("Create Input", Create.of(file1));
    PCollectionRowTuple outputTuple = inputFiles.apply(addFiles);

    PAssert.that(outputTuple.get("errors"))
        .containsInAnyOrder(
            Row.withSchema(AddFiles.ERROR_SCHEMA)
                .addValues(
                    file1,
                    UNKNOWN_PARTITION_ERROR
                        + "Found records with conflicting transformed values, for column: id")
                .build());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCatchFileNotFoundException() throws IOException {
    String file = root + "non-existent.parquet";

    PCollectionRowTuple outputTuple =
        pipeline
            .apply("Create Input", Create.of(file))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));

    PAssert.that(outputTuple.get("errors"))
        .satisfies(
            rows -> {
              Row error = Iterables.getOnlyElement(rows);
              String errorFile = error.getString("file");
              String message = error.getString("error");

              assertEquals(file, errorFile);
              assertThat(message, containsString("No files found"));
              assertThat(message, containsString(errorFile));
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  /** Infrastructure failures must fail the bundle, not become silently-dropped error rows. */
  @Test
  public void testCatalogOutageFailsBundleInsteadOfDlq() throws Exception {
    String file1 = root + "data1.parquet";
    DataWriter<Record> writer = createWriter(file1);
    writer.write(record(1, "Mark", 20));
    writer.close();

    IcebergCatalogConfig unreachable =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(ImmutableMap.of("type", "rest", "uri", "http://localhost:1"))
            .build();
    pipeline
        .apply("Create Input", Create.of(file1))
        .apply(new AddFiles(unreachable, tableId.toString(), null, null, null, null, null, null));
    assertThrows(Exception.class, () -> pipeline.run().waitUntilFinish());
  }

  /** A file deleted between listing and registration is one error row, never a stuck bundle. */
  @Test
  public void testMissingFileWithExistingTableRoutesToDlq() {
    catalog.createTable(tableId, icebergSchema);
    String missing = root + "missing.parquet";

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(missing))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));
    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              Row row = Iterables.getOnlyElement(rows);
              assertEquals(missing, row.getString("file"));
              assertThat(row.getString("error"), containsString("No files found"));
              return null;
            });
    pipeline.run().waitUntilFinish();

    assertEquals(0, Iterables.size(catalog.loadTable(tableId).snapshots()));
  }

  @Test
  public void testGarbageParquetWithExistingTableRoutesToDlq() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    File garbage = temp.newFile("garbage.parquet");
    java.nio.file.Files.write(
        garbage.toPath(), "not parquet".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    String file = garbage.getAbsolutePath();

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(file))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));
    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              Row row = Iterables.getOnlyElement(rows);
              assertEquals(file, row.getString("file"));
              assertNotNull(row.getString("error"));
              return null;
            });
    pipeline.run().waitUntilFinish();

    assertEquals(0, Iterables.size(catalog.loadTable(tableId).snapshots()));
  }

  @Test
  public void testStaleNameMappingRegeneratedAtCommit() throws Exception {
    Table table = catalog.createTable(tableId, icebergSchema);
    // A mapping generated against an older version of the schema (covers only "id"), carrying a
    // user-added alias.
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            json("[ {'field-id': 1, 'names': ['id', 'ident']} ]"))
        .commit();

    String file1 = root + "data1.parquet";
    DataWriter<Record> writer = createWriter(file1);
    writer.write(record(1, "Mark", 20));
    writer.close();

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(file1))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));
    PAssert.that(output.get("errors")).empty();
    pipeline.run().waitUntilFinish();

    // Regenerated from the schema, with the user's alias carried over.
    NameMapping expected =
        NameMappingParser.fromJson(
            json(
                "[ {'field-id': 1, 'names': ['id', 'ident']},"
                    + "  {'field-id': 2, 'names': ['name']},"
                    + "  {'field-id': 3, 'names': ['age']} ]"));
    assertEquals(expected.asMappedFields(), currentMapping().asMappedFields());
  }

  /**
   * The stale mapping resolves every top-level name but is missing a field nested inside a {@code
   * list<struct>}; only the recursive coverage walk can detect it.
   */
  @Test
  public void testStaleNestedNameMappingRegeneratedAtCommit() throws Exception {
    Schema tableSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(
                4,
                "events",
                Types.ListType.ofOptional(
                    5,
                    Types.StructType.of(
                        Types.NestedField.optional(6, "a", Types.IntegerType.get()),
                        Types.NestedField.optional(7, "b", Types.StringType.get())))));
    Table table = catalog.createTable(tableId, tableSchema);
    Schema staleView =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(
                4,
                "events",
                Types.ListType.ofOptional(
                    5,
                    Types.StructType.of(
                        Types.NestedField.optional(6, "a", Types.IntegerType.get())))));
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(staleView)))
        .commit();

    String file1 = root + "data1.parquet";
    DataWriter<Record> writer = createWriter(file1);
    writer.write(record(1, "Mark", 20));
    writer.close();

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(file1))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));
    PAssert.that(output.get("errors")).empty();
    pipeline.run().waitUntilFinish();

    assertEquals(
        MappingUtil.create(tableSchema).asMappedFields(), currentMapping().asMappedFields());
  }

  @Test
  public void testMalformedNameMappingRegeneratedAtCommit() throws Exception {
    Table table = catalog.createTable(tableId, icebergSchema);
    // Duplicate names make NameMappingParser.fromJson throw eagerly.
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            json("[ {'field-id': 1, 'names': ['dup']}, {'field-id': 2, 'names': ['dup']} ]"))
        .commit();

    String file1 = root + "data1.parquet";
    DataWriter<Record> writer = createWriter(file1);
    writer.write(record(1, "Mark", 20));
    writer.close();

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(file1))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));
    PAssert.that(output.get("errors")).empty();
    pipeline.run().waitUntilFinish();

    assertEquals(
        MappingUtil.create(icebergSchema).asMappedFields(), currentMapping().asMappedFields());
  }

  /** A mapping that already covers the schema is never rewritten, whatever custom names it has. */
  @Test
  public void testHealthyCustomizedMappingLeftUntouched() throws Exception {
    Table table = catalog.createTable(tableId, icebergSchema);
    String custom =
        json(
            "[ {'field-id': 1, 'names': ['id', 'ident']},"
                + "  {'field-id': 2, 'names': ['name']},"
                + "  {'field-id': 3, 'names': ['age']} ]");
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, custom).commit();

    String file1 = root + "data1.parquet";
    DataWriter<Record> writer = createWriter(file1);
    writer.write(record(1, "Mark", 20));
    writer.close();

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(file1))
            .apply(
                new AddFiles(
                    catalogConfig, tableId.toString(), null, null, null, null, null, null));
    PAssert.that(output.get("errors")).empty();
    pipeline.run().waitUntilFinish();

    assertEquals(
        custom, catalog.loadTable(tableId).properties().get(TableProperties.DEFAULT_NAME_MAPPING));
  }

  /** Single-quoted JSON keeps test literals free of escape noise. */
  private static String json(String singleQuoted) {
    return singleQuoted.replace('\'', '"');
  }

  private NameMapping currentMapping() {
    return NameMappingParser.fromJson(
        catalog.loadTable(tableId).properties().get(TableProperties.DEFAULT_NAME_MAPPING));
  }

  @Test
  public void testErrorMessageToleratesNullMessage() {
    assertEquals("java.io.IOException", AddFiles.errorMessage(new IOException((String) null)));
    assertEquals("boom", AddFiles.errorMessage(new IOException("boom")));
  }

  @Test
  public void testGetPartitionFromMetrics() throws IOException, InterruptedException {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .bucket("id", 2)
            .truncate("name", 4)
            .identity("age")
            .build();

    List<PartitionTestCase> testCases =
        Arrays.asList(
            PartitionTestCase.of(
                root + "data_1.parquet",
                record(1, "aaaa", 10),
                Arrays.asList(
                    record(1, "aaaa123", 10),
                    record(10, "aaaa789", 10),
                    record(100, "aaaa456", 10)),
                Arrays.asList(1, CharBuffer.wrap("aaaa123"), 10),
                Arrays.asList(100, CharBuffer.wrap("aaaa789"), 10),
                "id_bucket=0/name_trunc=aaaa/age=10"),
            PartitionTestCase.of(
                root + "data_2.parquet",
                record(1, "bbbb", 30),
                Arrays.asList(
                    record(5, "bbbb789", 30),
                    record(55, "bbbb456", 30),
                    record(500, "bbbb123", 30)),
                Arrays.asList(5, CharBuffer.wrap("bbbb123"), 30),
                Arrays.asList(500, CharBuffer.wrap("bbbb789"), 30),
                "id_bucket=1/name_trunc=bbbb/age=30"));

    PartitionKey pk = new PartitionKey(partitionSpec, icebergSchema);
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProps);
    Table table = catalog.createTable(tableId, icebergSchema, partitionSpec);

    for (PartitionTestCase caze : testCases) {
      List<Record> records = caze.records;
      String fileName = caze.fileName;
      pk.wrap(caze.partition);
      DataWriter<Record> writer = createWriter(fileName, pk.copy());

      for (Record record : records) {
        writer.write(record);
      }
      writer.close();
      InputFile file = table.io().newInputFile(fileName);

      ParquetMetadata footer = ParquetFooters.read(fileName);
      Metrics metrics =
          AddFiles.getFileMetrics(
              file, FileFormat.PARQUET, metricsConfig, MappingUtil.create(icebergSchema), footer);
      for (int i = 0; i < partitionSpec.fields().size(); i++) {
        PartitionField partitionField = partitionSpec.fields().get(i);
        Types.NestedField field = icebergSchema.findField(partitionField.sourceId());
        ByteBuffer lowerBytes = metrics.lowerBounds().get(field.fieldId());
        ByteBuffer upperBytes = metrics.upperBounds().get(field.fieldId());

        Object lower = Conversions.fromByteBuffer(field.type(), lowerBytes);
        Object upper = Conversions.fromByteBuffer(field.type(), upperBytes);

        assertEquals(caze.expectedLower.get(i), lower);
        assertEquals(caze.expectedUpper.get(i), upper);
      }

      String partitionPath = getPartitionFromMetrics(metrics, file, table, footer);
      assertEquals(caze.expectedPartition, partitionPath);
    }
  }

  @Test
  public void testThrowPartitionMismatchError() throws IOException, InterruptedException {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(icebergSchema)
            .bucket("id", 2)
            .truncate("name", 4)
            .identity("age")
            .build();

    List<PartitionTestCase> testCases =
        Arrays.asList(
            PartitionTestCase.of(
                root + "data_1.parquet",
                record(1, "aaaa", 10),
                Arrays.asList(
                    record(1, "aaaa123", 10), record(10, "abab", 10), record(100, "aaaa789", 10)),
                Arrays.asList(1, CharBuffer.wrap("aaaa123"), 10),
                Arrays.asList(100, CharBuffer.wrap("abab"), 10),
                "error"),
            PartitionTestCase.of(
                root + "data_2.parquet",
                record(1, "bbbb", 30),
                Arrays.asList(
                    record(5, "bbbb", 30), record(55, "bbbb", 30), record(500, "bbbb", 50)),
                Arrays.asList(5, CharBuffer.wrap("bbbb"), 30),
                Arrays.asList(500, CharBuffer.wrap("bbbb"), 50),
                "error"));

    PartitionKey pk = new PartitionKey(partitionSpec, icebergSchema);
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProps);
    Table table = catalog.createTable(tableId, icebergSchema, partitionSpec);

    for (PartitionTestCase caze : testCases) {
      List<Record> records = caze.records;
      String fileName = caze.fileName;
      pk.wrap(caze.partition);
      DataWriter<Record> writer = createWriter(fileName, pk.copy());

      for (Record record : records) {
        writer.write(record);
      }
      writer.close();
      InputFile file = table.io().newInputFile(fileName);

      ParquetMetadata footer = ParquetFooters.read(fileName);
      Metrics metrics =
          AddFiles.getFileMetrics(
              file, FileFormat.PARQUET, metricsConfig, MappingUtil.create(icebergSchema), footer);
      // check that lower/upper stats are still fetched correctly
      for (int i = 0; i < partitionSpec.fields().size(); i++) {
        PartitionField partitionField = partitionSpec.fields().get(i);
        Types.NestedField field = icebergSchema.findField(partitionField.sourceId());
        ByteBuffer lowerBytes = metrics.lowerBounds().get(field.fieldId());
        ByteBuffer upperBytes = metrics.upperBounds().get(field.fieldId());

        Object lower = Conversions.fromByteBuffer(field.type(), lowerBytes);
        Object upper = Conversions.fromByteBuffer(field.type(), upperBytes);

        assertEquals(caze.expectedLower.get(i), lower);
        assertEquals(caze.expectedUpper.get(i), upper);
      }

      assertThrows(
          AddFiles.UnknownPartitionException.class,
          () -> getPartitionFromMetrics(metrics, file, table, footer));
    }
  }

  static class PartitionTestCase {
    String fileName;
    StructLike partition;
    List<Record> records;
    List<Object> expectedLower;
    List<Object> expectedUpper;
    String expectedPartition;

    PartitionTestCase(
        String fileName,
        StructLike partition,
        List<Record> records,
        List<Object> expectedLower,
        List<Object> expectedUpper,
        String expectedPartition) {
      this.fileName = fileName;
      this.partition = partition;
      this.records = records;
      this.expectedLower = expectedLower;
      this.expectedUpper = expectedUpper;
      this.expectedPartition = expectedPartition;
    }

    static PartitionTestCase of(
        String fileName,
        StructLike partition,
        List<Record> records,
        List<Object> expectedLower,
        List<Object> expectedUpper,
        String expectedPartition) {
      return new PartitionTestCase(
          fileName, partition, records, expectedLower, expectedUpper, expectedPartition);
    }
  }

  private DataWriter<Record> createWriter(String file) throws IOException {
    return createWriter(file, null);
  }

  private DataWriter<Record> createWriter(String file, @Nullable StructLike partition)
      throws IOException {
    return Parquet.writeData(Files.localOutput(file))
        .schema(icebergSchema)
        .withSpec(partition != null ? spec : PartitionSpec.unpartitioned())
        .withPartition(partition)
        .createWriterFunc(GenericParquetWriter::create)
        .build();
  }

  private Record record(int id, String name, int age) {
    return GenericRecord.create(icebergSchema).copy("id", id, "name", name, "age", age);
  }

  // ---- AddFiles with schema evolution

  private AddFiles addFiles(@Nullable SchemaEvolutionConfig config) {
    return new AddFiles(
        catalogConfig, tableId.toString(), null, null, null, null, null, null, config);
  }

  private static int countTransforms(Pipeline pipeline, String name) {
    int[] count = {0};
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (node.getFullName().contains(name)) {
              count[0]++;
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    return count[0];
  }

  /** A file whose name column is an int: a type conflict no option allows. */
  private String writeConflicting(String name) throws IOException {
    Schema conflicting =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.IntegerType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()));
    Record record = GenericRecord.create(conflicting);
    record.setField("id", 1);
    record.setField("name", 5);
    record.setField("age", 1);
    return writeWithSchema(name, conflicting, record);
  }

  private static Map<Integer, Long> nullCountsOf(Table table, String fileName) {
    for (FileScanTask task : table.newScan().includeColumnStats().planFiles()) {
      if (task.file().path().toString().endsWith(fileName)) {
        return checkStateNotNull(task.file().nullValueCounts());
      }
    }
    throw new AssertionError(fileName + " is not registered");
  }

  private void assertEmailAddedAndFilesRegistered(int files) {
    Table table = catalog.loadTable(tableId);
    Types.NestedField email = table.schema().findField("email");
    assertNotNull(email);
    assertTrue(email.isOptional());
    assertEquals(files, Iterables.size(table.newScan().planFiles()));
  }

  @Test
  public void testEvolutionAddsColumnsBeforeRegisteringFiles() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String narrow = writeOneRecord("narrow.parquet");
    String wide = writeWider("wide.parquet");

    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(narrow, wide)).apply(addFiles(ADDITIONS));
    PAssert.that(output.get("errors")).empty();
    assertEquals(1, countTransforms(pipeline, "ReadFooterSchema"));

    pipeline.run().waitUntilFinish();

    assertEmailAddedAndFilesRegistered(2);
    Table table = catalog.loadTable(tableId);
    assertEquals(1, Iterables.size(table.snapshots()));
    int emailId = table.schema().findField("email").fieldId();
    assertTrue(
        "stats for the added column", nullCountsOf(table, "wide.parquet").containsKey(emailId));
  }

  @Test
  public void testDryRunReportsWithoutCommittingOrRegistering() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String before = metadataLocation();
    String covered = writeOneRecord("covered.parquet");
    String wide = writeWider("wide.parquet");
    String conflict = writeConflicting("conflict.parquet");

    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(covered, wide, conflict)).apply(addFiles(DRY_RUN));
    PAssert.that(output.get("errors")).empty();
    PAssert.that(output.get("snapshots")).empty();
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              Collection<Row> schemas = schemas(report);
              assertEquals(3, schemas.size());
              boolean sawAddition = false;
              boolean sawConflict = false;
              for (Row schema : schemas) {
                Collection<String> changes = schema.getArray("changes");
                if (changes.contains("add optional email string")) {
                  sawAddition = schema.getBoolean("allowed");
                }
                if (!schema.getBoolean("allowed")) {
                  sawConflict = schema.getString("reason").contains("conflicts");
                }
              }
              assertTrue(sawAddition);
              assertTrue(sawConflict);
              assertFalse(report.getBoolean("allowed"));
              assertEquals(Long.valueOf(2), report.getInt64("files_allowed"));
              assertEquals(Long.valueOf(1), report.getInt64("files_incompatible"));
              assertThat(report.getString("reason"), containsString("would fail"));
              return null;
            });
    assertEquals(0, countTransforms(pipeline, "ConvertToDataFiles"));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    Table table = catalog.loadTable(tableId);
    assertEquals(0, Iterables.size(table.snapshots()));
    assertEquals(2, counted(result, DryRunReport.class, DryRunReport.FILES_ALLOWED_COUNTER));
    assertEquals(1, counted(result, DryRunReport.class, DryRunReport.FILES_INCOMPATIBLE_COUNTER));
    assertEquals(0, counted(result, DryRunReport.class, DryRunReport.FILES_UNREADABLE_COUNTER));
    assertEquals(0, counted(result, DryRunReport.class, DryRunReport.CONFIG_PROBLEMS_COUNTER));
    assertEquals(before, metadataLocation());
  }

  private String metadataLocation() {
    return ((BaseTable) catalog.loadTable(tableId)).operations().current().metadataFileLocation();
  }

  private static Row report(Iterable<Row> rows) {
    return Iterables.getOnlyElement(rows);
  }

  private static Collection<Row> schemas(Row report) {
    return checkStateNotNull(report.getArray("schemas"));
  }

  @Test
  public void testDryRunAgainstMissingTableReportsCreation() throws Exception {
    String wide = writeWider("wide.parquet");
    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(wide)).apply(addFiles(DRY_RUN));
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              assertTrue(report.getBoolean("allowed"));
              assertTrue(report.getBoolean("would_create_table"));
              Row created = checkStateNotNull(report.getRow("created_table"));
              assertThat(
                  created.getArray("columns").toString(),
                  containsString("create optional email string"));
              assertThat(created.getString("schema"), containsString("\"name\":\"email\""));
              for (Row schema : schemas(report)) {
                assertTrue(
                    "the created table is described once", schema.getArray("changes").isEmpty());
              }
              return null;
            });
    pipeline.run().waitUntilFinish();
    assertFalse(catalog.tableExists(tableId));
  }

  /** A real run under FAIL_PIPELINE throws before creating the table, and the report says so. */
  @Test
  public void testDryRunAgainstMissingTableDoesNotCreateWhenARealRunWouldFail() throws Exception {
    String wide = writeWider("wide.parquet");
    String conflict = writeConflicting("conflict.parquet");
    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(wide, conflict)).apply(addFiles(DRY_RUN));
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              assertFalse(report.getBoolean("allowed"));
              assertFalse(report.getBoolean("would_create_table"));
              assertNotNull("the union still describes the table", report.getRow("created_table"));
              assertThat(report.getString("reason"), containsString("would fail"));
              return null;
            });
    pipeline.run().waitUntilFinish();
    assertFalse(catalog.tableExists(tableId));
  }

  /** A table-level change without a schema change is reported as such. */
  @Test
  public void testDryRunReportsNameMappingRepair() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String before = metadataLocation();
    String covered = writeOneRecord("covered.parquet");
    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(covered)).apply(addFiles(DRY_RUN));
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              assertTrue(report.getBoolean("allowed"));
              assertEquals(
                  Arrays.asList(DryRunReport.NAME_MAPPING_CHANGE),
                  new ArrayList<>(checkStateNotNull(report.getArray("table_changes"))));
              return null;
            });
    pipeline.run().waitUntilFinish();
    assertEquals(before, metadataLocation());
  }

  /** Creation settings are part of the plan: a partition field the union lacks is reported. */
  @Test
  public void testDryRunReportsCreationBlockedByPartitionFields() throws Exception {
    String wide = writeWider("wide.parquet");
    AddFiles partitionedByMissing =
        new AddFiles(
            catalogConfig,
            tableId.toString(),
            null,
            Arrays.asList("missing"),
            null,
            null,
            null,
            null,
            DRY_RUN);
    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(wide)).apply(partitionedByMissing);
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              assertFalse(report.getBoolean("allowed"));
              assertFalse(report.getBoolean("would_create_table"));
              assertThat(report.getString("reason"), containsString("would fail to create"));
              assertThat(report.getString("reason"), containsString("partition fields [missing]"));
              assertEquals(1, checkStateNotNull(report.getArray("config_problems")).size());
              return null;
            });
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(1, counted(result, DryRunReport.class, DryRunReport.CONFIG_PROBLEMS_COUNTER));
    assertFalse(catalog.tableExists(tableId));
  }

  /**
   * The dry run reuses the real fold on scratch transactions: a schema compatible with the table
   * but incompatible with another schema of the input is reported, as a real run would.
   */
  @Test
  public void testDryRunSurfacesCrossSchemaConflicts() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    Schema emailInt =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "email", Types.IntegerType.get()));
    String a = writeWider("email_string.parquet");
    Record intRecord = GenericRecord.create(emailInt);
    intRecord.setField("id", 1);
    intRecord.setField("name", "a");
    intRecord.setField("age", 1);
    intRecord.setField("email", 7);
    String b = writeWithSchema("email_int.parquet", emailInt, intRecord);

    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(a, b)).apply(addFiles(DRY_RUN));
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Collection<Row> schemas = schemas(report(rows));
              assertEquals(2, schemas.size());
              int allowed = 0;
              for (Row schema : schemas) {
                if (schema.getBoolean("allowed")) {
                  allowed++;
                }
              }
              assertEquals("one of the two schemas loses the fold", 1, allowed);
              return null;
            });
    pipeline.run().waitUntilFinish();
    assertNull(catalog.loadTable(tableId).schema().findField("email"));
  }

  /** With evolution on, the pre-pass is the only creator; registration never falls back to one. */
  @Test
  public void testMissingTableIsNotCreatedAtRegistrationWithEvolution() throws Exception {
    File avro = temp.newFile("data.avro");
    File garbage = temp.newFile("garbage.parquet");
    java.nio.file.Files.write(garbage.toPath(), "not parquet".getBytes(StandardCharsets.UTF_8));
    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(avro.getAbsolutePath(), garbage.getAbsolutePath()))
            .apply(addFiles(ADDITIONS));
    PAssert.that(output.get("snapshots")).empty();
    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              int count = 0;
              for (Row row : rows) {
                count++;
                assertThat(
                    row.getString("error"),
                    containsString(AddFiles.ConvertToDataFile.MISSING_TABLE_ERROR));
              }
              assertEquals(2, count);
              return null;
            });
    pipeline.run().waitUntilFinish();
    assertFalse(catalog.tableExists(tableId));
  }

  /** Nothing can create the table, so nothing is allowed, whatever ACCEPT would register. */
  @Test
  public void testDryRunAgainstMissingTableWithNoUsableSchema() throws Exception {
    File avro = temp.newFile("data.avro");
    SchemaEvolutionConfig config =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION))
            .setUnverifiableFileHandling(UnverifiableFileHandling.ACCEPT)
            .setDryRun(true)
            .build();
    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(avro.getAbsolutePath())).apply(addFiles(config));
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              assertFalse(report.getBoolean("allowed"));
              assertFalse(report.getBoolean("would_create_table"));
              assertFalse(report.getBoolean("unchecked_registered"));
              assertEquals(Long.valueOf(1), report.getInt64("files_unchecked"));
              assertThat(report.getString("reason"), containsString(DryRunReport.NO_TABLE_REASON));
              return null;
            });
    pipeline.run().waitUntilFinish();
    assertFalse(catalog.tableExists(tableId));
  }

  /** Files that contribute no schema are counted instead of vanishing. */
  @Test
  public void testDryRunReportsUnreadableAndNonParquetFiles() throws Exception {
    dryRunWithUnreadableAndAvro(UnverifiableFileHandling.REJECT, false);
  }

  @Test
  public void testDryRunReportsNonParquetFilesAsRegisteredWhenAccepted() throws Exception {
    dryRunWithUnreadableAndAvro(UnverifiableFileHandling.ACCEPT, true);
  }

  private void dryRunWithUnreadableAndAvro(
      UnverifiableFileHandling handling, boolean uncheckedRegistered) throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String good = writeOneRecord("good.parquet");
    File garbage = temp.newFile("garbage.parquet");
    java.nio.file.Files.write(garbage.toPath(), "not parquet".getBytes(StandardCharsets.UTF_8));
    File avro = temp.newFile("data.avro");

    SchemaEvolutionConfig config =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION))
            .setUnverifiableFileHandling(handling)
            .setDryRun(true)
            .build();
    PCollectionRowTuple output =
        pipeline
            .apply(
                "Create Input", Create.of(good, garbage.getAbsolutePath(), avro.getAbsolutePath()))
            .apply(addFiles(config));
    PAssert.that(output.get(AddFiles.DRY_RUN_TAG))
        .satisfies(
            rows -> {
              Row report = report(rows);
              assertTrue(report.getBoolean("allowed"));
              assertEquals(Long.valueOf(1), report.getInt64("files_allowed"));
              assertEquals(Long.valueOf(1), report.getInt64("files_unreadable"));
              assertEquals(Long.valueOf(1), report.getInt64("files_unchecked"));
              assertEquals(uncheckedRegistered, report.getBoolean("unchecked_registered"));
              return null;
            });
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(1, counted(result, DryRunReport.class, DryRunReport.FILES_ALLOWED_COUNTER));
    assertEquals(0, counted(result, DryRunReport.class, DryRunReport.FILES_INCOMPATIBLE_COUNTER));
    assertEquals(1, counted(result, DryRunReport.class, DryRunReport.FILES_UNREADABLE_COUNTER));
    assertEquals(1, counted(result, DryRunReport.class, DryRunReport.FILES_UNCHECKED_COUNTER));
  }

  @Test
  public void testEvolutionDisabledAddsNoPrePassTransforms() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeOneRecord("data.parquet");

    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(file)).apply(addFiles(null));
    PAssert.that(output.get("errors")).empty();
    assertEquals(0, countTransforms(pipeline, "ReadFooterSchema"));
    assertEquals(0, countTransforms(pipeline, "WaitForSchemaCommit"));

    pipeline.run().waitUntilFinish();

    assertEquals(1, Iterables.size(catalog.loadTable(tableId).newScan().planFiles()));
  }

  @Test
  public void testIncompatibleSchemaFailsBatchPipelineByDefault() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String good = writeOneRecord("good.parquet");
    String bad = writeConflicting("bad.parquet");

    pipeline
        .apply("Create Input", Create.of(good, bad))
        .apply(addFiles(SchemaEvolutionConfig.of(SchemaEvolutionOption.values())));

    Exception e = assertThrows(Exception.class, () -> pipeline.run().waitUntilFinish());

    assertThat(e.getMessage(), containsString("Incompatible schemas"));
    assertEquals(0, Iterables.size(catalog.loadTable(tableId).snapshots()));
  }

  @Test
  public void testIncompatibleSchemaRoutedToErrorsWhenConfigured() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String good = writeOneRecord("good.parquet");
    String bad = writeConflicting("bad.parquet");
    SchemaEvolutionConfig route =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setIncompatibleSchemaHandling(
                SchemaEvolutionConfig.IncompatibleSchemaHandling.ROUTE_TO_ERRORS)
            .build();

    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(good, bad)).apply(addFiles(route));
    PAssert.that(output.get("errors"))
        .satisfies(
            rows -> {
              Row row = Iterables.getOnlyElement(rows);
              assertEquals(bad, row.getString("file"));
              assertThat(row.getString("error"), containsString("does not cover the file"));
              return null;
            });

    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(tableId);
    assertEquals(1, Iterables.size(table.snapshots()));
    assertEquals(1, Iterables.size(table.newScan().planFiles()));
  }

  @Test
  public void testMissingTableIsCreatedFromTheFilesUnion() throws Exception {
    String narrow = writeOneRecord("narrow.parquet");
    String wide = writeWider("wide.parquet");

    PCollectionRowTuple output =
        pipeline.apply("Create Input", Create.of(narrow, wide)).apply(addFiles(ADDITIONS));
    PAssert.that(output.get("errors")).empty();

    pipeline.run().waitUntilFinish();

    assertEmailAddedAndFilesRegistered(2);
    assertTrue(
        "created columns are optional",
        catalog.loadTable(tableId).schema().findField("id").isOptional());
  }

  /** Streaming schema evolution comes in a follow-up: until then the front door rejects it. */
  @Test
  public void testUnboundedInputWithEvolutionIsRejected() {
    pipeline.enableAbandonedNodeEnforcement(false);
    PCollection<String> unbounded =
        pipeline.apply(TestStream.create(StringUtf8Coder.of()).advanceWatermarkToInfinity());
    AddFiles streaming =
        new AddFiles(
            catalogConfig,
            tableId.toString(),
            null,
            null,
            null,
            null,
            10,
            Duration.standardSeconds(5),
            ADDITIONS);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> unbounded.apply(streaming));

    assertThat(e.getMessage(), containsString("not yet supported for unbounded input"));
  }

  /**
   * Whatever windowing the caller applied upstream, the pre-pass rewindows into the global window:
   * one schema commit covers the whole input and the Wait.on gate holds every file behind it.
   */
  @Test
  public void testUpstreamWindowedBatchInputEvolvesAndRegisters() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String narrow = writeOneRecord("narrow.parquet");
    String wide = writeWider("wide.parquet");

    PCollectionRowTuple output =
        pipeline
            .apply(
                "Create Input",
                Create.timestamped(
                    TimestampedValue.of(narrow, new Instant(0)),
                    TimestampedValue.of(wide, new Instant(60_000))))
            .apply("UpstreamWindow", Window.into(FixedWindows.of(Duration.standardSeconds(30))))
            .apply(addFiles(ADDITIONS));
    PAssert.that(output.get("errors")).empty();

    pipeline.run().waitUntilFinish();

    assertEmailAddedAndFilesRegistered(2);
  }

  /**
   * The schema commit retries a CommitFailedException (another writer got in first). The committer
   * is serialized with the DoFn, so the retry shows in the table and in the time the backoff
   * reports to the runner as throttled.
   */
  @Test
  public void testTransientSchemaCommitFailureIsRetried() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String wide = writeWider("wide.parquet");
    AtomicInteger attempts = new AtomicInteger();
    CommitSchemaUnion.Committer failsOnce =
        txn -> {
          if (attempts.incrementAndGet() == 1) {
            throw new CommitFailedException("transient");
          }
          txn.commitTransaction();
        };

    PCollectionRowTuple output =
        pipeline
            .apply("Create Input", Create.of(wide))
            .apply(addFiles(ADDITIONS).withSchemaCommitter(failsOnce));
    PAssert.that(output.get("errors")).empty();

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    assertEmailAddedAndFilesRegistered(1);
    long throttledMillis = 0;
    for (MetricResult<Long> metric :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            org.apache.beam.sdk.metrics.Metrics.THROTTLE_TIME_NAMESPACE,
                            org.apache.beam.sdk.metrics.Metrics.THROTTLE_TIME_COUNTER_NAME))
                    .build())
            .getCounters()) {
      throttledMillis += metric.getAttempted();
    }
    assertTrue("one backoff wait was reported: " + throttledMillis, throttledMillis > 0);
  }

  // ---- ConvertToDataFile coverage check and pinned columns

  private static final SchemaEvolutionConfig ADDITIONS =
      SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION);

  private static final SchemaEvolutionConfig DRY_RUN =
      SchemaEvolutionConfig.builder()
          .setOptions(EnumSet.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION))
          .setDryRun(true)
          .build();

  private PCollectionTuple convert(SchemaEvolutionConfig config, String... files) {
    PCollectionTuple out =
        pipeline
            .apply("Create Input", Create.of(Arrays.asList(files)))
            .apply(
                ParDo.of(
                        new AddFiles.ConvertToDataFile(
                            catalogConfig, tableId.toString(), null, null, null, null, config))
                    .withOutputTags(
                        AddFiles.ConvertToDataFile.DATA_FILES,
                        TupleTagList.of(AddFiles.ConvertToDataFile.ERRORS)));
    out.get(AddFiles.ConvertToDataFile.ERRORS).setRowSchema(AddFiles.ERROR_SCHEMA);
    return out;
  }

  private String writeWithSchema(String name, Schema schema, Record... records) throws IOException {
    String file = root + name;
    DataWriter<Record> writer =
        Parquet.writeData(Files.localOutput(file))
            .schema(schema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return file;
  }

  private String writeOneRecord(String name) throws IOException {
    String file = root + name;
    DataWriter<Record> writer = createWriter(file);
    writer.write(record(1, "a", 1));
    writer.close();
    return file;
  }

  private static final Schema WIDER =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name", Types.StringType.get()),
          Types.NestedField.required(3, "age", Types.IntegerType.get()),
          Types.NestedField.optional(4, "email", Types.StringType.get()));

  private String writeWider(String name) throws IOException {
    Record record = GenericRecord.create(WIDER);
    record.setField("id", 1);
    record.setField("name", "a");
    record.setField("age", 1);
    record.setField("email", "e");
    return writeWithSchema(name, WIDER, record);
  }

  private void assertSingleError(PCollectionTuple out, String file, String contains) {
    PAssert.that(out.get(AddFiles.ConvertToDataFile.DATA_FILES)).empty();
    PAssert.that(out.get(AddFiles.ConvertToDataFile.ERRORS))
        .satisfies(
            rows -> {
              Row row = Iterables.getOnlyElement(rows);
              assertEquals(file, row.getString("file"));
              assertThat(row.getString("error"), containsString(contains));
              return null;
            });
  }

  private void assertRegisters(PCollectionTuple out, long files) {
    PAssert.that(out.get(AddFiles.ConvertToDataFile.ERRORS)).empty();
    PAssert.thatSingleton(out.get(AddFiles.ConvertToDataFile.DATA_FILES).apply(Count.globally()))
        .isEqualTo(files);
  }

  @Test
  public void testEmptySchemaTableWarnsAndStillRegisters() throws Exception {
    catalog.createTable(tableId, new Schema());
    String file = writeOneRecord("data.parquet");

    PCollectionTuple out = convert(SchemaEvolutionConfig.disabled(), file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
    logs.verifyWarn("has no columns");
  }

  /** Files without embedded field ids against a zero-column table still register. */
  @Test
  public void testEmptySchemaTableWithIdLessParquetStillRegisters() throws Exception {
    catalog.createTable(tableId, new Schema());
    File file = new File(temp.getRoot(), "idless.parquet");
    org.apache.avro.Schema avro =
        org.apache.avro.SchemaBuilder.record("r")
            .fields()
            .requiredInt("id")
            .optionalString("name")
            .name("address")
            .type()
            .record("address")
            .fields()
            .optionalString("city")
            .endRecord()
            .noDefault()
            .endRecord();
    try (org.apache.parquet.hadoop.ParquetWriter<Object> writer =
        org.apache.parquet.avro.AvroParquetWriter.builder(
                new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
            .withSchema(avro)
            .build()) {
      org.apache.avro.generic.GenericData.Record record =
          new org.apache.avro.generic.GenericData.Record(avro);
      record.put("id", 1);
      record.put("name", "a");
      org.apache.avro.generic.GenericData.Record address =
          new org.apache.avro.generic.GenericData.Record(avro.getField("address").schema());
      address.put("city", "c");
      record.put("address", address);
      writer.write(record);
    }

    PCollectionTuple out = convert(SchemaEvolutionConfig.disabled(), file.getAbsolutePath());

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCoveredFileRegistersWithEvolutionEnabled() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeOneRecord("data.parquet");

    PCollectionTuple out = convert(ADDITIONS, file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUncoveredFileRoutesToErrorsWhenEvolutionEnabled() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeWider("wider.parquet");

    PCollectionTuple out = convert(ADDITIONS, file);

    assertSingleError(out, file, "does not cover the file");
    PAssert.that(out.get(AddFiles.ConvertToDataFile.ERRORS))
        .satisfies(
            rows -> {
              assertThat(
                  Iterables.getOnlyElement(rows).getString("error"),
                  containsString("add optional email string"));
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExtraColumnsRegisterWhenEvolutionDisabled() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeWider("wider.parquet");

    PCollectionTuple out = convert(SchemaEvolutionConfig.disabled(), file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnreadableSchemaRoutesToErrorsWithConverterMessage() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    // a legacy unannotated repeated field: readable Parquet, rejected by Iceberg's converter
    org.apache.parquet.schema.MessageType legacy =
        org.apache.parquet.schema.Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .repeated(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
            .named("vals")
            .named("root");
    File legacyFile = new File(temp.getRoot(), "legacy.parquet");
    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer =
        org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(
                new org.apache.hadoop.fs.Path(legacyFile.getAbsolutePath()))
            .withType(legacy)
            .build()) {
      org.apache.parquet.example.data.Group group =
          new org.apache.parquet.example.data.simple.SimpleGroupFactory(legacy).newGroup();
      group.add("id", 1);
      group.add("vals", 2);
      writer.write(group);
    }
    String file = legacyFile.getAbsolutePath();

    PCollectionTuple out = convert(ADDITIONS, file);

    assertSingleError(out, file, AddFiles.ConvertToDataFile.UNREADABLE_SCHEMA_ERROR);
    PAssert.that(out.get(AddFiles.ConvertToDataFile.ERRORS))
        .satisfies(
            rows -> {
              assertThat(
                  Iterables.getOnlyElement(rows).getString("error"),
                  containsString("repetition REPEATED"));
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  // ---- pinned columns
  //
  // A pin is enforced in two layers. A pinned column the table holds as REQUIRED is protected by
  // the coverage check: a file that declares it optional with nulls, or lacks it, needs a
  // relaxation of a pinned column, which SchemaDelta refuses, so the file is routed there
  // (testPinnedRequiredColumnIsProtectedByCoverage). The per-file pin walk is reached only for
  // pinned columns the table holds as OPTIONAL: columns the pre-pass added (it never creates them
  // required) or pre-existing optional ones. The tests below therefore pin optional columns.

  private static SchemaEvolutionConfig pinned(String column) {
    return SchemaEvolutionConfig.builder()
        .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
        .setRequiredColumns(Collections.singleton(column))
        .build();
  }

  private static final Schema OPTIONAL_NAME =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.required(3, "age", Types.IntegerType.get()));

  private static final Schema WITHOUT_NAME =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(3, "age", Types.IntegerType.get()));

  private String writeCleanName(String name) throws IOException {
    return writeWithSchema(
        name,
        OPTIONAL_NAME,
        GenericRecord.create(OPTIONAL_NAME).copy("id", 1, "name", "a", "age", 1));
  }

  private String writeOneNullName(String name) throws IOException {
    return writeWithSchema(
        name,
        OPTIONAL_NAME,
        GenericRecord.create(OPTIONAL_NAME).copy("id", 1, "name", "a", "age", 1),
        GenericRecord.create(OPTIONAL_NAME).copy("id", 2, "age", 2));
  }

  private String writeWithoutName(String name) throws IOException {
    return writeWithSchema(
        name, WITHOUT_NAME, GenericRecord.create(WITHOUT_NAME).copy("id", 1, "age", 1));
  }

  @Test
  public void testPinnedRequiredColumnIsProtectedByCoverage() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String withNull = writeOneNullName("nulls.parquet");
    String absent = writeWithoutName("noname.parquet");

    PCollectionTuple out = convert(pinned("name"), withNull, absent);

    PAssert.that(out.get(AddFiles.ConvertToDataFile.DATA_FILES)).empty();
    PAssert.that(out.get(AddFiles.ConvertToDataFile.ERRORS))
        .satisfies(
            rows -> {
              assertEquals(2, Iterables.size(rows));
              for (Row row : rows) {
                assertThat(row.getString("error"), containsString("does not cover the file"));
                assertThat(row.getString("error"), containsString("pinned as required"));
              }
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnWithNullsRoutesToErrors() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeOneNullName("nulls.parquet");

    PCollectionTuple out = convert(pinned("name"), file);

    assertSingleError(out, file, "Pinned required column name has 1 null(s)");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnAbsentRoutesToErrors() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeWithoutName("noname.parquet");

    PCollectionTuple out = convert(pinned("name"), file);

    assertSingleError(out, file, "Pinned required column name is absent from the file");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnProvenNullFreeRegisters() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeCleanName("clean.parquet");

    PCollectionTuple out = convert(pinned("name"), file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnZeroRowFileRegisters() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    // Iceberg's writer creates no file for zero rows; parquet-avro does.
    File empty = new File(temp.getRoot(), "empty.parquet");
    org.apache.parquet.avro.AvroParquetWriter.builder(
            new org.apache.hadoop.fs.Path(empty.getAbsolutePath()))
        .withSchema(AVRO_OPTIONAL_NAME)
        .build()
        .close();
    String file = empty.getAbsolutePath();

    PCollectionTuple out = convert(pinned("name"), file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  /** Pin evidence comes from the footer: the table's metrics configuration cannot disable it. */
  @Test
  public void testPinnedColumnProvenNullFreeRegistersUnderMetricsModeNone() throws Exception {
    catalog.createTable(
        tableId,
        OPTIONAL_NAME,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("write.metadata.metrics.default", "none"));
    String file = writeCleanName("clean.parquet");

    PCollectionTuple out = convert(pinned("name"), file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnNullsDetectedUnderMetricsModeNone() throws Exception {
    catalog.createTable(
        tableId,
        OPTIONAL_NAME,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("write.metadata.metrics.default", "none"));
    String file = writeOneNullName("nulls.parquet");

    PCollectionTuple out = convert(pinned("name"), file);

    assertSingleError(out, file, "Pinned required column name has 1 null(s)");
    pipeline.run().waitUntilFinish();
  }

  /** With evolution on, a format the checks cannot read must not register unchecked. */
  @Test
  public void testNonParquetFileRoutesToErrorsWhenEvolutionEnabled() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeAvroFile("data.avro");

    PCollectionTuple out = convert(ADDITIONS, file);

    assertSingleError(out, file, AddFiles.ConvertToDataFile.UNCHECKED_FORMAT_ERROR + "AVRO");
    pipeline.run().waitUntilFinish();
  }

  // ---- UnverifiableFileHandling.ACCEPT

  private static SchemaEvolutionConfig accepting(SchemaEvolutionConfig base) {
    return SchemaEvolutionConfig.builder()
        .setOptions(base.getOptions())
        .setRequiredColumns(base.getRequiredColumns())
        .setUnverifiableFileHandling(UnverifiableFileHandling.ACCEPT)
        .build();
  }

  private static long counted(PipelineResult result, String counter) {
    return counted(result, AddFiles.class, counter);
  }

  private static long counted(PipelineResult result, Class<?> namespace, String counter) {
    long total = 0;
    for (MetricResult<Long> metric :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, counter))
                    .build())
            .getCounters()) {
      total += metric.getAttempted();
    }
    return total;
  }

  private String writeAvroFile(String name) throws IOException {
    File avroFile = new File(temp.getRoot(), name);
    org.apache.avro.Schema avro =
        org.apache.avro.SchemaBuilder.record("r").fields().requiredInt("id").endRecord();
    try (org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer =
        new org.apache.avro.file.DataFileWriter<>(
            new org.apache.avro.generic.GenericDatumWriter<>(avro))) {
      writer.create(avro, avroFile);
      org.apache.avro.generic.GenericData.Record avroRecord =
          new org.apache.avro.generic.GenericData.Record(avro);
      avroRecord.put("id", 1);
      writer.append(avroRecord);
    }
    return avroFile.getAbsolutePath();
  }

  private static final org.apache.avro.Schema AVRO_OPTIONAL_NAME =
      org.apache.avro.SchemaBuilder.record("r")
          .fields()
          .requiredInt("id")
          .optionalString("name")
          .requiredInt("age")
          .endRecord();

  /**
   * One OPTIONAL_NAME-shaped row written by parquet-avro with column statistics switched off, for
   * the named columns or for every column when none is named.
   */
  private String writeWithoutStatistics(String name, List<String> statlessColumns, boolean nullName)
      throws IOException {
    File file = new File(temp.getRoot(), name);
    org.apache.parquet.avro.AvroParquetWriter.Builder<Object> builder =
        org.apache.parquet.avro.AvroParquetWriter.builder(
                new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
            .withSchema(AVRO_OPTIONAL_NAME);
    if (statlessColumns.isEmpty()) {
      builder = builder.withStatisticsEnabled(false);
    }
    for (String column : statlessColumns) {
      builder = builder.withStatisticsEnabled(column, false);
    }
    try (org.apache.parquet.hadoop.ParquetWriter<Object> writer = builder.build()) {
      org.apache.avro.generic.GenericData.Record record =
          new org.apache.avro.generic.GenericData.Record(AVRO_OPTIONAL_NAME);
      record.put("id", 1);
      record.put("name", nullName ? null : "a");
      record.put("age", 1);
      writer.write(record);
    }
    return file.getAbsolutePath();
  }

  private static final org.apache.avro.Schema AVRO_REQUIRED_NAME =
      org.apache.avro.SchemaBuilder.record("r")
          .fields()
          .requiredInt("id")
          .requiredString("name")
          .requiredInt("age")
          .endRecord();

  /** Parquet cannot encode a null in a required column, so no statistics are needed to prove it. */
  @Test
  public void testPinnedColumnDeclaredRequiredRegistersWithoutStatistics() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    File file = new File(temp.getRoot(), "required_nostats.parquet");
    try (org.apache.parquet.hadoop.ParquetWriter<Object> writer =
        org.apache.parquet.avro.AvroParquetWriter.builder(
                new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
            .withSchema(AVRO_REQUIRED_NAME)
            .withStatisticsEnabled(false)
            .build()) {
      org.apache.avro.generic.GenericData.Record record =
          new org.apache.avro.generic.GenericData.Record(AVRO_REQUIRED_NAME);
      record.put("id", 1);
      record.put("name", "a");
      record.put("age", 1);
      writer.write(record);
    }

    PCollectionTuple out = convert(pinned("name"), file.getAbsolutePath());

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnWithoutStatisticsRoutesToErrors() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeWithoutStatistics("nostats.parquet", Collections.emptyList(), false);

    PCollectionTuple out = convert(pinned("name"), file);

    assertSingleError(
        out, file, "Pinned required column name has no null count statistics in the file");
    pipeline.run().waitUntilFinish();
  }

  /** The trusted file does hold a null the footer cannot report; ACCEPT registers it anyway. */
  @Test
  public void testPinnedColumnWithoutStatisticsRegistersWhenAccepted() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeWithoutStatistics("nostats.parquet", Collections.emptyList(), true);

    PCollectionTuple out = convert(accepting(pinned("name")), file);

    assertRegisters(out, 1);
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(1, counted(result, AddFiles.UNPROVEN_PINS_COUNTER));
    assertEquals(0, counted(result, AddFiles.UNCHECKED_FORMAT_COUNTER));
    logs.verifyWarn("no null count statistics for pinned column(s) [name]");
  }

  /**
   * ACCEPT trusts only what the footer cannot say; a pin the footer does count is still enforced.
   */
  @Test
  public void testPinWithNullsStillCaughtWhenAnotherPinIsUnproven() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeWithoutStatistics("mixed.parquet", Collections.singletonList("age"), true);
    SchemaEvolutionConfig config =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.allOf(SchemaEvolutionOption.class))
            .setRequiredColumns(new LinkedHashSet<>(Arrays.asList("age", "name")))
            .setUnverifiableFileHandling(UnverifiableFileHandling.ACCEPT)
            .build();

    PCollectionTuple out = convert(config, file);

    assertSingleError(out, file, "Pinned required column name has 1 null(s)");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedColumnAbsentStillCaughtWhenAccepted() throws Exception {
    catalog.createTable(tableId, OPTIONAL_NAME);
    String file = writeWithoutName("noname.parquet");

    PCollectionTuple out = convert(accepting(pinned("name")), file);

    assertSingleError(out, file, "Pinned required column name is absent from the file");
    pipeline.run().waitUntilFinish();
  }

  private static final Schema WITH_ITEMS =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(
              2,
              "items",
              Types.ListType.ofOptional(
                  3,
                  Types.StructType.of(
                      Types.NestedField.optional(4, "sku", Types.StringType.get())))));

  private String writeWithItems(String name) throws IOException {
    Types.StructType item =
        WITH_ITEMS.findField("items").type().asListType().elementType().asStructType();
    Record row = GenericRecord.create(WITH_ITEMS);
    row.setField("id", 1);
    row.setField("items", Collections.singletonList(GenericRecord.create(item).copy("sku", "x")));
    return writeWithSchema(name, WITH_ITEMS, row);
  }

  /**
   * The footer's chunk paths under a list are never mapped onto the pin, so it cannot be proven.
   */
  @Test
  public void testPinUnderListCannotBeProvenAndIsRejected() throws Exception {
    catalog.createTable(tableId, WITH_ITEMS);
    String file = writeWithItems("items.parquet");

    PCollectionTuple out = convert(pinned("items.element.sku"), file);

    assertSingleError(
        out,
        file,
        "Pinned required column items.element.sku has no null count statistics in the file");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinUnderListRegistersWhenAccepted() throws Exception {
    catalog.createTable(tableId, WITH_ITEMS);
    String file = writeWithItems("items.parquet");

    PCollectionTuple out = convert(accepting(pinned("items.element.sku")), file);

    assertRegisters(out, 1);
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(1, counted(result, AddFiles.UNPROVEN_PINS_COUNTER));
  }

  private static final Schema WITH_ADDRESS =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(
              2,
              "address",
              Types.StructType.of(
                  Types.NestedField.optional(3, "city", Types.StringType.get()),
                  Types.NestedField.optional(4, "zip", Types.IntegerType.get()))));

  private String writeWithAddress(String name, @Nullable String city, boolean nullAddress)
      throws IOException {
    Types.StructType addressType = WITH_ADDRESS.findField("address").type().asStructType();
    Record row = GenericRecord.create(WITH_ADDRESS);
    row.setField("id", 1);
    if (!nullAddress) {
      row.setField("address", GenericRecord.create(addressType).copy("city", city, "zip", 1));
    }
    return writeWithSchema(name, WITH_ADDRESS, row);
  }

  /** A struct pin is proven by any null-free leaf beneath it, exactly as tighten proves it. */
  @Test
  public void testPinnedStructProvenByOneLeafRegisters() throws Exception {
    catalog.createTable(tableId, WITH_ADDRESS);
    String file = writeWithAddress("address.parquet", null, false);

    PCollectionTuple out = convert(pinned("address"), file);

    assertRegisters(out, 1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPinnedStructNullInARowRoutesToErrors() throws Exception {
    catalog.createTable(tableId, WITH_ADDRESS);
    String file = writeWithAddress("noaddress.parquet", "c", true);

    PCollectionTuple out = convert(pinned("address"), file);

    assertSingleError(
        out, file, "Pinned required column address has no null count statistics in the file");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testNonParquetFileRegistersUncheckedWhenAccepted() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeAvroFile("data.avro");

    PCollectionTuple out = convert(accepting(ADDITIONS), file);

    assertRegisters(out, 1);
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(1, counted(result, AddFiles.UNCHECKED_FORMAT_COUNTER));
    assertEquals(0, counted(result, AddFiles.UNPROVEN_PINS_COUNTER));
    logs.verifyWarn("unchecked (UnverifiableFileHandling.ACCEPT)");
  }

  @Test
  public void testNonParquetFileRegistersWhenEvolutionDisabled() throws Exception {
    catalog.createTable(tableId, icebergSchema);
    String file = writeAvroFile("data.avro");

    PCollectionTuple out = convert(SchemaEvolutionConfig.disabled(), file);

    assertRegisters(out, 1);
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    assertEquals(0, counted(result, AddFiles.UNCHECKED_FORMAT_COUNTER));
  }
}
