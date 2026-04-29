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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
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
  private final org.apache.iceberg.Schema icebergSchema =
      new org.apache.iceberg.Schema(
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

      Metrics metrics =
          AddFiles.getFileMetrics(
              file, FileFormat.PARQUET, metricsConfig, MappingUtil.create(icebergSchema));
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

      String partitionPath = getPartitionFromMetrics(metrics, file, table);
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

      Metrics metrics =
          AddFiles.getFileMetrics(
              file, FileFormat.PARQUET, metricsConfig, MappingUtil.create(icebergSchema));
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
          () -> getPartitionFromMetrics(metrics, file, table));
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
}
