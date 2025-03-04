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

import static org.apache.beam.sdk.io.iceberg.ReadUtils.OPERATION;
import static org.apache.beam.sdk.io.iceberg.ReadUtils.RECORD;
import static org.apache.beam.sdk.io.iceberg.TestFixtures.createRecord;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTimestamp;
import static org.apache.iceberg.util.DateTimeUtil.microsToMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class IcebergIOReadTest {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergIOReadTest.class);

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestPipeline testPipeline = TestPipeline.create();
  @Rule public TestName testName = new TestName();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  // TODO(#34168, ahmedabu98): Update tests when we close feature gaps between regular and cdc
  // sources
  @Parameter(0)
  public boolean useIncrementalScan;

  static class PrintRow extends PTransform<PCollection<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      Schema inputSchema = input.getSchema();

      return input
          .apply(
              ParDo.of(
                  new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(@Element Row row, OutputReceiver<Row> output) {
                      LOG.info("Got row {}", row);
                      output.output(row);
                    }
                  }))
          .setRowSchema(inputSchema);
    }
  }

  @Test
  public void testFailWhenBothStartingSnapshotAndTimestampAreSet() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig())
            .from(tableId)
            .withCdc()
            .fromSnapshot(123L)
            .fromTimestamp(123L);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: only one of 'from_timestamp' or 'from_snapshot' can be set");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWhenBothEndingSnapshotAndTimestampAreSet() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig())
            .withCdc()
            .from(tableId)
            .toSnapshot(123L)
            .toTimestamp(123L);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: only one of 'to_timestamp' or 'to_snapshot' can be set");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWhenStartingPointAndStartingStrategyAreSet() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig())
            .withCdc()
            .from(tableId)
            .fromSnapshot(123L)
            .withStartingStrategy(StartingStrategy.EARLIEST);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: 'from_timestamp' and 'from_snapshot' are not allowed when 'starting_strategy' is set");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWhenPollIntervalIsSetOnBatchRead() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig())
            .withCdc()
            .from(tableId)
            .withPollInterval(Duration.standardSeconds(5));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: 'poll_interval_seconds' can only be set when streaming is true");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWhenWatermarkColumnDoesNotExist() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig()).withCdc().from(tableId).withWatermarkColumn("unknown");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: the specified 'watermark_column' field does not exist: 'unknown'");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWithInvalidWatermarkColumnType() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig()).withCdc().from(tableId).withWatermarkColumn("data");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: invalid 'watermark_column' type: string. "
            + "Valid types are [long, timestamp, timestamptz]");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWhenWatermarkColumnMissingMetrics() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    table.updateProperties().set("write.metadata.metrics.default", "none").commit();
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig()).withCdc().from(tableId).withWatermarkColumn("id");

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Invalid source configuration: source table");
    thrown.expectMessage(
        "not configured to capture lower bound metrics for the specified watermark column 'id'.");
    thrown.expectMessage("found 'none'");

    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testFailWhenWatermarkTimeUnitUsedWithoutSpecifyingColumn() {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergIO.ReadRows read =
        IcebergIO.readRows(catalogConfig()).withCdc().from(tableId).withWatermarkTimeUnit("hours");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid source configuration: cannot set 'watermark_time_unit' without "
            + "also setting a 'watermark_column' of Long type");
    read.expand(PBegin.in(testPipeline));
  }

  @Test
  public void testSimpleScan() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    List<List<Record>> expectedRecords = warehouse.commitData(simpleTable);

    IcebergIO.ReadRows read = IcebergIO.readRows(catalogConfig()).from(tableId);

    if (useIncrementalScan) {
      read = read.withCdc().toSnapshot(simpleTable.currentSnapshot().snapshotId());
    }
    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    PCollection<Row> output = testPipeline.apply(read).apply(new PrintRow());

    if (useIncrementalScan) {
      output = output.apply(ReadUtils.extractRecords());
    }

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testIdentityColumnScan() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    String identityColumnName = "identity";
    String identityColumnValue = "some-value";
    simpleTable.updateSchema().addColumn(identityColumnName, Types.StringType.get()).commit();
    simpleTable.updateSpec().addField(identityColumnName).commit();

    PartitionSpec spec = simpleTable.spec();
    PartitionKey partitionKey = new PartitionKey(simpleTable.spec(), simpleTable.schema());
    partitionKey.set(0, identityColumnValue);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet",
                TestFixtures.SCHEMA,
                spec,
                partitionKey,
                TestFixtures.FILE1SNAPSHOT1))
        .commit();

    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema());
    final List<Row> expectedRows =
        Stream.of(TestFixtures.FILE1SNAPSHOT1_DATA)
            .flatMap(List::stream)
            .map(
                d ->
                    ImmutableMap.<String, Object>builder()
                        .putAll(d)
                        .put(identityColumnName, identityColumnValue)
                        .build())
            .map(r -> createRecord(simpleTable.schema(), r))
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    IcebergIO.ReadRows read = IcebergIO.readRows(catalogConfig()).from(tableId);
    if (useIncrementalScan) {
      read = read.withCdc().toSnapshot(simpleTable.currentSnapshot().snapshotId());
    }
    PCollection<Row> output = testPipeline.apply(read).apply(new PrintRow());

    if (useIncrementalScan) {
      output = output.apply(ReadUtils.extractRecords());
    }

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testNameMappingScan() throws Exception {
    org.apache.avro.Schema metadataSchema =
        org.apache.avro.Schema.createRecord(
            "metadata",
            null,
            null,
            false,
            ImmutableList.of(
                new org.apache.avro.Schema.Field(
                    "source",
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                    null,
                    null)));

    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord(
            "test",
            null,
            null,
            false,
            ImmutableList.of(
                new org.apache.avro.Schema.Field(
                    "data",
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                    null,
                    null),
                new org.apache.avro.Schema.Field(
                    "id",
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
                    null,
                    null),
                new org.apache.avro.Schema.Field("metadata", metadataSchema, null, null)));

    List<Map<String, Object>> recordData =
        ImmutableList.<Map<String, Object>>builder()
            .add(
                ImmutableMap.of(
                    "id",
                    0L,
                    "data",
                    "clarification",
                    "metadata",
                    ImmutableMap.of("source", "systemA")))
            .add(
                ImmutableMap.of(
                    "id", 1L, "data", "risky", "metadata", ImmutableMap.of("source", "systemB")))
            .add(
                ImmutableMap.of(
                    "id", 2L, "data", "falafel", "metadata", ImmutableMap.of("source", "systemC")))
            .build();

    List<GenericRecord> avroRecords =
        recordData.stream()
            .map(data -> avroGenericRecord(avroSchema, data))
            .collect(Collectors.toList());

    Configuration hadoopConf = new Configuration();
    String path = createParquetFile(avroSchema, avroRecords);
    HadoopInputFile inputFile = HadoopInputFile.fromLocation(path, hadoopConf);

    NameMapping defaultMapping =
        NameMapping.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(3, "metadata", MappedFields.of(MappedField.of(4, "source"))));
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.<String, String>builder()
            .put(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(defaultMapping))
            .build();

    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable =
        warehouse
            .buildTable(tableId, TestFixtures.NESTED_SCHEMA)
            .withProperties(tableProperties)
            .withPartitionSpec(PartitionSpec.unpartitioned())
            .create();

    MetricsConfig metricsConfig = MetricsConfig.forTable(simpleTable);
    Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFormat(FileFormat.PARQUET)
            .withInputFile(inputFile)
            .withMetrics(metrics)
            .build();

    final Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.NESTED_SCHEMA);

    simpleTable.newFastAppend().appendFile(dataFile).commit();

    IcebergIO.ReadRows read = IcebergIO.readRows(catalogConfig()).from(tableId);
    if (useIncrementalScan) {
      read = read.withCdc().toSnapshot(simpleTable.currentSnapshot().snapshotId());
    }
    PCollection<Row> output = testPipeline.apply(read).apply(new PrintRow());

    final Row[] expectedRows =
        recordData.stream()
            .map(data -> icebergGenericRecord(TestFixtures.NESTED_SCHEMA.asStruct(), data))
            .map(record -> IcebergUtils.icebergRecordToBeamRow(beamSchema, record))
            .toArray(Row[]::new);

    if (useIncrementalScan) {
      output = output.apply(ReadUtils.extractRecords());
    }
    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testBatchDefaultsToEarliestStartingStrategy() throws IOException {
    runWithStartingStrategy(null, false);
  }

  @Test
  public void testStreamingDefaultsToLatestStartingStrategy() throws IOException {
    runWithStartingStrategy(null, true);
  }

  @Test
  public void testUseLatestStartingStrategyWithBatch() throws IOException {
    runWithStartingStrategy(StartingStrategy.LATEST, false);
  }

  @Test
  public void testUseEarliestStartingStrategyWithStreaming() throws IOException {
    runWithStartingStrategy(StartingStrategy.EARLIEST, true);
  }

  @Test
  public void testStreamingReadBetweenSnapshots() throws IOException {
    runReadWithBoundary(false, true);
  }

  @Test
  public void testBatchReadBetweenSnapshots() throws IOException {
    runReadWithBoundary(false, false);
  }

  @Test
  public void testStreamingReadBetweenTimestamps() throws IOException {
    runReadWithBoundary(false, true);
  }

  @Test
  public void testBatchReadBetweenTimestamps() throws IOException {
    runReadWithBoundary(false, false);
  }

  @Test
  public void testWatermarkColumnLongType() throws IOException {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    // configure the table to capture full metrics
    simpleTable
        .updateProperties()
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "full")
        .commit();

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s4.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT4))
        .commit();
    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file2s4.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT4))
        .appendFile(
            warehouse.writeRecords(
                "file3s4.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT4))
        .commit();

    IcebergIO.ReadRows readRows =
        IcebergIO.readRows(catalogConfig())
            .from(tableId)
            .withCdc()
            .streaming(true)
            .withStartingStrategy(StartingStrategy.EARLIEST)
            .toSnapshot(simpleTable.currentSnapshot().snapshotId())
            .withWatermarkColumn("id")
            .withWatermarkTimeUnit("days");
    PCollection<Row> output = testPipeline.apply(readRows);
    output.apply(
        ParDo.of(
            new CheckWatermarks(
                lowestMillisOf(TestFixtures.FILE1SNAPSHOT4_DATA),
                lowestMillisOf(TestFixtures.FILE2SNAPSHOT4_DATA),
                lowestMillisOf(TestFixtures.FILE3SNAPSHOT4_DATA),
                row -> TimeUnit.DAYS.toMillis(checkStateNotNull(row.getInt64("id"))))));
    testPipeline.run().waitUntilFinish();
  }

  private static long lowestMillisOf(List<Map<String, Object>> data) {
    long lowestId = data.stream().mapToLong(m -> (long) m.get("id")).min().getAsLong();
    return TimeUnit.DAYS.toMillis(lowestId);
  }

  @Test
  public void testWatermarkColumnTimestampType() throws IOException {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Schema schema =
        Schema.builder()
            .addStringField("str")
            .addLogicalTypeField("datetime", SqlTypes.DATETIME)
            .build();
    org.apache.iceberg.Schema iceSchema = IcebergUtils.beamSchemaToIcebergSchema(schema);
    Table simpleTable = warehouse.createTable(tableId, iceSchema);

    LocalDateTime file1Watermark = LocalDateTime.parse("2025-01-01T12:00:00");
    List<Map<String, Object>> data1 =
        ImmutableList.of(
            ImmutableMap.of("str", "a", "datetime", file1Watermark),
            ImmutableMap.of("str", "b", "datetime", file1Watermark.plusMinutes(10)),
            ImmutableMap.of("str", "c", "datetime", file1Watermark.plusMinutes(30)));

    LocalDateTime file2Watermark = LocalDateTime.parse("2025-02-01T12:00:00");
    List<Map<String, Object>> data2 =
        ImmutableList.of(
            ImmutableMap.of("str", "d", "datetime", file2Watermark),
            ImmutableMap.of("str", "e", "datetime", file2Watermark.plusMinutes(10)),
            ImmutableMap.of("str", "f", "datetime", file2Watermark.plusMinutes(30)));

    LocalDateTime file3Watermark = LocalDateTime.parse("2025-03-01T12:00:00");
    List<Map<String, Object>> data3 =
        ImmutableList.of(
            ImmutableMap.of("str", "g", "datetime", file3Watermark),
            ImmutableMap.of("str", "h", "datetime", file3Watermark.plusMinutes(10)),
            ImmutableMap.of("str", "i", "datetime", file3Watermark.plusMinutes(30)));

    // configure the table to capture full metrics
    simpleTable
        .updateProperties()
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "datetime", "full")
        .commit();

    simpleTable
        .newFastAppend()
        .appendFile(warehouse.writeData("file1.parquet", iceSchema, data1))
        .commit();
    simpleTable
        .newFastAppend()
        .appendFile(warehouse.writeData("file2.parquet", iceSchema, data2))
        .appendFile(warehouse.writeData("file3.parquet", iceSchema, data3))
        .commit();

    IcebergIO.ReadRows readRows =
        IcebergIO.readRows(catalogConfig())
            .from(tableId)
            .withCdc()
            .streaming(true)
            .withStartingStrategy(StartingStrategy.EARLIEST)
            .toSnapshot(simpleTable.currentSnapshot().snapshotId())
            .withWatermarkColumn("datetime");
    PCollection<Row> output = testPipeline.apply(readRows);
    output.apply(
        ParDo.of(
            new CheckWatermarks(
                microsToMillis(microsFromTimestamp(file1Watermark)),
                microsToMillis(microsFromTimestamp(file2Watermark)),
                microsToMillis(microsFromTimestamp(file3Watermark)),
                row -> {
                  LocalDateTime dt =
                      checkStateNotNull(row.getLogicalTypeValue("datetime", LocalDateTime.class));
                  return microsToMillis(microsFromTimestamp(dt));
                })));
    testPipeline.run().waitUntilFinish();
  }

  static class CheckWatermarks extends DoFn<Row, Void> {
    long file1Watermark;
    long file2Watermark;
    long file3Watermark;
    SerializableFunction<Row, Long> getMillisFn;

    CheckWatermarks(
        long file1Watermark,
        long file2Watermark,
        long file3Watermark,
        SerializableFunction<Row, Long> getMillisFn) {
      this.file1Watermark = file1Watermark;
      this.file2Watermark = file2Watermark;
      this.file3Watermark = file3Watermark;
      this.getMillisFn = getMillisFn;
    }

    @ProcessElement
    public void process(@Element Row row, @Timestamp Instant timestamp) {
      Row record = checkStateNotNull(row.getRow(RECORD));
      long expectedMillis = getMillisFn.apply(record);
      long actualMillis = timestamp.getMillis();

      if (expectedMillis >= file3Watermark) {
        assertEquals(file3Watermark, actualMillis);
      } else if (expectedMillis >= file2Watermark) {
        assertEquals(file2Watermark, actualMillis);
      } else {
        assertEquals(file1Watermark, actualMillis);
      }
    }
  }

  public void runWithStartingStrategy(@Nullable StartingStrategy strategy, boolean streaming)
      throws IOException {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    List<List<Record>> expectedRecords = warehouse.commitData(simpleTable);
    if ((strategy == StartingStrategy.LATEST) || (streaming && strategy == null)) {
      expectedRecords = expectedRecords.subList(9, 12);
    }
    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    IcebergIO.ReadRows readRows =
        IcebergIO.readRows(catalogConfig())
            .from(tableId)
            .withCdc()
            .streaming(streaming)
            .toSnapshot(simpleTable.currentSnapshot().snapshotId());
    if (strategy != null) {
      readRows = readRows.withStartingStrategy(strategy);
    }

    PCollection<Row> output = testPipeline.apply(readRows);
    PAssert.that(output)
        .satisfies(
            rows -> {
              for (Row row : rows) {
                assertEquals(DataOperations.APPEND, checkStateNotNull(row.getString(OPERATION)));
              }
              return null;
            });
    PCollection<Row> rows = output.apply(ReadUtils.extractRecords());
    PCollection.IsBounded expectedBoundedness =
        streaming ? PCollection.IsBounded.UNBOUNDED : PCollection.IsBounded.BOUNDED;
    assertEquals(expectedBoundedness, rows.isBounded());

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    testPipeline.run().waitUntilFinish();
  }

  public void runReadWithBoundary(boolean useSnapshotBoundary, boolean streaming)
      throws IOException {
    assumeTrue(useIncrementalScan);
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    // only read data committed in the second and third snapshots
    List<List<Record>> expectedRecords = warehouse.commitData(simpleTable).subList(3, 9);

    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    List<Snapshot> snapshots = Lists.newArrayList(simpleTable.snapshots());
    Snapshot secondSnapshot = snapshots.get(1);
    Snapshot thirdSnapshot = snapshots.get(2);

    IcebergIO.ReadRows readRows =
        IcebergIO.readRows(catalogConfig()).withCdc().from(tableId).streaming(streaming);

    if (useSnapshotBoundary) {
      readRows =
          readRows.fromSnapshot(secondSnapshot.snapshotId()).toSnapshot(thirdSnapshot.snapshotId());
    } else { // use timestamp boundary
      readRows =
          readRows
              .fromTimestamp(secondSnapshot.timestampMillis() - 1)
              .toTimestamp(thirdSnapshot.timestampMillis() + 1);
    }

    PCollection<Row> output = testPipeline.apply(readRows).apply(new PrintRow());
    PAssert.that(output)
        .satisfies(
            rows -> {
              for (Row row : rows) {
                assertEquals(DataOperations.APPEND, checkStateNotNull(row.getString(OPERATION)));
              }
              return null;
            });
    PCollection<Row> rows = output.apply(ReadUtils.extractRecords());
    PCollection.IsBounded expectedBoundedness =
        streaming ? PCollection.IsBounded.UNBOUNDED : PCollection.IsBounded.BOUNDED;
    assertEquals(expectedBoundedness, rows.isBounded());

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    testPipeline.run();
  }

  @SuppressWarnings("unchecked")
  public static GenericRecord avroGenericRecord(
      org.apache.avro.Schema schema, Map<String, Object> values) {
    GenericRecord record = new GenericData.Record(schema);
    for (org.apache.avro.Schema.Field field : schema.getFields()) {
      Object rawValue = values.get(field.name());
      Object avroValue =
          rawValue instanceof Map
              ? avroGenericRecord(field.schema(), (Map<String, Object>) rawValue)
              : rawValue;
      record.put(field.name(), avroValue);
    }
    return record;
  }

  @SuppressWarnings("unchecked")
  public static Record icebergGenericRecord(Types.StructType type, Map<String, Object> values) {
    org.apache.iceberg.data.GenericRecord record =
        org.apache.iceberg.data.GenericRecord.create(type);
    for (Types.NestedField field : type.fields()) {
      Object rawValue = values.get(field.name());
      Object value =
          rawValue instanceof Map
              ? icebergGenericRecord(field.type().asStructType(), (Map<String, Object>) rawValue)
              : rawValue;
      record.setField(field.name(), value);
    }
    return record;
  }

  public static String createParquetFile(org.apache.avro.Schema schema, List<GenericRecord> records)
      throws IOException {

    File tempFile = createTempFile();
    Path file = new Path(tempFile.getPath());

    AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(file);
    ParquetWriter<GenericRecord> parquetWriter = builder.withSchema(schema).build();
    for (GenericRecord record : records) {
      parquetWriter.write(record);
    }
    parquetWriter.close();

    return tempFile.getPath();
  }

  private static File createTempFile() throws IOException {
    File tempFile = File.createTempFile(ScanSourceTest.class.getSimpleName(), ".tmp");
    tempFile.deleteOnExit();
    boolean unused = tempFile.delete();
    return tempFile;
  }

  private IcebergCatalogConfig catalogConfig() {
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    return IcebergCatalogConfig.builder()
        .setCatalogName("name")
        .setCatalogProperties(catalogProps)
        .build();
  }
}
