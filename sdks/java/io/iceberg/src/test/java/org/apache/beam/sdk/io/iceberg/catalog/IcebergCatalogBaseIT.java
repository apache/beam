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
package org.apache.beam.sdk.io.iceberg.catalog;

import static org.apache.beam.sdk.managed.Managed.ICEBERG;
import static org.apache.beam.sdk.managed.Managed.ICEBERG_CDC;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.PCollection.IsBounded.BOUNDED;
import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link Managed} {@link org.apache.beam.sdk.io.iceberg.IcebergIO} read and write
 * tests.
 *
 * <p>To test a new catalog, create a subclass of this test class and implement the following two
 * methods:
 *
 * <ul>
 *   <li>{@link #createCatalog()}
 *   <li>{@link #managedIcebergConfig(String)}
 * </ul>
 *
 * <p>If the catalog needs further logic to set up and tear down, you can override and implement
 * these methods:
 *
 * <ul>
 *   <li>{@link #catalogSetup()}
 *   <li>{@link #catalogCleanup(List)}
 * </ul>
 *
 * <p>1,000 records are used for each test by default. You can change this by overriding {@link
 * #numRecords()}.
 */
public abstract class IcebergCatalogBaseIT implements Serializable {
  private static final long SETUP_TEARDOWN_SLEEP_MS = 5000;
  private static final long AFTER_UPDATE_SLEEP_MS = 2000;

  public abstract Catalog createCatalog();

  public abstract Map<String, Object> managedIcebergConfig(String tableId);

  public abstract String type();

  public void catalogSetup() {
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(namespace()));
  }

  public void catalogCleanup(List<Namespace> namespaces) throws IOException {
    for (Namespace namespace : namespaces) {
      for (TableIdentifier identifier : catalog.listTables(namespace)) {
        catalog.dropTable(identifier);
      }
      if (catalog instanceof SupportsNamespaces) {
        ((SupportsNamespaces) catalog).dropNamespace(namespace);
      }
    }
  }

  public Integer numRecords() {
    return OPTIONS.getRunner().equals(DirectRunner.class) ? 10 : 1000;
  }

  public String namespace() {
    return catalogName + "_" + testName.getMethodName();
  }

  public String tableId() {
    return namespace() + ".test_table";
  }

  public static String warehouse(Class<? extends IcebergCatalogBaseIT> testClass) {
    return String.format(
        "%s/%s/%s",
        TestPipeline.testingPipelineOptions().getTempLocation(), testClass.getSimpleName(), RANDOM);
  }

  public String catalogName = type() + "_test_catalog_" + System.currentTimeMillis();

  @Before
  public void setUp() throws Exception {
    OPTIONS.as(DirectOptions.class).setTargetParallelism(1);
    warehouse = warehouse(getClass());
    namespacesToCleanup.add(namespace());
    catalog = createCatalog();
    catalogSetup();
    Thread.sleep(SETUP_TEARDOWN_SLEEP_MS);
  }

  @After
  public void cleanUp() throws Exception {
    catalogCleanup(namespacesToCleanup.stream().map(Namespace::of).collect(Collectors.toList()));
    LOG.info("Successfully cleaned up namespaces: {}", namespacesToCleanup);
    Thread.sleep(SETUP_TEARDOWN_SLEEP_MS);

    try {
      GcsUtil gcsUtil = OPTIONS.as(GcsOptions.class).getGcsUtil();
      GcsPath path = GcsPath.fromUri(warehouse);

      @Nullable
      List<StorageObject> objects =
          gcsUtil
              .listObjects(
                  path.getBucket(),
                  getClass().getSimpleName() + "/" + path.getFileName().toString(),
                  null)
              .getItems();

      // sometimes a catalog's cleanup will take care of all the files.
      // If any files are left though, manually delete them with GCS utils
      if (objects != null) {
        List<String> filesToDelete =
            objects.stream()
                .map(obj -> "gs://" + path.getBucket() + "/" + obj.getName())
                .collect(Collectors.toList());
        gcsUtil.remove(filesToDelete);
      }
      Thread.sleep(SETUP_TEARDOWN_SLEEP_MS);
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS files.", e);
    }
  }

  protected static String warehouse;
  protected List<String> namespacesToCleanup = new ArrayList<>();
  public Catalog catalog;
  protected static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);
  private static final String RANDOM = UUID.randomUUID().toString();
  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TestName testName = new TestName();

  @Rule
  public transient Timeout globalTimeout =
      Timeout.seconds(OPTIONS.getRunner().equals(DirectRunner.class) ? 300 : 20 * 60);

  private static final int NUM_SHARDS = OPTIONS.getRunner().equals(DirectRunner.class) ? 1 : 10;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogBaseIT.class);
  private static final Schema DOUBLY_NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("doubly_nested_str")
          .addInt64Field("doubly_nested_float")
          .build();

  private static final Schema NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("nested_str")
          .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
          .addInt32Field("nested_int")
          .addFloatField("nested_float")
          .build();
  protected static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addStringField("char")
          .addInt64Field("modulo_5")
          .addBooleanField("bool_field")
          .addInt32Field("int_field")
          .addRowField("row", NESTED_ROW_SCHEMA)
          .addArrayField("arr_long", Schema.FieldType.INT64)
          .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
          .addNullableInt64Field("nullable_long")
          .addDateTimeField("datetime_tz")
          .addLogicalTypeField("datetime", SqlTypes.DATETIME)
          .addLogicalTypeField("date", SqlTypes.DATE)
          .addLogicalTypeField("time", SqlTypes.TIME)
          .build();

  private static final SimpleFunction<Long, Row> ROW_FUNC =
      new SimpleFunction<Long, Row>() {
        final long offset2025Millis =
            LocalDate.parse("2025-01-01").toDateTime(LocalTime.MIDNIGHT).getMillis();

        @Override
        public Row apply(Long num) {
          String strNum = Long.toString(num);
          Row nestedRow =
              Row.withSchema(NESTED_ROW_SCHEMA)
                  .addValue("nested_str_value_" + strNum)
                  .addValue(
                      Row.withSchema(DOUBLY_NESTED_ROW_SCHEMA)
                          .addValue("doubly_nested_str_value_" + strNum)
                          .addValue(num)
                          .build())
                  .addValue(Integer.valueOf(strNum))
                  .addValue(Float.valueOf(strNum + "." + strNum))
                  .build();

          long timestampMillis = offset2025Millis + TimeUnit.HOURS.toMillis(num);
          return Row.withSchema(BEAM_SCHEMA)
              .addValue("value_" + strNum)
              .addValue(String.valueOf((char) (97 + num % 5)))
              .addValue(num % 5)
              .addValue(num % 2 == 0)
              .addValue(Integer.valueOf(strNum))
              .addValue(nestedRow)
              .addValue(LongStream.range(0, num % 10).boxed().collect(Collectors.toList()))
              .addValue(num % 2 == 0 ? null : nestedRow)
              .addValue(num)
              .addValue(new DateTime(timestampMillis).withZone(DateTimeZone.forOffsetHours(4)))
              .addValue(DateTimeUtil.timestampFromMicros(timestampMillis * 1000))
              .addValue(DateTimeUtil.dateFromDays(Integer.parseInt(strNum)))
              .addValue(DateTimeUtil.timeFromMicros(num))
              .build();
        }
      };

  protected static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA);
  protected static final SimpleFunction<Row, Record> RECORD_FUNC =
      new SimpleFunction<Row, Record>() {
        @Override
        public Record apply(Row input) {
          return IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, input);
        }
      };
  protected final List<Row> inputRows =
      LongStream.range(0, numRecords()).boxed().map(ROW_FUNC::apply).collect(Collectors.toList());

  /** Populates the Iceberg table and Returns a {@link List<Row>} of expected elements. */
  public List<Row> populateTable(Table table) throws IOException {
    return populateTable(table, null);
  }

  /** Populates the Iceberg table with rows, but overrides one field. */
  private List<Row> populateTable(Table table, @Nullable String charOverride) throws IOException {
    double recordsPerShardFraction = numRecords().doubleValue() / NUM_SHARDS;
    long maxRecordsPerShard = Math.round(Math.ceil(recordsPerShardFraction));

    table.updateProperties().set("write.metadata.metrics.default", "full").commit();
    AppendFiles appendFiles = table.newAppend();
    List<Row> expectedRows = new ArrayList<>(numRecords());
    int totalRecords = 0;
    for (int shardNum = 0; shardNum < NUM_SHARDS; ++shardNum) {
      String filepath = table.location() + "/" + UUID.randomUUID();
      OutputFile file;
      try (FileIO io = table.io()) {
        file = io.newOutputFile(filepath);
      }
      DataWriter<Record> writer =
          Parquet.writeData(file)
              .schema(ICEBERG_SCHEMA)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .withSpec(table.spec())
              .build();

      for (int recordNum = 0;
          recordNum < maxRecordsPerShard && totalRecords < numRecords();
          ++recordNum, ++totalRecords) {

        Row expectedBeamRow = ROW_FUNC.apply((long) recordNum);
        if (charOverride != null) {
          expectedBeamRow =
              Row.fromRow(expectedBeamRow).withFieldValue("char", charOverride).build();
        }
        Record icebergRecord = RECORD_FUNC.apply(expectedBeamRow);

        writer.write(icebergRecord);
        expectedRows.add(expectedBeamRow);
      }
      writer.close();
      appendFiles.appendFile(writer.toDataFile());
    }
    appendFiles.commit();

    return expectedRows;
  }

  private static Map<Integer, ?> constantsMap(
      FileScanTask task,
      BiFunction<Type, Object, Object> converter,
      org.apache.iceberg.Schema schema) {
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();
    org.apache.iceberg.Schema partitionSchema = TypeUtil.select(schema, idColumns);
    boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();

    if (projectsIdentityPartitionColumns) {
      return PartitionUtil.constantsMap(task, converter);
    } else {
      return Collections.emptyMap();
    }
  }

  private List<Record> readRecords(Table table) throws IOException {
    org.apache.iceberg.Schema tableSchema = table.schema();
    TableScan tableScan = table.newScan().project(tableSchema);
    List<Record> writtenRecords = new ArrayList<>();
    CloseableIterable<CombinedScanTask> tasks = tableScan.planTasks();
    for (CombinedScanTask task : tasks) {
      InputFilesDecryptor decryptor;
      try (FileIO io = table.io()) {
        decryptor = new InputFilesDecryptor(task, io, table.encryption());
      }
      for (FileScanTask fileTask : task.files()) {
        Map<Integer, ?> idToConstants =
            constantsMap(fileTask, IdentityPartitionConverters::convertConstant, tableSchema);
        InputFile inputFile = decryptor.getInputFile(fileTask);
        CloseableIterable<Record> iterable =
            Parquet.read(inputFile)
                .split(fileTask.start(), fileTask.length())
                .project(tableSchema)
                .createReaderFunc(
                    fileSchema ->
                        GenericParquetReaders.buildReader(tableSchema, fileSchema, idToConstants))
                .filter(fileTask.residual())
                .build();

        for (Record rec : iterable) {
          writtenRecords.add(rec);
        }
        iterable.close();
      }
    }
    tasks.close();
    return writtenRecords;
  }

  @Test
  public void testRead() throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    Map<String, Object> config = managedIcebergConfig(tableId());

    PCollection<Row> rows =
        pipeline.apply(Managed.read(ICEBERG).withConfig(config)).getSinglePCollection();

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithColumnPruning_keep() throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    List<String> fieldsToKeep = Arrays.asList("row", "str", "modulo_5", "nullable_long");
    RowFilter rowFilter = new RowFilter(BEAM_SCHEMA).keep(fieldsToKeep);

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    config.put("keep", fieldsToKeep);

    PCollection<Row> rows =
        pipeline.apply(Managed.read(ICEBERG).withConfig(config)).getSinglePCollection();
    PAssert.that(rows).containsInAnyOrder(rowFilter.filter(expectedRows));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithFilterAndColumnPruning_keep() throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);

    List<String> keepFields = Arrays.asList("datetime_tz", "modulo_5", "str");
    RowFilter rowFilter = new RowFilter(BEAM_SCHEMA).keep(keepFields);

    List<Row> expectedRows =
        populateTable(table).stream()
            .filter(
                row ->
                    row.getLogicalTypeValue("datetime", LocalDateTime.class)
                            .isAfter(LocalDateTime.parse("2025-01-01T09:00:00"))
                        && (row.getInt32("int_field") < 500 || row.getInt32("modulo_5") == 3))
            .map(rowFilter::filter)
            .collect(Collectors.toList());

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    config.put(
        "filter",
        "\"datetime\" > '2025-01-01 09:00' AND (\"int_field\" < 500 OR \"modulo_5\" = 3)");
    config.put("keep", keepFields);

    PCollection<Row> rows =
        pipeline.apply(Managed.read(ICEBERG).withConfig(config)).getSinglePCollection();
    assertEquals(rowFilter.outputSchema(), rows.getSchema());

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testStreamingReadWithFilter() throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);

    List<Row> expectedRows =
        populateTable(table).stream()
            .filter(
                row ->
                    row.getBoolean("bool_field")
                        && (row.getInt32("int_field") < 350 || row.getInt32("modulo_5") == 2))
            .collect(Collectors.toList());

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    config.put("streaming", true);
    config.put("to_snapshot", table.currentSnapshot().snapshotId());
    config.put("filter", "\"bool_field\" = TRUE AND (\"int_field\" < 350 OR \"modulo_5\" = 2)");

    PCollection<Row> rows =
        pipeline.apply(Managed.read(ICEBERG_CDC).withConfig(config)).getSinglePCollection();

    assertThat(rows.isBounded(), equalTo(UNBOUNDED));
    PAssert.that(rows).containsInAnyOrder(expectedRows);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testStreamingReadWithColumnPruning_drop() throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    List<String> fieldsToDrop = Arrays.asList("row", "str", "modulo_5", "nullable_long");
    RowFilter rowFilter = new RowFilter(BEAM_SCHEMA).drop(fieldsToDrop);

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    config.put("streaming", true);
    config.put("to_snapshot", table.currentSnapshot().snapshotId());
    config.put("drop", fieldsToDrop);

    PCollection<Row> rows =
        pipeline.apply(Managed.read(ICEBERG_CDC).withConfig(config)).getSinglePCollection();

    assertThat(rows.isBounded(), equalTo(UNBOUNDED));
    PAssert.that(rows).containsInAnyOrder(rowFilter.filter(expectedRows));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testBatchReadBetweenSnapshots() throws Exception {
    runReadBetween(true, false);
  }

  @Test
  public void testStreamingReadBetweenTimestamps() throws Exception {
    runReadBetween(false, true);
  }

  @Test
  public void testWriteRead() throws IOException {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);
    List<Row> expectedRows = populateTable(table);
    Map<String, Object> readConfig = managedIcebergConfig(tableId());
    String writeTableId = tableId() + "_2";
    Map<String, Object> writeConfig = managedIcebergConfig(writeTableId);

    pipeline
        .apply("read", Managed.read(ICEBERG).withConfig(readConfig))
        .getSinglePCollection()
        .apply("write", Managed.write(ICEBERG).withConfig(writeConfig));
    pipeline.run().waitUntilFinish();

    List<Record> returnedRecords =
        readRecords(catalog.loadTable(TableIdentifier.parse(writeTableId)));
    assertThat(
        returnedRecords,
        containsInAnyOrder(expectedRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testWriteReadWithFilter() throws IOException {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);
    List<Row> expectedRows =
        populateTable(table).stream()
            .filter(
                row ->
                    row.getBoolean("bool_field")
                        && (row.getInt32("int_field") < 350 || row.getInt32("modulo_5") == 2))
            .collect(Collectors.toList());
    Map<String, Object> readConfig = new HashMap<>(managedIcebergConfig(tableId()));
    readConfig.put("filter", "\"bool_field\" = TRUE AND (\"int_field\" < 350 OR \"modulo_5\" = 2)");
    String writeTableId = tableId() + "_2";
    Map<String, Object> writeConfig = managedIcebergConfig(writeTableId);

    pipeline
        .apply("read", Managed.read(ICEBERG).withConfig(readConfig))
        .getSinglePCollection()
        .apply("write", Managed.write(ICEBERG).withConfig(writeConfig));
    pipeline.run().waitUntilFinish();

    List<Record> returnedRecords =
        readRecords(catalog.loadTable(TableIdentifier.parse(writeTableId)));
    assertThat(
        returnedRecords,
        containsInAnyOrder(expectedRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testReadWriteStreaming() throws IOException {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);
    List<Row> expectedRows = populateTable(table);

    Map<String, Object> config = managedIcebergConfig(tableId());

    Map<String, Object> readConfig = new HashMap<>(config);
    readConfig.put("to_timestamp", System.currentTimeMillis());
    readConfig.put("streaming", true);

    String writeTableId = tableId() + "_2";
    Map<String, Object> writeConfig = new HashMap<>(config);
    writeConfig.put("triggering_frequency_seconds", 5);
    writeConfig.put("table", writeTableId);

    pipeline
        .apply("streaming read", Managed.read(ICEBERG_CDC).withConfig(readConfig))
        .getSinglePCollection()
        .apply("streaming write", Managed.write(ICEBERG).withConfig(writeConfig));
    pipeline.run().waitUntilFinish();

    List<Record> returnedRecords =
        readRecords(catalog.loadTable(TableIdentifier.parse(writeTableId)));
    assertThat(
        returnedRecords,
        containsInAnyOrder(expectedRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testWrite() throws IOException {
    // Write with Beam
    // Expect the sink to create the table
    Map<String, Object> config = managedIcebergConfig(tableId());
    PCollection<Row> input = pipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(TableIdentifier.parse(tableId()));
    assertTrue(table.schema().sameSchema(ICEBERG_SCHEMA));

    // Read back and check records are correct
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testWriteToPartitionedTable() throws IOException {
    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    int truncLength = "value_x".length();
    config.put(
        "partition_fields",
        Arrays.asList("bool_field", "hour(datetime)", "truncate(str, " + truncLength + ")"));
    PCollection<Row> input = pipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    // Read back and check records are correct
    Table table = catalog.loadTable(TableIdentifier.parse(tableId()));
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  private PeriodicImpulse getStreamingSource() {
    return PeriodicImpulse.create()
        .stopAfter(Duration.millis(numRecords() - 1))
        .withInterval(Duration.millis(1));
  }

  @Test
  public void testStreamingWrite() throws IOException {
    int numRecords = numRecords();

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    config.put("triggering_frequency_seconds", 4);
    config.put("partition_fields", Arrays.asList("bool_field", "modulo_5"));
    // Add table properties for testing
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.format.default", "parquet");
    tableProperties.put("commit.retry.num-retries", "3");
    config.put("table_properties", tableProperties);

    // create elements from longs in range [0, 1000)
    PCollection<Row> input =
        pipeline
            .apply(getStreamingSource())
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(instant -> ROW_FUNC.apply(instant.getMillis() % numRecords)))
            .setRowSchema(BEAM_SCHEMA);

    assertThat(input.isBounded(), equalTo(UNBOUNDED));

    input.apply(Managed.write(ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(TableIdentifier.parse(tableId()));
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
    assertEquals("parquet", table.properties().get("write.format.default"));
    assertEquals("3", table.properties().get("commit.retry.num-retries"));
  }

  @Test
  public void testStreamingWriteWithPriorWindowing() throws IOException {
    int numRecords = numRecords();

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    config.put("triggering_frequency_seconds", 4);
    config.put("partition_fields", Arrays.asList("bool_field", "modulo_5"));

    // over a span of 10 seconds, create elements from longs in range [0, 1000)
    PCollection<Row> input =
        pipeline
            .apply(getStreamingSource())
            .apply(
                Window.<Instant>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .accumulatingFiredPanes())
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(instant -> ROW_FUNC.apply(instant.getMillis() % numRecords)))
            .setRowSchema(BEAM_SCHEMA);

    assertThat(input.isBounded(), equalTo(UNBOUNDED));

    input.apply(Managed.write(ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(TableIdentifier.parse(tableId()));
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  private void writeToDynamicDestinations(@Nullable String filterOp) throws IOException {
    writeToDynamicDestinations(filterOp, false, false);
  }

  /**
   * @param filterOp if null, just perform a normal dynamic destination write test; otherwise,
   *     performs a simple filter on the record before writing. Valid options are "keep", "drop",
   *     and "only"
   */
  private void writeToDynamicDestinations(
      @Nullable String filterOp, boolean streaming, boolean partitioning) throws IOException {
    int numRecords = numRecords();
    String tableIdentifierTemplate = tableId() + "_{modulo_5}_{char}";
    Map<String, Object> writeConfig = new HashMap<>(managedIcebergConfig(tableIdentifierTemplate));

    List<String> fieldsToFilter = Arrays.asList("row", "str", "int_field", "nullable_long");
    // an un-configured filter will just return the same row
    RowFilter rowFilter = new RowFilter(BEAM_SCHEMA);
    if (filterOp != null) {
      switch (filterOp) {
        case "drop":
          rowFilter = rowFilter.drop(fieldsToFilter);
          writeConfig.put(filterOp, fieldsToFilter);
          break;
        case "keep":
          rowFilter = rowFilter.keep(fieldsToFilter);
          writeConfig.put(filterOp, fieldsToFilter);
          break;
        case "only":
          rowFilter = rowFilter.only(fieldsToFilter.get(0));
          writeConfig.put(filterOp, fieldsToFilter.get(0));
          break;
        default:
          throw new UnsupportedOperationException("Unknown operation: " + filterOp);
      }
    }

    if (partitioning) {
      Preconditions.checkState(filterOp == null || !filterOp.equals("only"));
      writeConfig.put("partition_fields", Arrays.asList("bool_field", "modulo_5"));
    }

    // Write with Beam
    PCollection<Row> input;
    if (streaming) {
      writeConfig.put("triggering_frequency_seconds", 5);
      input =
          pipeline
              .apply(getStreamingSource())
              .apply(
                  MapElements.into(TypeDescriptors.rows())
                      .via(instant -> ROW_FUNC.apply(instant.getMillis() % numRecords)));
    } else {
      input = pipeline.apply(Create.of(inputRows));
    }

    input.setRowSchema(BEAM_SCHEMA).apply(Managed.write(ICEBERG).withConfig(writeConfig));
    pipeline.run().waitUntilFinish();

    Table table0 = catalog.loadTable(TableIdentifier.parse(tableId() + "_0_a"));
    Table table1 = catalog.loadTable(TableIdentifier.parse(tableId() + "_1_b"));
    Table table2 = catalog.loadTable(TableIdentifier.parse(tableId() + "_2_c"));
    Table table3 = catalog.loadTable(TableIdentifier.parse(tableId() + "_3_d"));
    Table table4 = catalog.loadTable(TableIdentifier.parse(tableId() + "_4_e"));

    org.apache.iceberg.Schema tableSchema =
        IcebergUtils.beamSchemaToIcebergSchema(rowFilter.outputSchema());
    for (Table t : Arrays.asList(table0, table1, table2, table3, table4)) {
      assertTrue(t.schema().sameSchema(tableSchema));
    }

    // Read back and check records are correct
    List<List<Record>> returnedRecords =
        Arrays.asList(
            readRecords(table0),
            readRecords(table1),
            readRecords(table2),
            readRecords(table3),
            readRecords(table4));

    SerializableFunction<Row, Record> recordFunc =
        row -> IcebergUtils.beamRowToIcebergRecord(tableSchema, row);

    for (int i = 0; i < returnedRecords.size(); i++) {
      List<Record> records = returnedRecords.get(i);
      long l = i;
      Stream<Record> expectedRecords =
          inputRows.stream()
              .filter(rec -> checkStateNotNull(rec.getInt64("modulo_5")) == l)
              .map(rowFilter::filter)
              .map(recordFunc::apply);

      assertThat(records, containsInAnyOrder(expectedRecords.toArray()));
    }
  }

  @Test
  public void testWriteToDynamicDestinations() throws IOException {
    writeToDynamicDestinations(null);
  }

  @Test
  public void testWriteToDynamicDestinationsAndDropFields() throws IOException {
    writeToDynamicDestinations("drop");
  }

  @Test
  public void testWriteToDynamicDestinationsWithOnlyRecord() throws IOException {
    writeToDynamicDestinations("only");
  }

  @Test
  public void testStreamToDynamicDestinationsAndKeepFields() throws IOException {
    writeToDynamicDestinations("keep", true, false);
  }

  @Test
  public void testStreamToPartitionedDynamicDestinations() throws IOException {
    writeToDynamicDestinations(null, true, true);
  }

  @Test
  public void testWriteToDynamicNamespaces() throws IOException {
    // run this test only on catalogs that support namespace management
    assumeTrue(catalog instanceof SupportsNamespaces);

    String tableIdentifierTemplate = namespace() + "_{modulo_5}.table_{bool_field}";
    Map<String, Object> writeConfig = new HashMap<>(managedIcebergConfig(tableIdentifierTemplate));
    // override with table template
    writeConfig.put("table", tableIdentifierTemplate);

    Namespace namespace0 = Namespace.of(namespace() + "_0");
    Namespace namespace1 = Namespace.of(namespace() + "_1");
    Namespace namespace2 = Namespace.of(namespace() + "_2");
    Namespace namespace3 = Namespace.of(namespace() + "_3");
    Namespace namespace4 = Namespace.of(namespace() + "_4");

    TableIdentifier tableId0true = TableIdentifier.of(namespace0, "table_true");
    TableIdentifier tableId0false = TableIdentifier.of(namespace0, "table_false");
    TableIdentifier tableId1true = TableIdentifier.of(namespace1, "table_true");
    TableIdentifier tableId1false = TableIdentifier.of(namespace1, "table_false");
    TableIdentifier tableId2true = TableIdentifier.of(namespace2, "table_true");
    TableIdentifier tableId2false = TableIdentifier.of(namespace2, "table_false");
    TableIdentifier tableId3true = TableIdentifier.of(namespace3, "table_true");
    TableIdentifier tableId3false = TableIdentifier.of(namespace3, "table_false");
    TableIdentifier tableId4true = TableIdentifier.of(namespace4, "table_true");
    TableIdentifier tableId4false = TableIdentifier.of(namespace4, "table_false");

    List<Namespace> namespaces =
        Arrays.asList(namespace0, namespace1, namespace2, namespace3, namespace4);
    SupportsNamespaces sN = (SupportsNamespaces) catalog;
    // assert namespace don't exist beforehand
    namespaces.forEach(n -> assertFalse(sN.namespaceExists(n)));

    pipeline
        .apply(Create.of(inputRows))
        .setRowSchema(BEAM_SCHEMA)
        .apply(Managed.write(ICEBERG).withConfig(writeConfig));
    pipeline.run().waitUntilFinish();

    // assert namespace were created
    namespaces.forEach(n -> assertTrue(sN.namespaceExists(n)));

    Table table0true = catalog.loadTable(tableId0true);
    Table table0false = catalog.loadTable(tableId0false);
    Table table1true = catalog.loadTable(tableId1true);
    Table table1false = catalog.loadTable(tableId1false);
    Table table2true = catalog.loadTable(tableId2true);
    Table table2false = catalog.loadTable(tableId2false);
    Table table3true = catalog.loadTable(tableId3true);
    Table table3false = catalog.loadTable(tableId3false);
    Table table4true = catalog.loadTable(tableId4true);
    Table table4false = catalog.loadTable(tableId4false);

    for (Table t :
        Arrays.asList(
            table0true,
            table0false,
            table1true,
            table1false,
            table2true,
            table2false,
            table3true,
            table3false,
            table4true,
            table4false)) {
      assertTrue(t.schema().sameSchema(ICEBERG_SCHEMA));
    }

    // Read back and check records are correct
    Map<KV<Long, Boolean>, List<Record>> results =
        ImmutableMap.<KV<Long, Boolean>, List<Record>>builder()
            .put(KV.of(0L, true), readRecords(table0true))
            .put(KV.of(0L, false), readRecords(table0false))
            .put(KV.of(1L, true), readRecords(table1true))
            .put(KV.of(1L, false), readRecords(table1false))
            .put(KV.of(2L, true), readRecords(table2true))
            .put(KV.of(2L, false), readRecords(table2false))
            .put(KV.of(3L, true), readRecords(table3true))
            .put(KV.of(3L, false), readRecords(table3false))
            .put(KV.of(4L, true), readRecords(table4true))
            .put(KV.of(4L, false), readRecords(table4false))
            .build();

    for (Map.Entry<KV<Long, Boolean>, List<Record>> entry : results.entrySet()) {
      long modulo = entry.getKey().getKey();
      boolean bool = entry.getKey().getValue();
      List<Record> records = entry.getValue();
      Stream<Record> expectedRecords =
          inputRows.stream()
              .filter(
                  rec ->
                      checkStateNotNull(rec.getInt64("modulo_5")) == modulo
                          && checkStateNotNull(rec.getBoolean("bool_field")) == bool)
              .map(RECORD_FUNC::apply);

      assertThat(records, containsInAnyOrder(expectedRecords.toArray()));
    }

    namespaces.stream().map(Namespace::toString).forEach(namespacesToCleanup::add);
  }

  public void runReadBetween(boolean useSnapshotBoundary, boolean streaming) throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA);

    populateTable(table, "a"); // first snapshot
    Thread.sleep(AFTER_UPDATE_SLEEP_MS);
    List<Row> expectedRows = populateTable(table, "b"); // second snapshot
    Snapshot from = table.currentSnapshot();
    Thread.sleep(AFTER_UPDATE_SLEEP_MS);
    expectedRows.addAll(populateTable(table, "c")); // third snapshot
    Snapshot to = table.currentSnapshot();
    Thread.sleep(AFTER_UPDATE_SLEEP_MS);
    populateTable(table, "d"); // fourth snapshot
    Thread.sleep(AFTER_UPDATE_SLEEP_MS);

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    if (useSnapshotBoundary) {
      config.put("from_snapshot", from.snapshotId());
      config.put("to_snapshot", to.snapshotId());
    } else { // use timestamp boundary
      config.put("from_timestamp", from.timestampMillis() - 1);
      config.put("to_timestamp", to.timestampMillis() + 1);
    }
    config.put("streaming", streaming);

    PCollection<Row> rows =
        pipeline.apply(Managed.read(ICEBERG_CDC).withConfig(config)).getSinglePCollection();

    IsBounded expectedBoundedness = streaming ? UNBOUNDED : BOUNDED;
    assertEquals(expectedBoundedness, rows.isBounded());

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteWithTableProperties() throws IOException {
    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId()));
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.format.default", "parquet");
    tableProperties.put("commit.retry.num-retries", "3");
    config.put("table_properties", tableProperties);
    PCollection<Row> input = pipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(TableIdentifier.parse(tableId()));
    // Read back and check records are correct
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
    // Assert that the table properties are set on the Iceberg table
    assertEquals("parquet", table.properties().get("write.format.default"));
    assertEquals("3", table.properties().get("commit.retry.num-retries"));
  }
}
