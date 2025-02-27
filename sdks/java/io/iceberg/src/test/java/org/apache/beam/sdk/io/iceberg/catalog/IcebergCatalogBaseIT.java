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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.PipelineResult;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
 *   <li>{@link #catalogCleanup()}
 * </ul>
 *
 * <p>1,000 records are used for each test by default. You can change this by overriding {@link
 * #numRecords()}.
 */
public abstract class IcebergCatalogBaseIT implements Serializable {

  protected long salt = System.nanoTime();

  public abstract Catalog createCatalog();

  public abstract Map<String, Object> managedIcebergConfig(String tableId);

  public void catalogSetup() throws Exception {}

  public void catalogCleanup() throws Exception {}

  public Integer numRecords() {
    return 1000;
  }

  public String tableId() {
    return testName.getMethodName() + ".test_table";
  }

  public static String warehouse(Class<? extends IcebergCatalogBaseIT> testClass, String random) {
    return String.format(
        "%s/%s/%s",
        TestPipeline.testingPipelineOptions().getTempLocation(), testClass.getSimpleName(), random);
  }

  public String catalogName = "test_catalog_" + System.nanoTime();

  @Before
  public void setUp() throws Exception {
    salt = System.nanoTime();
    catalogName = "test_catalog_" + System.nanoTime();
    catalogSetup();
    catalog = createCatalog();
  }

  @After
  public void cleanUp() throws Exception {
    try {
      catalogCleanup();
    } catch (Exception e) {
      LOG.warn("Catalog cleanup failed.", e);
    }
  }

  @BeforeClass
  public static void createWarehouse() {
    random = UUID.randomUUID().toString();
    warehouse = warehouse(IcebergCatalogBaseIT.class, random);
  }

  @AfterClass
  public static void cleanUpGCS() {
    try {
      GcsUtil gcsUtil = OPTIONS.as(GcsOptions.class).getGcsUtil();
      GcsPath path = GcsPath.fromUri(warehouse);

      @Nullable
      List<StorageObject> objects =
          gcsUtil
              .listObjects(
                  path.getBucket(),
                  IcebergCatalogBaseIT.class.getSimpleName() + "/" + path.getFileName().toString(),
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
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS files.", e);
    }
  }

  protected static String warehouse;
  public Catalog catalog;
  protected static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);
  protected static String random = UUID.randomUUID().toString();
  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TestName testName = new TestName();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(300);
  private static final int NUM_SHARDS = 10;
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
          .addBooleanField("bool")
          .addInt32Field("int")
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
              .addValue(new DateTime(num).withZone(DateTimeZone.forOffsetHoursMinutes(3, 25)))
              .addValue(DateTimeUtil.timestampFromMicros(num))
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
  private List<Row> populateTable(Table table) throws IOException {
    double recordsPerShardFraction = numRecords().doubleValue() / NUM_SHARDS;
    long maxRecordsPerShard = Math.round(Math.ceil(recordsPerShardFraction));

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

    try (CloseableIterable<CombinedScanTask> tasks = tableScan.planTasks();
        FileIO io = table.io()) {

      for (CombinedScanTask task : tasks) {
        InputFilesDecryptor decryptor = new InputFilesDecryptor(task, io, table.encryption());

        for (FileScanTask fileTask : task.files()) {
          long startTime = System.currentTimeMillis();
          LOG.info("Reading file: {}", fileTask.file().path());

          Map<Integer, ?> idToConstants =
              constantsMap(fileTask, IdentityPartitionConverters::convertConstant, tableSchema);
          InputFile inputFile = decryptor.getInputFile(fileTask);

          try (CloseableIterable<Record> iterable =
              Parquet.read(inputFile)
                  .split(fileTask.start(), fileTask.length())
                  .project(tableSchema)
                  .createReaderFunc(
                      fileSchema ->
                          GenericParquetReaders.buildReader(tableSchema, fileSchema, idToConstants))
                  .filter(fileTask.residual())
                  .build()) {

            for (Record rec : iterable) {
              writtenRecords.add(rec);
            }
          }
          LOG.info(
              "Finished reading file: {} in {} ms",
              fileTask.file().path(),
              System.currentTimeMillis() - startTime);
        }
      }
    }
    return writtenRecords;
  }

  @Test
  public void testRead() throws Exception {
    String tableId = tableId();
    Table table = catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    Map<String, Object> config = managedIcebergConfig(tableId);

    PCollection<Row> rows =
        pipeline.apply(Managed.read(Managed.ICEBERG).withConfig(config)).getSinglePCollection();

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWrite() throws IOException {
    // Write with Beam
    // Expect the sink to create the table
    String tableId = tableId();
    Map<String, Object> config = managedIcebergConfig(tableId);
    PCollection<Row> input = pipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    Table table = catalog.loadTable(TableIdentifier.parse(tableId));
    assertTrue(table.schema().sameSchema(ICEBERG_SCHEMA));

    // Read back and check records are correct
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testWriteToPartitionedTable() throws Exception {
    // For an example row where bool=true, modulo_5=3, str=value_303,
    // this partition spec will create a partition like: /bool=true/modulo_5=3/str_trunc=value_3/
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("bool")
            .hour("datetime")
            .truncate("str", "value_x".length())
            .build();
    String tableId = tableId();
    Table table =
        catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA, partitionSpec);
    LOG.info("TABLE CREATED: {}", tableId);
    verifyTableExists(TableIdentifier.parse(tableId));

    // Write with Beam
    Map<String, Object> config = managedIcebergConfig(tableId);
    PCollection<Row> input = pipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    // Read back and check records are correct
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
  public void testStreamingWrite() throws Exception {
    int numRecords = numRecords();
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("bool").identity("modulo_5").build();
    String tableId = tableId();
    Table table =
        catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA, partitionSpec);
    LOG.info("TABLE CREATED: {}", tableId);
    verifyTableExists(TableIdentifier.parse(tableId));

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId));
    config.put("triggering_frequency_seconds", 4);

    // create elements from longs in range [0, 1000)
    PCollection<Row> input =
        pipeline
            .apply(getStreamingSource())
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(instant -> ROW_FUNC.apply(instant.getMillis() % numRecords)))
            .setRowSchema(BEAM_SCHEMA);

    assertThat(input.isBounded(), equalTo(PCollection.IsBounded.UNBOUNDED));

    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testStreamingWriteWithPriorWindowing() throws Exception {
    int numRecords = numRecords();
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("bool").identity("modulo_5").build();
    String tableId = tableId();
    Table table =
        catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA, partitionSpec);
    LOG.info("TABLE CREATED: {}", tableId);
    verifyTableExists(TableIdentifier.parse(tableId));

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId));
    config.put("triggering_frequency_seconds", 4);

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

    assertThat(input.isBounded(), equalTo(PCollection.IsBounded.UNBOUNDED));

    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));
  }

  private void writeToDynamicDestinations(@Nullable String filterOp) throws Exception {
    writeToDynamicDestinations(filterOp, false, false);
  }

  public abstract void verifyTableExists(TableIdentifier tableIdentifier) throws Exception;

  /**
   * @param filterOp if null, just perform a normal dynamic destination write test; otherwise,
   *     performs a simple filter on the record before writing. Valid options are "keep", "drop",
   *     and "only"
   */
  private void writeToDynamicDestinations(
      @Nullable String filterOp, boolean streaming, boolean partitioning) throws Exception {
    int numRecords = numRecords();
    String tableId = tableId();
    String tableIdentifierTemplate = tableId + "_{modulo_5}_{char}";
    Map<String, Object> writeConfig = new HashMap<>(managedIcebergConfig(tableIdentifierTemplate));

    List<String> fieldsToFilter = Arrays.asList("row", "str", "int", "nullable_long");
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

    org.apache.iceberg.Schema tableSchema =
        IcebergUtils.beamSchemaToIcebergSchema(rowFilter.outputSchema());

    TableIdentifier tableIdentifier0 = TableIdentifier.parse(tableId + "_0_a");
    TableIdentifier tableIdentifier1 = TableIdentifier.parse(tableId + "_1_b");
    TableIdentifier tableIdentifier2 = TableIdentifier.parse(tableId + "_2_c");
    TableIdentifier tableIdentifier3 = TableIdentifier.parse(tableId + "_3_d");
    TableIdentifier tableIdentifier4 = TableIdentifier.parse(tableId + "_4_e");
    // the sink doesn't support creating partitioned tables yet,
    // so we need to create it manually for this test case
    if (partitioning) {
      Preconditions.checkState(filterOp == null || !filterOp.equals("only"));
      PartitionSpec partitionSpec =
          PartitionSpec.builderFor(tableSchema).identity("bool").identity("modulo_5").build();
      catalog.createTable(tableIdentifier0, tableSchema, partitionSpec);
      LOG.info("TABLE 0 CREATED");
      verifyTableExists(tableIdentifier0);
      catalog.createTable(tableIdentifier1, tableSchema, partitionSpec);
      LOG.info("TABLE 1 CREATED");
      verifyTableExists(tableIdentifier1);
      catalog.createTable(tableIdentifier2, tableSchema, partitionSpec);
      LOG.info("TABLE 2 CREATED");
      verifyTableExists(tableIdentifier2);
      catalog.createTable(tableIdentifier3, tableSchema, partitionSpec);
      LOG.info("TABLE 3 CREATED");
      verifyTableExists(tableIdentifier4);
      catalog.createTable(tableIdentifier4, tableSchema, partitionSpec);
      LOG.info("TABLE 4 CREATED");
      verifyTableExists(tableIdentifier4);
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

    input.setRowSchema(BEAM_SCHEMA).apply(Managed.write(Managed.ICEBERG).withConfig(writeConfig));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    Table table0 = catalog.loadTable(tableIdentifier0);
    Table table1 = catalog.loadTable(tableIdentifier1);
    Table table2 = catalog.loadTable(tableIdentifier2);
    Table table3 = catalog.loadTable(tableIdentifier3);
    Table table4 = catalog.loadTable(tableIdentifier4);

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
  public void testWriteToDynamicDestinations() throws Exception {
    writeToDynamicDestinations(null);
  }

  @Test
  public void testWriteToDynamicDestinationsAndDropFields() throws Exception {
    writeToDynamicDestinations("drop");
  }

  @Test
  public void testWriteToDynamicDestinationsWithOnlyRecord() throws Exception {
    writeToDynamicDestinations("only");
  }

  @Test
  public void testStreamToDynamicDestinationsAndKeepFields() throws Exception {
    writeToDynamicDestinations("keep", true, false);
  }

  @Test
  public void testStreamToPartitionedDynamicDestinations() throws Exception {
    writeToDynamicDestinations(null, true, true);
  }
}
