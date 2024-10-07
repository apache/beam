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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.managed.Managed;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.DateTimeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link IcebergIO} source and sink. */
@RunWith(JUnit4.class)
public class IcebergIOIT implements Serializable {
  private static final org.apache.beam.sdk.schemas.Schema DOUBLY_NESTED_ROW_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addStringField("doubly_nested_str")
          .addInt64Field("doubly_nested_float")
          .build();

  private static final org.apache.beam.sdk.schemas.Schema NESTED_ROW_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addStringField("nested_str")
          .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
          .addInt32Field("nested_int")
          .addFloatField("nested_float")
          .build();
  private static final org.apache.beam.sdk.schemas.Schema BEAM_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addStringField("str")
          .addStringField("char")
          .addInt64Field("modulo_5")
          .addBooleanField("bool")
          .addInt32Field("int")
          .addRowField("row", NESTED_ROW_SCHEMA)
          .addArrayField("arr_long", org.apache.beam.sdk.schemas.Schema.FieldType.INT64)
          .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
          .addNullableInt64Field("nullable_long")
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
              .addValue(DateTimeUtil.timestampFromMicros(num))
              .addValue(DateTimeUtil.dateFromDays(Integer.parseInt(strNum)))
              .addValue(DateTimeUtil.timeFromMicros(num))
              .build();
        }
      };

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA);
  private static final SimpleFunction<Row, Record> RECORD_FUNC =
      new SimpleFunction<Row, Record>() {
        @Override
        public Record apply(Row input) {
          return IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, input);
        }
      };
  private static final Integer NUM_RECORDS = 1000;
  private static final Integer NUM_SHARDS = 10;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  static GcpOptions options;

  static Configuration catalogHadoopConf;

  @Rule public TestName testName = new TestName();

  private String warehouseLocation;

  private String tableId;
  private Catalog catalog;

  @BeforeClass
  public static void beforeClass() {
    options = TestPipeline.testingPipelineOptions().as(GcpOptions.class);

    catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", options.getProject());
    catalogHadoopConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT");
  }

  @Before
  public void setUp() {
    warehouseLocation =
        String.format("%s/IcebergIOIT/%s", options.getTempLocation(), UUID.randomUUID());

    tableId = testName.getMethodName() + ".test_table";
    catalog = new HadoopCatalog(catalogHadoopConf, warehouseLocation);
  }

  /** Populates the Iceberg table and Returns a {@link List<Row>} of expected elements. */
  private List<Row> populateTable(Table table) throws IOException {
    double recordsPerShardFraction = NUM_RECORDS.doubleValue() / NUM_SHARDS;
    long maxRecordsPerShard = Math.round(Math.ceil(recordsPerShardFraction));

    AppendFiles appendFiles = table.newAppend();
    List<Row> expectedRows = new ArrayList<>(NUM_RECORDS);
    int totalRecords = 0;
    for (int shardNum = 0; shardNum < NUM_SHARDS; ++shardNum) {
      String filepath = table.location() + "/" + UUID.randomUUID();
      OutputFile file = table.io().newOutputFile(filepath);
      DataWriter<Record> writer =
          Parquet.writeData(file)
              .schema(ICEBERG_SCHEMA)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .overwrite()
              .withSpec(table.spec())
              .build();

      for (int recordNum = 0;
          recordNum < maxRecordsPerShard && totalRecords < NUM_RECORDS;
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

  private List<Record> readRecords(Table table) {
    Schema tableSchema = table.schema();
    TableScan tableScan = table.newScan().project(tableSchema);
    List<Record> writtenRecords = new ArrayList<>();
    for (CombinedScanTask task : tableScan.planTasks()) {
      InputFilesDecryptor descryptor =
          new InputFilesDecryptor(task, table.io(), table.encryption());
      for (FileScanTask fileTask : task.files()) {
        InputFile inputFile = descryptor.getInputFile(fileTask);
        CloseableIterable<Record> iterable =
            Parquet.read(inputFile)
                .split(fileTask.start(), fileTask.length())
                .project(tableSchema)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(tableSchema, fileSchema))
                .filter(fileTask.residual())
                .build();

        for (Record rec : iterable) {
          writtenRecords.add(rec);
        }
      }
    }
    return writtenRecords;
  }

  private Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put("catalog_name", "test-name")
        .put(
            "catalog_properties",
            ImmutableMap.<String, String>builder()
                .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
                .put("warehouse", warehouseLocation)
                .build())
        .build();
  }

  /**
   * Test of a predetermined moderate number of records written directly to Iceberg then read via a
   * Beam pipeline. Table initialization is done on a single process using the Iceberg APIs so the
   * data cannot be "big".
   */
  @Test
  public void testRead() throws Exception {
    Table table = catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    Map<String, Object> config = managedIcebergConfig(tableId);

    PCollection<Row> rows =
        pipeline.apply(Managed.read(Managed.ICEBERG).withConfig(config)).getSinglePCollection();

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    pipeline.run().waitUntilFinish();
  }

  private static final List<Row> INPUT_ROWS =
      LongStream.range(0, NUM_RECORDS).boxed().map(ROW_FUNC::apply).collect(Collectors.toList());

  /**
   * Test of a predetermined moderate number of records written to Iceberg using a Beam pipeline,
   * then read directly using Iceberg API.
   */
  @Test
  public void testWrite() {
    Table table = catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA);

    // Write with Beam
    Map<String, Object> config = managedIcebergConfig(tableId);
    PCollection<Row> input = pipeline.apply(Create.of(INPUT_ROWS)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    // Read back and check records are correct
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(INPUT_ROWS.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testWritePartitionedData() {
    // For an example row where bool=true, modulo_5=3, str=value_303,
    // this partition spec will create a partition like: /bool=true/modulo_5=3/str_trunc=value_3/
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("bool")
            .identity("modulo_5")
            .truncate("str", "value_x".length())
            .build();
    Table table =
        catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA, partitionSpec);

    // Write with Beam
    Map<String, Object> config = managedIcebergConfig(tableId);
    PCollection<Row> input = pipeline.apply(Create.of(INPUT_ROWS)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    // Read back and check records are correct
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(INPUT_ROWS.stream().map(RECORD_FUNC::apply).toArray()));
  }

  private PeriodicImpulse getStreamingSource() {
    return PeriodicImpulse.create()
        .stopAfter(Duration.millis(NUM_RECORDS - 1))
        .withInterval(Duration.millis(1));
  }

  @Test
  public void testStreamingWrite() {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("bool").identity("modulo_5").build();
    Table table =
        catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA, partitionSpec);

    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId));
    config.put("triggering_frequency_seconds", 4);

    // create elements from longs in range [0, 1000)
    PCollection<Row> input =
        pipeline
            .apply(getStreamingSource())
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(instant -> ROW_FUNC.apply(instant.getMillis() % NUM_RECORDS)))
            .setRowSchema(BEAM_SCHEMA);

    assertThat(input.isBounded(), equalTo(PCollection.IsBounded.UNBOUNDED));

    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(INPUT_ROWS.stream().map(RECORD_FUNC::apply).toArray()));
  }

  @Test
  public void testStreamingWriteWithPriorWindowing() {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("bool").identity("modulo_5").build();
    Table table =
        catalog.createTable(TableIdentifier.parse(tableId), ICEBERG_SCHEMA, partitionSpec);

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
                    .via(instant -> ROW_FUNC.apply(instant.getMillis() % NUM_RECORDS)))
            .setRowSchema(BEAM_SCHEMA);

    assertThat(input.isBounded(), equalTo(PCollection.IsBounded.UNBOUNDED));

    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(INPUT_ROWS.stream().map(RECORD_FUNC::apply).toArray()));
  }

  private void writeToDynamicDestinations(@Nullable String filterOp) {
    writeToDynamicDestinations(filterOp, false, false);
  }

  /**
   * @param filterOp if null, just perform a normal dynamic destination write test; otherwise,
   *     performs a simple filter on the record before writing. Valid options are "keep", "drop",
   *     and "only"
   */
  private void writeToDynamicDestinations(
      @Nullable String filterOp, boolean streaming, boolean partitioning) {
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

    Schema tableSchema = IcebergUtils.beamSchemaToIcebergSchema(rowFilter.outputSchema());

    PartitionSpec partitionSpec = null;
    if (partitioning) {
      Preconditions.checkState(filterOp == null || !filterOp.equals("only"));
      partitionSpec =
          PartitionSpec.builderFor(tableSchema).identity("bool").identity("modulo_5").build();
    }
    Table table0 =
        catalog.createTable(TableIdentifier.parse(tableId + "_0_a"), tableSchema, partitionSpec);
    Table table1 =
        catalog.createTable(TableIdentifier.parse(tableId + "_1_b"), tableSchema, partitionSpec);
    Table table2 =
        catalog.createTable(TableIdentifier.parse(tableId + "_2_c"), tableSchema, partitionSpec);
    Table table3 =
        catalog.createTable(TableIdentifier.parse(tableId + "_3_d"), tableSchema, partitionSpec);
    Table table4 =
        catalog.createTable(TableIdentifier.parse(tableId + "_4_e"), tableSchema, partitionSpec);

    // Write with Beam
    PCollection<Row> input;
    if (streaming) {
      writeConfig.put("triggering_frequency_seconds", 5);
      input =
          pipeline
              .apply(getStreamingSource())
              .apply(
                  MapElements.into(TypeDescriptors.rows())
                      .via(instant -> ROW_FUNC.apply(instant.getMillis() % NUM_RECORDS)));
    } else {
      input = pipeline.apply(Create.of(INPUT_ROWS));
    }
    input.setRowSchema(BEAM_SCHEMA).apply(Managed.write(Managed.ICEBERG).withConfig(writeConfig));
    pipeline.run().waitUntilFinish();

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
          INPUT_ROWS.stream()
              .filter(rec -> checkStateNotNull(rec.getInt64("modulo_5")) == l)
              .map(rowFilter::filter)
              .map(recordFunc::apply);

      assertThat(records, containsInAnyOrder(expectedRecords.toArray()));
    }
  }

  @Test
  public void testWriteToDynamicDestinations() {
    writeToDynamicDestinations(null);
  }

  @Test
  public void testWriteToDynamicDestinationsAndDropFields() {
    writeToDynamicDestinations("drop");
  }

  @Test
  public void testWriteToDynamicDestinationsAndKeepFields() {
    writeToDynamicDestinations("keep");
  }

  @Test
  public void testWriteToDynamicDestinationsWithOnlyRecord() {
    writeToDynamicDestinations("only");
  }

  @Test
  public void testStreamToDynamicDestinationsAndKeepFields() {
    writeToDynamicDestinations("keep", true, false);
  }

  @Test
  public void testStreamToPartitionedDynamicDestinations() {
    writeToDynamicDestinations(null, true, true);
  }
}
