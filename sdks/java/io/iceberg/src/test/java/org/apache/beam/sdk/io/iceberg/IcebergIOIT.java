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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IcebergIOIT implements Serializable {

  public interface IcebergIOTestPipelineOptions extends GcpOptions {
    @Description("Number of records that will be written and/or read by the test")
    @Default.Integer(1000)
    Integer getNumRecords();

    void setNumRecords(Integer numRecords);

    @Description("Number of shards in the test table")
    @Default.Integer(10)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  static IcebergIOTestPipelineOptions options;

  static Configuration catalogHadoopConf;

  @Rule public TestName testName = new TestName();

  private String warehouseLocation;

  private TableIdentifier tableId;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptionsFactory.register(IcebergIOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(IcebergIOTestPipelineOptions.class);

    catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", options.getProject());
    catalogHadoopConf.set(
        "fs.gs.auth.service.account.json.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
  }

  @Before
  public void setUp() {
    warehouseLocation =
        String.format(
            "%s/IcebergIOIT/%s/%s",
            options.getTempLocation(), testName.getMethodName(), UUID.randomUUID());

    tableId =
        TableIdentifier.of(
            testName.getMethodName(), "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
  }

  static final org.apache.beam.sdk.schemas.Schema BEAM_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addInt32Field("int")
          .addFloatField("float")
          .addDoubleField("double")
          .addInt64Field("long")
          .addStringField("str")
          .addBooleanField("bool")
          .addByteArrayField("bytes")
          .build();

  static final Schema ICEBERG_SCHEMA =
      SchemaAndRowConversions.beamSchemaToIcebergSchema(BEAM_SCHEMA);

  Map<String, Object> getValues(int num) {
    String strNum = Integer.toString(num);
    return ImmutableMap.<String, Object>builder()
        .put("int", num)
        .put("float", Float.valueOf(strNum))
        .put("double", Double.valueOf(strNum))
        .put("long", Long.valueOf(strNum))
        .put("str", strNum)
        .put("bool", num % 2 == 0)
        .put("bytes", ByteBuffer.wrap(new byte[] {(byte) num}))
        .build();
  }

  /**
   * Populates the Iceberg table according to the configuration specified in {@link
   * IcebergIOTestPipelineOptions}. Returns a {@link List<Row>} of expected elements.
   */
  List<Row> populateTable(Table table) throws IOException {
    double recordsPerShardFraction = options.getNumRecords().doubleValue() / options.getNumShards();
    long maxRecordsPerShard = Math.round(Math.ceil(recordsPerShardFraction));

    AppendFiles appendFiles = table.newAppend();
    List<Row> expectedRows = new ArrayList<>(options.getNumRecords());
    int totalRecords = 0;
    for (int shardNum = 0; shardNum < options.getNumShards(); ++shardNum) {
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
          recordNum < maxRecordsPerShard && totalRecords < options.getNumRecords();
          ++recordNum, ++totalRecords) {
        Map<String, Object> values = getValues(recordNum);

        GenericRecord rec = GenericRecord.create(ICEBERG_SCHEMA).copy(values);
        writer.write(rec);

        expectedRows.add(Row.withSchema(BEAM_SCHEMA).withFieldValues(values).build());
      }
      writer.close();
      appendFiles.appendFile(writer.toDataFile());
    }
    appendFiles.commit();

    return expectedRows;
  }

  /**
   * Test of a predetermined moderate number of records written directly to Iceberg then read via a
   * Beam pipeline. Table initialization is done on a single process using the Iceberg APIs so the
   * data cannot be "big".
   */
  @Test
  public void testRead() throws Exception {
    Catalog catalog = new HadoopCatalog(catalogHadoopConf, warehouseLocation);
    Table table = catalog.createTable(tableId, ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    Map<String, Object> config =
        ImmutableMap.<String, Object>builder()
            .put("table", tableId.toString())
            .put(
                "catalog_config",
                ImmutableMap.<String, String>builder()
                    .put("catalog_name", "hadoop")
                    .put("catalog_type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
                    .put("warehouse_location", warehouseLocation)
                    .build())
            .build();

    PCollectionRowTuple output =
        PCollectionRowTuple.empty(readPipeline)
            .apply(Managed.read(Managed.ICEBERG).withConfig(config));

    PAssert.that(output.get("output")).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  /**
   * Test of a predetermined moderate number of records written to Iceberg using a Beam pipeline,
   * then read directly using Iceberg API.
   */
  @Test
  public void testWrite() {
    Catalog catalog = new HadoopCatalog(catalogHadoopConf, warehouseLocation);
    Table table = catalog.createTable(tableId, ICEBERG_SCHEMA);

    List<Record> inputRecords =
        IntStream.range(0, options.getNumRecords())
            .boxed()
            .map(i -> GenericRecord.create(ICEBERG_SCHEMA).copy(getValues(i)))
            .collect(Collectors.toList());

    List<Row> inputRows =
        inputRecords.stream()
            .map(record -> SchemaAndRowConversions.recordToRow(BEAM_SCHEMA, record))
            .collect(Collectors.toList());

    // Write with Beam
    Map<String, Object> config =
        ImmutableMap.<String, Object>builder()
            .put("table", tableId.toString())
            .put(
                "catalog_config",
                ImmutableMap.<String, String>builder()
                    .put("catalog_name", "hadoop")
                    .put("catalog_type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
                    .put("warehouse_location", warehouseLocation)
                    .build())
            .build();

    PCollection<Row> input = writePipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    PCollectionRowTuple.of("input", input).apply(Managed.write(Managed.ICEBERG).withConfig(config));

    writePipeline.run().waitUntilFinish();

    // Read back and check records are correct
    TableScan tableScan = table.newScan().project(ICEBERG_SCHEMA);
    List<Record> writtenRecords = new ArrayList<>();
    for (CombinedScanTask task : tableScan.planTasks()) {
      InputFilesDecryptor decryptor = new InputFilesDecryptor(task, table.io(), table.encryption());
      for (FileScanTask fileTask : task.files()) {
        InputFile inputFile = decryptor.getInputFile(fileTask);
        CloseableIterable<Record> iterable =
            Parquet.read(inputFile)
                .split(fileTask.start(), fileTask.length())
                .project(ICEBERG_SCHEMA)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(ICEBERG_SCHEMA, fileSchema))
                .filter(fileTask.residual())
                .build();

        for (Record rec : iterable) {
          writtenRecords.add(rec);
        }
      }
    }

    assertThat(inputRecords, containsInAnyOrder(writtenRecords.toArray()));
  }
}
