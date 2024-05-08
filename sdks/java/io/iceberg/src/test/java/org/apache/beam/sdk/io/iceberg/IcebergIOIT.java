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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IcebergIOIT implements Serializable {

  public interface IcebergIOTestPipelineOptions extends GcpOptions {
    @Description("Size of each record in bytes (for tests that vary record size)")
    @Default.Integer(100) // deliberately small so no-args execution is quick
    Integer getBytesPerRecord();

    void setBytesPerRecord(Integer bytesPerRecord);

    @Description("Number of records that will be written and/or read by the test")
    @Default.Long(1000) // deliberately small so no-args execution is quick
    Long getNumRecords();

    void setNumRecords(Long numRecords);

    @Description("Number of shards in the test table")
    @Default.Integer(10)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  static IcebergIOTestPipelineOptions options;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptionsFactory.register(IcebergIOTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(IcebergIOTestPipelineOptions.class);
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

  /**
   * Populates the Iceberg table according to the configuration specified in {@link
   * IcebergIOTestPipelineOptions}. Returns a {@link List<Row>} of expected elements.
   */
  List<Row> populateTable(Table table) throws IOException {
    double recordsPerShardFraction = options.getNumRecords().doubleValue() / options.getNumShards();
    long maxRecordsPerShard = Math.round(Math.ceil(recordsPerShardFraction));

    AppendFiles appendFiles = table.newAppend();
    List<Row> expectedRows = new ArrayList<>(options.getNumRecords().intValue());
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
        String strRecordNum = Integer.toString(recordNum);
        Map<String, Object> values =
            ImmutableMap.<String, Object>builder()
                .put("int", recordNum)
                .put("float", Float.valueOf(strRecordNum))
                .put("double", Double.valueOf(strRecordNum))
                .put("long", Long.valueOf(strRecordNum))
                .put("str", strRecordNum)
                .put("bool", recordNum % 2 == 0)
                .put("bytes", ByteBuffer.wrap(new byte[] {(byte) recordNum}))
                .build();
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
    Configuration catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", options.getProject());
    catalogHadoopConf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
    catalogHadoopConf.set(
        "fs.gs.auth.service.account.json.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
    String warehouseLocation =
        options.getTempLocation() + "/IcebergIOIT/testRead/" + UUID.randomUUID();

    Catalog catalog = new HadoopCatalog(catalogHadoopConf, warehouseLocation);
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table table = catalog.createTable(tableId, ICEBERG_SCHEMA);

    List<Row> expectedRows = populateTable(table);

    // Read with Dataflow
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setName("hadoop")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouseLocation)
            .build();

    PCollection<Row> output = readPipeline.apply(IcebergIO.readRows(catalogConfig).from(tableId));

    PAssert.that(output).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }
}
