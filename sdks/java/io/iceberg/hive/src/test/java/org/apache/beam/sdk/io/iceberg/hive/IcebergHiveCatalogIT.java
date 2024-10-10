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
package org.apache.beam.sdk.io.iceberg.hive;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.hive.testutils.HiveMetastoreExtension;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Read and write test for {@link Managed} {@link org.apache.beam.sdk.io.iceberg.IcebergIO} using
 * {@link HiveCatalog}.
 *
 * <p>Spins up a local Hive metastore to manage the Iceberg table. Warehouse path is set to a GCS
 * bucket.
 */
public class IcebergHiveCatalogIT {
  private static final Schema DOUBLY_NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("doubly_nested_str")
          .addInt64Field("doubly_nested_float")
          .build();

  private static final Schema NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("nested_str")
          .addInt32Field("nested_int")
          .addFloatField("nested_float")
          .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
          .build();
  private static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addBooleanField("bool")
          .addNullableInt32Field("nullable_int")
          .addNullableInt64Field("nullable_long")
          .addArrayField("arr_long", Schema.FieldType.INT64)
          .addRowField("row", NESTED_ROW_SCHEMA)
          .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
          .build();

  private static final SimpleFunction<Long, Row> ROW_FUNC =
      new SimpleFunction<Long, Row>() {
        @Override
        public Row apply(Long num) {
          String strNum = Long.toString(num);
          Row nestedRow =
              Row.withSchema(NESTED_ROW_SCHEMA)
                  .addValue("nested_str_value_" + strNum)
                  .addValue(Integer.valueOf(strNum))
                  .addValue(Float.valueOf(strNum + "." + strNum))
                  .addValue(
                      Row.withSchema(DOUBLY_NESTED_ROW_SCHEMA)
                          .addValue("doubly_nested_str_value_" + strNum)
                          .addValue(num)
                          .build())
                  .build();

          return Row.withSchema(BEAM_SCHEMA)
              .addValue("str_value_" + strNum)
              .addValue(num % 2 == 0)
              .addValue(Integer.valueOf(strNum))
              .addValue(num)
              .addValue(LongStream.range(1, num % 10).boxed().collect(Collectors.toList()))
              .addValue(nestedRow)
              .addValue(num % 2 == 0 ? null : nestedRow)
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

  private static HiveMetastoreExtension hiveMetastoreExtension;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_TABLE = "test_table";
  private static HiveCatalog catalog;
  private static final String TEST_DB = "test_db_" + System.nanoTime();

  @BeforeClass
  public static void setUp() throws TException {
    String warehousePath = TestPipeline.testingPipelineOptions().getTempLocation();
    hiveMetastoreExtension = new HiveMetastoreExtension(warehousePath);
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                TEST_CATALOG,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                hiveMetastoreExtension.hiveConf());

    String dbPath = hiveMetastoreExtension.metastore().getDatabasePath(TEST_DB);
    Database db = new Database(TEST_DB, "description", dbPath, Maps.newHashMap());
    hiveMetastoreExtension.metastoreClient().createDatabase(db);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    hiveMetastoreExtension.cleanup();
  }

  private Map<String, Object> getManagedIcebergConfig(TableIdentifier table) {
    String metastoreUri = hiveMetastoreExtension.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);

    Map<String, String> confProperties =
        ImmutableMap.<String, String>builder()
            .put(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUri)
            .build();

    return ImmutableMap.<String, Object>builder()
        .put("table", table.toString())
        .put("config_properties", confProperties)
        .build();
  }

  @Test
  public void testReadWithHiveCatalog() throws IOException {
    TableIdentifier tableIdentifier =
        TableIdentifier.parse(String.format("%s.%s", TEST_DB, TEST_TABLE + "_read_test"));
    Table table = catalog.createTable(tableIdentifier, ICEBERG_SCHEMA);

    List<Row> expectedRows =
        LongStream.range(1, 1000).boxed().map(ROW_FUNC::apply).collect(Collectors.toList());
    List<Record> records =
        expectedRows.stream().map(RECORD_FUNC::apply).collect(Collectors.toList());

    // write iceberg records with hive catalog
    String filepath = table.location() + "/" + UUID.randomUUID();
    DataWriter<Record> writer =
        Parquet.writeData(table.io().newOutputFile(filepath))
            .schema(ICEBERG_SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(table.spec())
            .build();
    for (Record rec : records) {
      writer.write(rec);
    }
    writer.close();
    AppendFiles appendFiles = table.newAppend();
    String manifestFilename = FileFormat.AVRO.addExtension(filepath + ".manifest");
    OutputFile outputFile = table.io().newOutputFile(manifestFilename);
    ManifestWriter<DataFile> manifestWriter;
    try (ManifestWriter<DataFile> openWriter = ManifestFiles.write(table.spec(), outputFile)) {
      openWriter.add(writer.toDataFile());
      manifestWriter = openWriter;
    }
    appendFiles.appendManifest(manifestWriter.toManifestFile());
    appendFiles.commit();

    // Run Managed Iceberg read
    PCollection<Row> outputRows =
        readPipeline
            .apply(
                Managed.read(Managed.ICEBERG).withConfig(getManagedIcebergConfig(tableIdentifier)))
            .getSinglePCollection();
    PAssert.that(outputRows).containsInAnyOrder(expectedRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteWithHiveCatalog() {
    TableIdentifier tableIdentifier =
        TableIdentifier.parse(String.format("%s.%s", TEST_DB, TEST_TABLE + "_write_test"));
    catalog.createTable(tableIdentifier, IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA));

    List<Row> inputRows =
        LongStream.range(1, 1000).mapToObj(ROW_FUNC::apply).collect(Collectors.toList());
    List<Record> expectedRecords =
        inputRows.stream().map(RECORD_FUNC::apply).collect(Collectors.toList());

    // Run Managed Iceberg write
    writePipeline
        .apply(Create.of(inputRows))
        .setRowSchema(BEAM_SCHEMA)
        .apply(Managed.write(Managed.ICEBERG).withConfig(getManagedIcebergConfig(tableIdentifier)));
    writePipeline.run().waitUntilFinish();

    // read back the records and check everything's there
    Table table = catalog.loadTable(tableIdentifier);
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
    assertThat(expectedRecords, containsInAnyOrder(writtenRecords.toArray()));
  }
}
