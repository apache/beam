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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/** Integration tests for reading and writing Iceberg tables using the BigQuery Metastore Catalog */
public class BigQueryMetastoreCatalogIT {
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

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static final String TEST_CATALOG = "beam_test_" + System.nanoTime();
  private static final String DATASET = "iceberg_bigquerymetastore_test_" + System.nanoTime();
  @Rule public TestName testName = new TestName();
  private static final String WAREHOUSE = TestPipeline.testingPipelineOptions().getTempLocation();
  private static Catalog catalog;
  private static Map<String, String> catalogProps;
  private TableIdentifier tableIdentifier;

  @BeforeClass
  public static void setUp() {
    GcpOptions options = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    catalogProps =
        ImmutableMap.<String, String>builder()
            .put("gcp_project", options.getProject())
            .put("gcp_location", "us-central1")
            .put("catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog")
            .put("warehouse", WAREHOUSE)
            .build();
    catalog =
        CatalogUtil.loadCatalog(
            catalogProps.get("catalog-impl"), TEST_CATALOG, catalogProps, new Configuration());
    catalog.initialize(TEST_CATALOG, catalogProps);
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(DATASET));
  }

  @After
  public void cleanup() {
    // We need to cleanup tables first before deleting the dataset
    catalog.dropTable(tableIdentifier);
  }

  @AfterClass
  public static void tearDown() {
    ((SupportsNamespaces) catalog).dropNamespace(Namespace.of(DATASET));
  }

  private Map<String, Object> getManagedIcebergConfig(TableIdentifier table) {
    return ImmutableMap.<String, Object>builder()
        .put("table", table.toString())
        .put("catalog_name", TEST_CATALOG)
        .put("catalog_properties", catalogProps)
        .build();
  }

  @Test
  public void testReadWithBqmsCatalog() throws IOException {
    tableIdentifier =
        TableIdentifier.parse(String.format("%s.%s", DATASET, testName.getMethodName()));
    Table table = catalog.createTable(tableIdentifier, ICEBERG_SCHEMA);

    List<Row> expectedRows =
        LongStream.range(1, 1000).boxed().map(ROW_FUNC::apply).collect(Collectors.toList());
    List<Record> records =
        expectedRows.stream().map(RECORD_FUNC::apply).collect(Collectors.toList());

    // write iceberg records with bqms catalog
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
  public void testWriteWithBqmsCatalog() {
    tableIdentifier =
        TableIdentifier.parse(String.format("%s.%s", DATASET, testName.getMethodName()));
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
      InputFilesDecryptor descryptor =
          new InputFilesDecryptor(task, table.io(), table.encryption());
      for (FileScanTask fileTask : task.files()) {
        InputFile inputFile = descryptor.getInputFile(fileTask);
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
