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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IcebergIOLT implements Serializable {
  private static final Map<String, TestConfiguration> TEST_CONFIGS =
      // num rows --- num fields --- byte size per field
      ImmutableMap.of(
          "local", // 1K rows, >50 KB
          TestConfiguration.of(1_000L, 1, 50),
          "small", // 100K rows, >50 MB
          TestConfiguration.of(100_000L, 5, 100),
          "medium", // 10M rows, >10 GB
          TestConfiguration.of(10_000_000L, 10, 100),
          "large", // 1B rows, >2 TB
          TestConfiguration.of(1_000_000_000L, 20, 100));

  /** Options for Iceberg IO load test. */
  @AutoValue
  abstract static class TestConfiguration {
    /** Number of rows to generate. */
    abstract Long numRows();

    /** Data shape: The number of fields per row. */
    abstract Integer numFields();

    /** Data shape: The byte-size for each field. */
    abstract Integer byteSizePerField();

    static TestConfiguration of(Long numRows, Integer numFields, Integer byteSizePerField) {
      return new AutoValue_IcebergIOLT_TestConfiguration(numRows, numFields, byteSizePerField);
    }
  }

  public interface IcebergLoadTestPipelineOptions extends GcpOptions {
    @Description(
        "Size of the data written/read. Possible values are: ['local', 'small', 'medium', 'large'].")
    @Default.String("local")
    String getTestSize();

    void setTestSize(String testSize);
  }

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  static IcebergLoadTestPipelineOptions options;

  @Rule public TestName testName = new TestName();

  private String warehouseLocation;

  private TableIdentifier tableId;

  private static TestConfiguration testConfiguration;

  static org.apache.beam.sdk.schemas.Schema beamSchema;
  static Schema icebergSchema;

  @BeforeClass
  public static void beforeClass() {
    PipelineOptionsFactory.register(IcebergLoadTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(IcebergLoadTestPipelineOptions.class);

    testConfiguration = TEST_CONFIGS.get(options.getTestSize());

    org.apache.beam.sdk.schemas.Schema.Builder builder =
        org.apache.beam.sdk.schemas.Schema.builder();
    for (int i = 0; i < testConfiguration.numFields(); i++) {
      builder = builder.addByteArrayField("bytes_" + i);
    }
    beamSchema = builder.build();
    icebergSchema = SchemaAndRowConversions.beamSchemaToIcebergSchema(beamSchema);
  }

  @Before
  public void setUp() {
    warehouseLocation =
        String.format(
            "%s/IcebergIOLT/%s/%s",
            options.getTempLocation(), testName.getMethodName(), UUID.randomUUID());

    tableId =
        TableIdentifier.of(
            testName.getMethodName(), "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
  }

  @Test
  public void testReadWriteHadoopType() {
    Configuration catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", options.getProject());
    catalogHadoopConf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
    catalogHadoopConf.set(
        "fs.gs.auth.service.account.json.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));

    Catalog catalog = new HadoopCatalog(catalogHadoopConf, warehouseLocation);
    catalog.createTable(tableId, icebergSchema);

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

    PCollection<Long> source =
        writePipeline.apply(GenerateSequence.from(0).to(testConfiguration.numRows()));

    PCollection<Row> inputRows =
        source
            .apply(
                "Create Rows",
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        new GenerateRow(
                            testConfiguration.numFields(),
                            testConfiguration.byteSizePerField(),
                            beamSchema)))
            .setRowSchema(beamSchema);

    PCollectionRowTuple.of("input", inputRows)
        .apply(Managed.write(Managed.ICEBERG).withConfig(config));
    writePipeline.run().waitUntilFinish();

    // read pipeline
    PCollection<Long> countRows =
        PCollectionRowTuple.empty(readPipeline)
            .apply(Managed.read(Managed.ICEBERG).withConfig(config))
            .get("output")
            .apply(Count.globally());

    PAssert.thatSingleton(countRows).isEqualTo(testConfiguration.numRows());
    readPipeline.run().waitUntilFinish();
  }

  private static class GenerateRow implements SerializableFunction<Long, Row> {
    final int numFields;
    final int byteSizePerField;
    final org.apache.beam.sdk.schemas.Schema schema;

    GenerateRow(int numFields, int byteSizePerField, org.apache.beam.sdk.schemas.Schema schema) {
      this.numFields = numFields;
      this.byteSizePerField = byteSizePerField;
      this.schema = schema;
    }

    @Override
    public Row apply(Long input) {
      Row.Builder rowBuilder = Row.withSchema(schema);
      for (int i = 0; i < numFields; i++) {
        rowBuilder = rowBuilder.addValue(new byte[byteSizePerField]);
      }
      return rowBuilder.build();
    }
  }
}
