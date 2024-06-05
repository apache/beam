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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.TestBigQueryOptions.BIGQUERY_EARLY_ROLLOUT_REGION;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link BigQueryIO#read(SerializableFunction)} using {@link
 * Method#DIRECT_READ}. This test reads from a pre-defined table and asserts that the number of
 * records read is equal to the expected count.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageReadIT {

  private static final Map<String, Long> EXPECTED_NUM_RECORDS =
      ImmutableMap.<String, Long>of(
          "empty", 0L,
          "1M", 10592L,
          "1G", 11110839L,
          "1T", 11110839000L,
          "multi_field", 11110839L);

  private static final String DATASET_ID =
      TestPipeline.testingPipelineOptions()
              .as(TestBigQueryOptions.class)
              .getBigQueryLocation()
              .equals(BIGQUERY_EARLY_ROLLOUT_REGION)
          ? "big_query_storage_day0"
          : "big_query_storage";
  private static final String TABLE_PREFIX = "storage_read_";

  private BigQueryIOStorageReadOptions options;

  /** Customized {@link TestPipelineOptions} for BigQueryIOStorageRead pipelines. */
  public interface BigQueryIOStorageReadOptions extends TestPipelineOptions, ExperimentalOptions {
    @Description("The table to be read")
    @Validation.Required
    String getInputTable();

    void setInputTable(String table);

    @Description("The expected number of records")
    @Validation.Required
    long getNumRecords();

    void setNumRecords(long numRecords);

    @Description("The data format to use")
    @Validation.Required
    DataFormat getDataFormat();

    void setDataFormat(DataFormat dataFormat);
  }

  private void setUpTestEnvironment(String tableSize, DataFormat format) {
    PipelineOptionsFactory.register(BigQueryIOStorageReadOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOStorageReadOptions.class);
    options.setNumRecords(EXPECTED_NUM_RECORDS.get(tableSize));
    options.setDataFormat(format);
    String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    options.setInputTable(project + ":" + DATASET_ID + "." + TABLE_PREFIX + tableSize);
  }

  private void runBigQueryIOStorageReadPipeline() {
    Pipeline p = Pipeline.create(options);
    PCollection<Long> count =
        p.apply(
                "Read",
                BigQueryIO.read(TableRowParser.INSTANCE)
                    .from(options.getInputTable())
                    .withMethod(Method.DIRECT_READ)
                    .withFormat(options.getDataFormat()))
            .apply("Count", Count.globally());
    PAssert.thatSingleton(count).isEqualTo(options.getNumRecords());
    p.run().waitUntilFinish();
  }

  static class FailingTableRowParser implements SerializableFunction<SchemaAndRecord, TableRow> {

    public static final FailingTableRowParser INSTANCE = new FailingTableRowParser();

    private int parseCount = 0;

    @Override
    public TableRow apply(SchemaAndRecord schemaAndRecord) {
      parseCount++;
      if (parseCount % 50 == 0) {
        throw new RuntimeException("ExpectedException");
      }
      return TableRowParser.INSTANCE.apply(schemaAndRecord);
    }
  }

  private void runBigQueryIOStorageReadPipelineErrorHandling() throws Exception {
    Pipeline p = Pipeline.create(options);
    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorSinkTransform());
    PCollection<Long> count =
        p.apply(
                "Read",
                BigQueryIO.read(FailingTableRowParser.INSTANCE)
                    .from(options.getInputTable())
                    .withMethod(Method.DIRECT_READ)
                    .withFormat(options.getDataFormat())
                    .withErrorHandler(errorHandler))
            .apply("Count", Count.globally());

    errorHandler.close();

    // When 1/50 elements fail sequentially, this is the expected success count
    PAssert.thatSingleton(count).isEqualTo(10381L);
    // this is the total elements, less the successful elements
    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(10592L - 10381L);
    p.run().waitUntilFinish();
  }

  @Test
  public void testBigQueryStorageRead1GAvro() throws Exception {
    setUpTestEnvironment("1G", DataFormat.AVRO);
    runBigQueryIOStorageReadPipeline();
  }

  @Test
  public void testBigQueryStorageRead1GArrow() throws Exception {
    setUpTestEnvironment("1G", DataFormat.ARROW);
    runBigQueryIOStorageReadPipeline();
  }

  @Test
  public void testBigQueryStorageRead1MErrorHandlingAvro() throws Exception {
    setUpTestEnvironment("1M", DataFormat.AVRO);
    runBigQueryIOStorageReadPipelineErrorHandling();
  }

  @Test
  public void testBigQueryStorageRead1MErrorHandlingArrow() throws Exception {
    setUpTestEnvironment("1M", DataFormat.ARROW);
    runBigQueryIOStorageReadPipelineErrorHandling();
  }

  @Test
  public void testBigQueryStorageReadWithAvro() throws Exception {
    storageReadWithSchema(DataFormat.AVRO);
  }

  @Test
  public void testBigQueryStorageReadWithArrow() throws Exception {
    storageReadWithSchema(DataFormat.ARROW);
  }

  private void storageReadWithSchema(DataFormat format) {
    setUpTestEnvironment("multi_field", format);

    Schema multiFieldSchema =
        Schema.builder()
            .addNullableField("string_field", FieldType.STRING)
            .addNullableField("int_field", FieldType.INT64)
            .build();

    Pipeline p = Pipeline.create(options);
    PCollection<Row> tableContents =
        p.apply(
                "Read",
                BigQueryIO.readTableRowsWithSchema()
                    .from(options.getInputTable())
                    .withMethod(Method.DIRECT_READ)
                    .withFormat(options.getDataFormat()))
            .apply(Convert.toRows());
    PAssert.thatSingleton(tableContents.apply(Count.globally())).isEqualTo(options.getNumRecords());
    assertEquals(tableContents.getSchema(), multiFieldSchema);
    p.run().waitUntilFinish();
  }

  /**
   * Tests a pipeline where {@link
   * org.apache.beam.sdk.util.construction.graph.ProjectionPushdownOptimizer} may do optimizations,
   * depending on the runner. The pipeline should run successfully either way.
   */
  @Test
  public void testBigQueryStorageReadProjectionPushdown() throws Exception {
    setUpTestEnvironment("multi_field", DataFormat.AVRO);

    Schema multiFieldSchema =
        Schema.builder()
            .addNullableField("string_field", FieldType.STRING)
            .addNullableField("int_field", FieldType.INT64)
            .build();

    Pipeline p = Pipeline.create(options);
    PCollection<Long> count =
        p.apply(
                "Read",
                BigQueryIO.read(
                        record ->
                            BigQueryUtils.toBeamRow(
                                record.getRecord(),
                                multiFieldSchema,
                                ConversionOptions.builder().build()))
                    .from(options.getInputTable())
                    .withMethod(Method.DIRECT_READ)
                    .withFormat(options.getDataFormat())
                    .withCoder(SchemaCoder.of(multiFieldSchema)))
            .apply(ParDo.of(new GetIntField()))
            .apply("Count", Count.globally());
    PAssert.thatSingleton(count).isEqualTo(options.getNumRecords());
    p.run().waitUntilFinish();
  }

  private static class GetIntField extends DoFn<Row, Long> {
    @SuppressWarnings("unused") // used by reflection
    @FieldAccess("row")
    private final FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("int_field");

    @ProcessElement
    public void processElement(@FieldAccess("row") Row row, OutputReceiver<Long> outputReceiver)
        throws Exception {
      outputReceiver.output(row.getValue("int_field"));
    }
  }
}
