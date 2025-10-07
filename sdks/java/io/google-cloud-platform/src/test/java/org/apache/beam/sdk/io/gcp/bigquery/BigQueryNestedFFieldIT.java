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

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.TreeMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for BigQuery Storage API write with nested structures containing 'f' field. This
 * test verifies the fix for IllegalArgumentException when setting a List field to Double in nested
 * TableRow structures, based on the scenario from BigQuerySetFPipeline.java.
 */
@RunWith(JUnit4.class)
public class BigQueryNestedFFieldIT {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryNestedFFieldIT.class);
  private static String project;
  private static final String DATASET_ID =
      "nested_f_field_it_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
  private static final String TABLE_NAME = "nested_f_field_test";

  private static TestBigQueryOptions bqOptions;
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryNestedFFieldIT");

  @BeforeClass
  public static void setup() throws Exception {
    bqOptions = TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class);
    project = bqOptions.as(GcpOptions.class).getProject();
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(project, DATASET_ID, null, bqOptions.getBigQueryLocation());
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(project, DATASET_ID);
  }

  /**
   * Test case that reproduces the scenario from BigQuerySetFPipeline.java where a nested structure
   * contains an 'f' field with a float value. This tests the fix for the IllegalArgumentException
   * that occurred when TableRowToStorageApiProto tried to set a List field to a Double value.
   */
  @Test
  public void testNestedFFieldWithFloat() throws IOException, InterruptedException {
    // Define the table schema with nested structure containing 'f' field
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("bytes").setType("BYTES"),
                    new TableFieldSchema()
                        .setName("sub")
                        .setType("RECORD")
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("a").setType("STRING"),
                                new TableFieldSchema().setName("c").setType("INTEGER"),
                                new TableFieldSchema().setName("f").setType("FLOAT")))));

    String tableSpec = String.format("%s:%s.%s", project, DATASET_ID, TABLE_NAME);

    // Set up pipeline options for Storage API
    bqOptions.setUseStorageWriteApi(true);
    bqOptions.setUseStorageWriteApiAtLeastOnce(true);

    Pipeline pipeline = Pipeline.create(bqOptions);

    // Create test data similar to BigQuerySetFPipeline.java
    pipeline
        .apply("CreateInput", Create.of("test"))
        .apply("GenerateTestData", ParDo.of(new GenerateTestDataFn()))
        .apply("CreateTableRows", MapElements.via(new CreateTableRowFn()))
        .apply(
            "WriteToBigQuery",
            BigQueryIO.writeTableRows()
                .to(tableSpec)
                .withSchema(schema)
                .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    // Run the pipeline
    pipeline.run().waitUntilFinish();

    // Verify the data was written correctly
    String testQuery =
        String.format("SELECT sub.a, sub.c, sub.f FROM [%s.%s];", DATASET_ID, TABLE_NAME);

    QueryResponse response = BQ_CLIENT.queryWithRetries(testQuery, project);
    assertEquals("Expected exactly one row", 1, response.getRows().size());

    // Verify the nested 'f' field value
    TableRow resultRow = response.getRows().get(0);
    assertEquals("hello", resultRow.getF().get(0).getV()); // sub.a
    assertEquals("3", resultRow.getF().get(1).getV()); // sub.c
    assertEquals("1.2", resultRow.getF().get(2).getV()); // sub.f

    LOG.info(
        "Successfully wrote and verified nested structure with 'f' field containing float value");
  }

  /** Test case that verifies multiple nested structures with 'f' fields work correctly. */
  @Test
  public void testMultipleNestedFFields() throws IOException, InterruptedException {
    String tableName = TABLE_NAME + "_multiple";

    // Define schema with multiple nested structures containing 'f' fields
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("id").setType("INTEGER"),
                    new TableFieldSchema()
                        .setName("nested1")
                        .setType("RECORD")
                        .setFields(
                            ImmutableList.of(new TableFieldSchema().setName("f").setType("FLOAT"))),
                    new TableFieldSchema()
                        .setName("nested2")
                        .setType("RECORD")
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("f").setType("FLOAT")))));

    String tableSpec = String.format("%s:%s.%s", project, DATASET_ID, tableName);

    Pipeline pipeline = Pipeline.create(bqOptions);

    pipeline
        .apply("CreateMultipleInput", Create.of(1, 2, 3))
        .apply("CreateMultipleTableRows", MapElements.via(new CreateMultipleTableRowFn()))
        .apply(
            "WriteMultipleToBigQuery",
            BigQueryIO.writeTableRows()
                .to(tableSpec)
                .withSchema(schema)
                .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    pipeline.run().waitUntilFinish();

    // Verify multiple rows were written
    String countQuery = String.format("SELECT COUNT(*) FROM [%s.%s];", DATASET_ID, tableName);
    QueryResponse countResponse = BQ_CLIENT.queryWithRetries(countQuery, project);
    assertEquals("3", countResponse.getRows().get(0).getF().get(0).getV());

    LOG.info("Successfully wrote and verified multiple nested structures with 'f' fields");
  }

  /** Static DoFn for generating test data to avoid serialization issues. */
  private static class GenerateTestDataFn extends DoFn<String, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(1000); // Small byte array size for test
    }
  }

  /** Static SimpleFunction for creating TableRows to avoid serialization issues. */
  private static class CreateTableRowFn extends SimpleFunction<Integer, TableRow> {
    @Override
    public TableRow apply(Integer bytesSize) {
      // Create nested structure with 'f' field containing float value
      // This reproduces the exact scenario from BigQuerySetFPipeline.java
      ImmutableMap<String, Object> data =
          ImmutableMap.of(
              "bytes",
              new byte[bytesSize],
              "sub",
              new TreeMap<>(ImmutableMap.of("a", "hello", "c", 3, "f", 1.2f)));

      TableRow row = new TableRow();
      row.putAll(new TreeMap<>(data));
      return row;
    }
  }

  /** Static SimpleFunction for creating multiple TableRows to avoid serialization issues. */
  private static class CreateMultipleTableRowFn extends SimpleFunction<Integer, TableRow> {
    @Override
    public TableRow apply(Integer id) {
      return new TableRow()
          .set("id", id)
          .set("nested1", new TreeMap<>(ImmutableMap.of("f", id * 1.1f)))
          .set("nested2", new TreeMap<>(ImmutableMap.of("f", id * 2.2f)));
    }
  }
}
