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
import java.io.IOException;
import java.security.SecureRandom;
import java.util.TreeMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
    WriteResult result =
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

    // Validate failed inserts using PAssert
    PCollection<BigQueryStorageApiInsertError> failedInserts = result.getFailedStorageApiInserts();

    // Assert that we expect exactly 3 failed inserts (entire batch fails when one row exceeds size
    // limit)
    // The test intentionally creates a batch with one row that exceeds BigQuery's size limit
    PAssert.that(failedInserts)
        .satisfies(
            (Iterable<BigQueryStorageApiInsertError> errors) -> {
              int count = 0;
              for (BigQueryStorageApiInsertError error : errors) {
                count++;
                if (!error.getErrorMessage().contains("Row payload too large")) {
                  throw new AssertionError(
                      "Expected 'Row payload too large' error, got: " + error.getErrorMessage());
                }
              }
              if (count != 3) {
                throw new AssertionError("Expected exactly 3 failed inserts, got: " + count);
              }
              return null;
            });

    // Run the pipeline
    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    // Check if the BigQuery table exists and has any rows
    String testQuery =
        String.format("SELECT sub.a, sub.c, sub.f FROM [%s.%s];", DATASET_ID, TABLE_NAME);

    try {
      QueryResponse response = BQ_CLIENT.queryWithRetries(testQuery, project);

      if (response.getRows() != null && response.getRows().size() > 0) {
        LOG.info("Found {} successful inserts in BigQuery table", response.getRows().size());

        // Verify the nested 'f' field value for all rows
        for (int i = 0; i < response.getRows().size(); i++) {
          TableRow resultRow = response.getRows().get(i);
          assertEquals("hello", resultRow.getF().get(0).getV()); // sub.a
          assertEquals("3", resultRow.getF().get(1).getV()); // sub.c
          assertEquals("1.2", resultRow.getF().get(2).getV()); // sub.f
        }

        LOG.info(
            "Successfully wrote and verified nested structure with 'f' field containing float value. "
                + "Verified {} rows",
            response.getRows().size());
      } else {
        LOG.info("No successful inserts found in BigQuery table - all rows may have failed");
      }
    } catch (Exception e) {
      LOG.info("BigQuery table query failed (table may not exist)", e);
      LOG.info("This suggests all inserts failed or the pipeline encountered an error");
    }
  }

  private static class GenerateTestDataFn extends DoFn<String, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(1_000); // Small byte array size for test
      c.output(1_000_000); // Medium byte array size for test
      c.output(10_000_000); // Large byte array size for test - this row will not be added
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
}
