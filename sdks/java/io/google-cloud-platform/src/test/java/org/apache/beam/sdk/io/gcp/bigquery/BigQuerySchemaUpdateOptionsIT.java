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
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for BigqueryIO with DataflowRunner and DirectRunner. */
@RunWith(JUnit4.class)
public class BigQuerySchemaUpdateOptionsIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySchemaUpdateOptionsIT.class);
  private static String project;

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("BigQuerySchemaUpdateOptionsIT");

  private static final String BIG_QUERY_DATASET_ID =
      "bq_query_schema_update_options_"
          + System.currentTimeMillis()
          + "_"
          + (new SecureRandom().nextInt(32));

  private static final String TEST_TABLE_NAME_BASE = "test_table_";

  private static final TableSchema BASE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("optional_field").setType("STRING"),
                  new TableFieldSchema()
                      .setName("required_field")
                      .setType("STRING")
                      .setMode("REQUIRED")));

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    BQ_CLIENT.createNewDataset(project, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    LOG.info("Start to clean up tables and datasets.");
    BQ_CLIENT.deleteDataset(project, BIG_QUERY_DATASET_ID);
  }

  public interface Options extends TestPipelineOptions, BigQueryOptions {}

  /**
   * Make a new table for use in a test.
   *
   * @return The name of the table
   * @throws Exception if anything goes awry
   */
  public String makeTestTable() throws Exception {
    String tableName =
        TEST_TABLE_NAME_BASE + System.currentTimeMillis() + "_" + (new SecureRandom().nextInt(32));

    BQ_CLIENT.createNewTable(
        project,
        BIG_QUERY_DATASET_ID,
        new Table()
            .setSchema(BASE_TABLE_SCHEMA)
            .setTableReference(
                new TableReference()
                    .setTableId(tableName)
                    .setDatasetId(BIG_QUERY_DATASET_ID)
                    .setProjectId(project)));

    return tableName;
  }

  /**
   * Runs a write test against a BigQuery table to check that SchemaUpdateOption sets are taking
   * effect.
   *
   * <p>Attempt write a row via BigQueryIO.writeTables with the given params, then run the given
   * query, and finaly check the results of the query.
   *
   * @param schemaUpdateOptions The SchemaUpdateOption set to use
   * @param tableName The table to write to
   * @param schema The schema to use for the table
   * @param rowToInsert The row to insert
   * @param testQuery A testing SQL query to run after writing the row
   * @param expectedResult The expected result of the query as a nested list of column values (one
   *     list per result row)
   */
  private void runWriteTest(
      Set<SchemaUpdateOption> schemaUpdateOptions,
      String tableName,
      TableSchema schema,
      TableRow rowToInsert,
      String testQuery,
      List<List<String>> expectedResult)
      throws Exception {
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    options.setTempLocation(options.getTempRoot() + "/bq_it_temp");

    Pipeline p = Pipeline.create(options);
    Create.Values<TableRow> input = Create.<TableRow>of(rowToInsert);

    Write<TableRow> writer =
        BigQueryIO.writeTableRows()
            .to(String.format("%s:%s.%s", options.getProject(), BIG_QUERY_DATASET_ID, tableName))
            .withSchema(schema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchemaUpdateOptions(schemaUpdateOptions);

    p.apply(input).apply(writer);
    p.run().waitUntilFinish();

    QueryResponse response = BQ_CLIENT.queryWithRetries(testQuery, project);

    List<List<String>> result =
        response.getRows().stream()
            .map(
                row ->
                    row.getF().stream()
                        .map(cell -> cell.getV().toString())
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());

    assertEquals(expectedResult, result);
  }

  @Test
  public void testAllowFieldAddition() throws Exception {
    String tableName = makeTestTable();

    Set<SchemaUpdateOption> schemaUpdateOptions =
        EnumSet.of(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION);

    TableSchema newSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("new_field").setType("STRING"),
                    new TableFieldSchema().setName("optional_field").setType("STRING"),
                    new TableFieldSchema()
                        .setName("required_field")
                        .setType("STRING")
                        .setMode("REQUIRED")));

    String[] values = {"meow", "bark"};
    TableRow rowToInsert =
        new TableRow().set("new_field", values[0]).set("required_field", values[1]);

    String testQuery =
        String.format(
            "SELECT new_field, required_field FROM [%s.%s];", BIG_QUERY_DATASET_ID, tableName);

    List<List<String>> expectedResult = Arrays.asList(Arrays.asList(values));
    runWriteTest(schemaUpdateOptions, tableName, newSchema, rowToInsert, testQuery, expectedResult);
  }

  @Test
  public void testAllowFieldRelaxation() throws Exception {
    String tableName = makeTestTable();

    Set<SchemaUpdateOption> schemaUpdateOptions =
        EnumSet.of(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);

    TableSchema newSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("optional_field").setType("STRING")));

    String value = "hellooo";
    TableRow rowToInsert = new TableRow().set("optional_field", value);

    String testQuery =
        String.format("SELECT optional_field FROM [%s.%s];", BIG_QUERY_DATASET_ID, tableName);

    List<List<String>> expectedResult = Arrays.asList(Arrays.asList(value));
    runWriteTest(schemaUpdateOptions, tableName, newSchema, rowToInsert, testQuery, expectedResult);
  }
}
