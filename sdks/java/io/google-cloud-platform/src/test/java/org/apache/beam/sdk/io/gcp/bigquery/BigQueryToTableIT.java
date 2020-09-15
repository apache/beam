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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for BigqueryIO with DataflowRunner and DirectRunner. */
@RunWith(JUnit4.class)
public class BigQueryToTableIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToTableIT.class);
  private static String project;

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryToTableIT");

  private static final String BIG_QUERY_DATASET_ID =
      "bq_query_to_table_" + System.currentTimeMillis() + "_" + (new SecureRandom().nextInt(32));

  private static final TableSchema LEGACY_QUERY_TABLE_SCHEMA =
      new TableSchema()
          .setFields(ImmutableList.of(new TableFieldSchema().setName("fruit").setType("STRING")));
  private static final TableSchema NEW_TYPES_QUERY_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("bytes").setType("BYTES"),
                  new TableFieldSchema().setName("date").setType("DATE"),
                  new TableFieldSchema().setName("time").setType("TIME")));
  private static final String NEW_TYPES_QUERY_TABLE_NAME = "types";
  private static final List<Map<String, Object>> NEW_TYPES_QUERY_TABLE_DATA =
      ImmutableList.of(
          ImmutableMap.of("bytes", "abc=", "date", "2000-01-01", "time", "00:00:00"),
          ImmutableMap.of("bytes", "dec=", "date", "3000-12-31", "time", "23:59:59.990000"),
          ImmutableMap.of("bytes", "xyw=", "date", "2011-01-01", "time", "23:59:59.999999"));
  private static final int MAX_RETRY = 5;

  private void runBigQueryToTablePipeline(BigQueryToTableOptions options) {
    Pipeline p = Pipeline.create(options);
    BigQueryIO.Read bigQueryRead = BigQueryIO.read().fromQuery(options.getQuery());
    if (options.getUsingStandardSql()) {
      bigQueryRead = bigQueryRead.usingStandardSql();
    }
    PCollection<TableRow> input = p.apply(bigQueryRead);
    if (options.getReshuffle()) {
      input =
          input
              .apply(WithKeys.<Void, TableRow>of((Void) null))
              .setCoder(KvCoder.of(VoidCoder.of(), TableRowJsonCoder.of()))
              .apply(Reshuffle.<Void, TableRow>of())
              .apply(Values.<TableRow>create());
    }
    input.apply(
        BigQueryIO.writeTableRows()
            .to(options.getOutput())
            .withSchema(options.getOutputSchema())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    p.run().waitUntilFinish();
  }

  private BigQueryToTableOptions setupLegacyQueryTest(String outputTable) {
    BigQueryToTableOptions options =
        TestPipeline.testingPipelineOptions().as(BigQueryToTableOptions.class);
    options.setTempLocation(options.getTempRoot() + "/bq_it_temp");
    options.setQuery("SELECT * FROM (SELECT \"apple\" as fruit), (SELECT \"orange\" as fruit),");
    options.setOutput(outputTable);
    options.setOutputSchema(BigQueryToTableIT.LEGACY_QUERY_TABLE_SCHEMA);
    return options;
  }

  private BigQueryToTableOptions setupNewTypesQueryTest(String outputTable) {
    BigQueryToTableOptions options =
        TestPipeline.testingPipelineOptions().as(BigQueryToTableOptions.class);
    options.setTempLocation(options.getTempRoot() + "/bq_it_temp");
    options.setQuery(
        String.format(
            "SELECT bytes, date, time FROM [%s:%s.%s]",
            project, BIG_QUERY_DATASET_ID, BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_NAME));
    options.setOutput(outputTable);
    options.setOutputSchema(BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_SCHEMA);
    return options;
  }

  private BigQueryToTableOptions setupStandardQueryTest(String outputTable) {
    BigQueryToTableOptions options = this.setupLegacyQueryTest(outputTable);
    options.setTempLocation(options.getTempRoot() + "/bq_it_temp");
    options.setQuery(
        "SELECT * FROM (SELECT \"apple\" as fruit) UNION ALL (SELECT \"orange\" as fruit)");
    options.setUsingStandardSql(true);
    return options;
  }

  private List<TableRow> getTableRowsFromQuery(String query, int maxRetry) throws Exception {
    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT
            .withMaxRetries(maxRetry)
            .withInitialBackoff(Duration.standardSeconds(1L));
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backoff = BackOffAdapter.toGcpBackOff(backoffFactory.backoff());
    do {
      LOG.info("Starting querying {}", query);
      QueryResponse response = BQ_CLIENT.queryWithRetries(query, project);
      if (response.getRows() != null) {
        LOG.info("Got table content with query {}", query);
        return response.getRows();
      }
    } while (BackOffUtils.next(sleeper, backoff));
    LOG.info("Got empty table for query {} with retry {}", query, maxRetry);
    return Collections.emptyList();
  }

  private void verifyLegacyQueryRes(String outputTable) throws Exception {
    List<String> legacyQueryExpectedRes = ImmutableList.of("apple", "orange");
    List<TableRow> tableRows =
        getTableRowsFromQuery(String.format("SELECT fruit from [%s];", outputTable), MAX_RETRY);
    List<String> tableResult =
        tableRows.stream()
            .flatMap(row -> row.getF().stream().map(cell -> cell.getV().toString()))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(legacyQueryExpectedRes, tableResult);
  }

  private void verifyNewTypesQueryRes(String outputTable) throws Exception {
    List<String> newTypeQueryExpectedRes =
        ImmutableList.of(
            "abc=,2000-01-01,00:00:00",
            "dec=,3000-12-31,23:59:59.990000",
            "xyw=,2011-01-01,23:59:59.999999");
    QueryResponse response =
        BQ_CLIENT.queryWithRetries(
            String.format("SELECT bytes, date, time FROM [%s];", outputTable), project);
    List<TableRow> tableRows =
        getTableRowsFromQuery(
            String.format("SELECT bytes, date, time FROM [%s];", outputTable), MAX_RETRY);
    List<String> tableResult =
        tableRows.stream()
            .map(
                row -> {
                  String res = "";
                  for (TableCell cell : row.getF()) {
                    if (res.isEmpty()) {
                      res = cell.getV().toString();
                    } else {
                      res = res + "," + cell.getV().toString();
                    }
                  }
                  return res;
                })
            .sorted()
            .collect(Collectors.toList());
    assertEquals(newTypeQueryExpectedRes, tableResult);
  }

  private void verifyStandardQueryRes(String outputTable) throws Exception {
    this.verifyLegacyQueryRes(outputTable);
  }

  /** Customized PipelineOption for BigQueryToTable Pipeline. */
  public interface BigQueryToTableOptions extends TestPipelineOptions, ExperimentalOptions {

    @Description("The BigQuery query to be used for creating the source")
    @Validation.Required
    String getQuery();

    void setQuery(String query);

    @Description(
        "BigQuery table to write to, specified as "
            + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

    @Description("BigQuery output table schema.")
    @Validation.Required
    TableSchema getOutputSchema();

    void setOutputSchema(TableSchema value);

    @Description("Whether to force reshuffle.")
    @Default.Boolean(false)
    boolean getReshuffle();

    void setReshuffle(boolean reshuffle);

    @Description("Whether to use the Standard SQL dialect when querying BigQuery.")
    @Default.Boolean(false)
    boolean getUsingStandardSql();

    void setUsingStandardSql(boolean usingStandardSql);
  }

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    PipelineOptionsFactory.register(BigQueryToTableOptions.class);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(project, BIG_QUERY_DATASET_ID);

    // Create table and insert data for new type query test cases.
    BQ_CLIENT.createNewTable(
        project,
        BIG_QUERY_DATASET_ID,
        new Table()
            .setSchema(BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_SCHEMA)
            .setTableReference(
                new TableReference()
                    .setTableId(BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_NAME)
                    .setDatasetId(BIG_QUERY_DATASET_ID)
                    .setProjectId(project)));
    BQ_CLIENT.insertDataToTable(
        project,
        BIG_QUERY_DATASET_ID,
        BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_NAME,
        BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_DATA);
  }

  @AfterClass
  public static void cleanup() {
    LOG.info("Start to clean up tables and datasets.");
    BQ_CLIENT.deleteDataset(project, BIG_QUERY_DATASET_ID);
  }

  @Test
  public void testLegacyQueryWithoutReshuffle() throws Exception {
    final String outputTable =
        project + ":" + BIG_QUERY_DATASET_ID + "." + "testLegacyQueryWithoutReshuffle";

    this.runBigQueryToTablePipeline(setupLegacyQueryTest(outputTable));

    this.verifyLegacyQueryRes(outputTable);
  }

  @Test
  public void testNewTypesQueryWithoutReshuffle() throws Exception {
    final String outputTable =
        project + ":" + BIG_QUERY_DATASET_ID + "." + "testNewTypesQueryWithoutReshuffle";

    this.runBigQueryToTablePipeline(setupNewTypesQueryTest(outputTable));

    this.verifyNewTypesQueryRes(outputTable);
  }

  @Test
  public void testNewTypesQueryWithReshuffle() throws Exception {
    final String outputTable =
        project + ":" + BIG_QUERY_DATASET_ID + "." + "testNewTypesQueryWithReshuffle";
    BigQueryToTableOptions options = setupNewTypesQueryTest(outputTable);
    options.setReshuffle(true);

    this.runBigQueryToTablePipeline(options);

    this.verifyNewTypesQueryRes(outputTable);
  }

  @Test
  public void testStandardQueryWithoutCustom() throws Exception {
    final String outputTable =
        project + ":" + BIG_QUERY_DATASET_ID + "." + "testStandardQueryWithoutCustom";

    this.runBigQueryToTablePipeline(setupStandardQueryTest(outputTable));

    this.verifyStandardQueryRes(outputTable);
  }
}
