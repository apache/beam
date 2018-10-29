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
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.DataflowPortabilityApiUnsupported;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for BigqueryIO with DataflowRunner and DirectRunner. */
@RunWith(JUnit4.class)
public class BigQueryToTableIT {

  private BigQueryToTableOptions options;
  private String project;

  private String bigQueryDatasetId;
  private static final String OUTPUT_TABLE_NAME = "output_table";
  private BigQueryOptions bqOption;
  private String outputTable;
  private BigqueryClient bqClient;
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

  private void runBigQueryToTablePipeline() {
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

  private void setupLegacyQueryTest() {
    options.setQuery("SELECT * FROM (SELECT \"apple\" as fruit), (SELECT \"orange\" as fruit),");
    options.setOutput(outputTable);
    options.setOutputSchema(BigQueryToTableIT.LEGACY_QUERY_TABLE_SCHEMA);
  }

  private void setupNewTypesQueryTest() {
    this.bqClient.createNewTable(
        this.project,
        this.bigQueryDatasetId,
        new Table()
            .setSchema(BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_SCHEMA)
            .setTableReference(
                new TableReference()
                    .setTableId(BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_NAME)
                    .setDatasetId(this.bigQueryDatasetId)
                    .setProjectId(this.project)));
    this.bqClient.insertDataToTable(
        this.project,
        this.bigQueryDatasetId,
        BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_NAME,
        BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_DATA);
    this.options.setQuery(
        String.format(
            "SELECT bytes, date, time FROM [%s:%s.%s]",
            project, this.bigQueryDatasetId, BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_NAME));
    this.options.setOutput(outputTable);
    this.options.setOutputSchema(BigQueryToTableIT.NEW_TYPES_QUERY_TABLE_SCHEMA);
  }

  private void setupStandardQueryTest() {
    this.setupLegacyQueryTest();
    this.options.setQuery(
        "SELECT * FROM (SELECT \"apple\" as fruit) UNION ALL (SELECT \"orange\" as fruit)");
    this.options.setUsingStandardSql(true);
  }

  private void verifyLegacyQueryRes() throws Exception {
    List<String> legacyQueryExpectedRes = ImmutableList.of("apple", "orange");
    QueryResponse response =
        bqClient.queryWithRetries(String.format("SELECT fruit from [%s];", outputTable), project);
    List<String> tableResult =
        response
            .getRows()
            .stream()
            .flatMap(row -> row.getF().stream().map(cell -> cell.getV().toString()))
            .sorted()
            .collect(Collectors.toList());

    assertEquals(legacyQueryExpectedRes, tableResult);
  }

  private void verifyNewTypesQueryRes() throws Exception {
    List<String> newTypeQueryExpectedRes =
        ImmutableList.of(
            "abc=,2000-01-01,00:00:00",
            "dec=,3000-12-31,23:59:59.990000",
            "xyw=,2011-01-01,23:59:59.999999");
    QueryResponse response =
        bqClient.queryWithRetries(
            String.format("SELECT bytes, date, time FROM [%s];", this.outputTable), this.project);
    List<String> tableResult =
        response
            .getRows()
            .stream()
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

  private void verifyStandardQueryRes() throws Exception {
    this.verifyLegacyQueryRes();
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

  @Before
  public void setupBqEnvironment() {
    Long timeSeed = System.currentTimeMillis();
    Integer random = new Random(timeSeed).nextInt(900) + 100;
    this.bigQueryDatasetId = "bq_query_to_table_" + timeSeed.toString() + "_" + random.toString();
    PipelineOptionsFactory.register(BigQueryToTableOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryToTableOptions.class);
    options.setTempLocation(options.getTempRoot() + "/bq_it_temp");
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    bqOption = options.as(BigQueryOptions.class);
    bqClient = new BigqueryClient(bqOption.getAppName());
    bqClient.createNewDataset(project, this.bigQueryDatasetId);
    outputTable =
        project + ":" + this.bigQueryDatasetId + "." + BigQueryToTableIT.OUTPUT_TABLE_NAME;
  }

  @After
  public void cleanBqEnvironment() {
    bqClient.deleteDataset(project, this.bigQueryDatasetId);
  }

  @Test
  public void testLegacyQueryWithoutReshuffle() throws Exception {
    this.setupLegacyQueryTest();

    this.runBigQueryToTablePipeline();

    this.verifyLegacyQueryRes();
  }

  @Test
  public void testNewTypesQueryWithoutReshuffle() throws Exception {
    this.setupNewTypesQueryTest();

    this.runBigQueryToTablePipeline();

    this.verifyNewTypesQueryRes();
  }

  @Test
  public void testNewTypesQueryWithReshuffle() throws Exception {
    this.setupNewTypesQueryTest();
    this.options.setReshuffle(true);

    this.runBigQueryToTablePipeline();

    this.verifyNewTypesQueryRes();
  }

  @Test
  public void testStandardQueryWithoutCustom() throws Exception {
    this.setupStandardQueryTest();

    this.runBigQueryToTablePipeline();

    this.verifyStandardQueryRes();
  }

  @Test
  @Category(DataflowPortabilityApiUnsupported.class)
  public void testNewTypesQueryWithoutReshuffleWithCustom() throws Exception {
    this.setupNewTypesQueryTest();
    this.options.setExperiments(
        ImmutableList.of("enable_custom_bigquery_sink", "enable_custom_bigquery_source"));

    this.runBigQueryToTablePipeline();

    this.verifyNewTypesQueryRes();
  }

  @Test
  @Category(DataflowPortabilityApiUnsupported.class)
  public void testLegacyQueryWithoutReshuffleWithCustom() throws Exception {
    this.setupLegacyQueryTest();
    this.options.setExperiments(
        ImmutableList.of("enable_custom_bigquery_sink", "enable_custom_bigquery_source"));

    this.runBigQueryToTablePipeline();

    this.verifyLegacyQueryRes();
  }

  @Test
  @Category(DataflowPortabilityApiUnsupported.class)
  public void testStandardQueryWithoutReshuffleWithCustom() throws Exception {
    this.setupStandardQueryTest();
    this.options.setExperiments(
        ImmutableList.of("enable_custom_bigquery_sink", "enable_custom_bigquery_source"));

    this.runBigQueryToTablePipeline();

    this.verifyStandardQueryRes();
  }
}
