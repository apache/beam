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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toBeamRow;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Test rule which creates a new table with specified schema, with randomized name and exposes few
 * APIs to work with it.
 *
 * <p>Deletes the table on test shutdown.
 */
public class TestBigQuery implements TestRule {
  private static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("YYYY_MM_dd_HH_mm_ss_SSS");

  private TestBigQueryOptions pipelineOptions;
  private Schema schema;
  private Table table;
  private BigQueryServices.DatasetService datasetService;

  /**
   * Creates an instance of this rule.
   *
   * <p>Loads GCP configuration from {@link TestPipelineOptions}.
   */
  public static TestBigQuery create(Schema tableSchema) {
    return new TestBigQuery(
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class),
        tableSchema);
  }

  private TestBigQuery(
      TestBigQueryOptions pipelineOptions, Schema tableSchema) {
    this.pipelineOptions = pipelineOptions;
    this.schema = tableSchema;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (TestBigQuery.this.datasetService != null) {
          throw new AssertionError(
              "BigQuery test was not shutdown previously. "
                  + "Table is'"
                  + table
                  + "'. "
                  + "Current test: "
                  + description.getDisplayName());
        }

        try {
          initializeBigQuery(description);
          base.evaluate();
        } finally {
          tearDown();
        }
      }
    };
  }

  private void initializeBigQuery(Description description)
      throws IOException, InterruptedException {

    this.datasetService = new BigQueryServicesImpl().getDatasetService(pipelineOptions);
    this.table = createTable(description);
  }

  private Table createTable(Description description) throws IOException, InterruptedException {
    TableReference tableReference =
        new TableReference()
            .setProjectId(pipelineOptions.getProject())
            .setDatasetId(pipelineOptions.getTargetDataset())
            .setTableId(createRandomizedName(description));

    table =
        new Table()
            .setTableReference(tableReference)
            .setSchema(BigQueryUtils.toTableSchema(schema))
            .setDescription(
                "Table created for "
                    + description.getDisplayName()
                    + " by TestBigQueryRule. "
                    + "Should be automatically cleaned up after test completion.");

    if (datasetService.getTable(tableReference) != null) {
      throw new IllegalStateException(
          "Table '"
              + tableReference
              + "' already exists. "
              + "It should have been cleaned up by the test rule.");
    }

    datasetService.createTable(table);
    return table;
  }

  private void tearDown() throws IOException, InterruptedException {
    if (this.datasetService == null) {
      return;
    }

    try {
      if (table != null) {
        datasetService.deleteTable(table.getTableReference());
      }
    } finally {
      datasetService = null;
      table = null;
    }
  }

  static String createRandomizedName(Description description) throws IOException {
    StringBuilder topicName = new StringBuilder();

    if (description.getClassName() != null) {
      try {
        topicName.append(Class.forName(description.getClassName()).getSimpleName()).append("_");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    if (description.getMethodName() != null) {
      topicName.append(description.getMethodName()).append("_");
    }

    DATETIME_FORMAT.printTo(topicName, Instant.now());

    return topicName.toString()
        + "_"
        + String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong()));
  }

  public String tableSpec() {
    return String.format(
        "%s:%s.%s",
        table.getTableReference().getProjectId(),
        table.getTableReference().getDatasetId(),
        table.getTableReference().getTableId());
  }

  /**
   * Loads rows from BigQuery into {@link Row Rows} with given {@link Schema}.
   *
   * <p>Current implementation only supports flat {@link Row Rows} and target {@link Schema Schemas}
   * with {@link FieldType#STRING} fields only.
   */
  public List<Row> getFlatJsonRows(Schema rowSchema) throws IOException {
    Bigquery bq = newBigQueryClient(pipelineOptions);
    return bqRowsToBeamRows(getSchema(bq), getTableRows(bq), rowSchema);
  }

  private List<Row> bqRowsToBeamRows(
      TableSchema bqSchema, List<TableRow> bqRows, Schema rowSchema) {
    return bqRows
        .stream()
        .map(bqRow -> toBeamRow(rowSchema, bqSchema, bqRow))
        .collect(Collectors.toList());
  }

  private TableSchema getSchema(Bigquery bq) throws IOException {
    return bq.tables()
        .get(
            pipelineOptions.getProject(),
            pipelineOptions.getTargetDataset(),
            table.getTableReference().getTableId())
        .execute()
        .getSchema();
  }

  private List<TableRow> getTableRows(Bigquery bq) throws IOException {
    return bq.tabledata()
        .list(
            pipelineOptions.getProject(),
            pipelineOptions.getTargetDataset(),
            table.getTableReference().getTableId())
        .execute()
        .getRows();
  }

  private static Bigquery newBigQueryClient(BigQueryOptions options) {
    return new Bigquery.Builder(
            Transport.getTransport(),
            Transport.getJsonFactory(),
            chainHttpRequestInitializer(
                options.getGcpCredential(),
                // Do not log 404. It clutters the output and is possibly even required by the
                // caller.
                new RetryHttpRequestInitializer(ImmutableList.of(404))))
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace())
        .build();
  }

  private static HttpRequestInitializer chainHttpRequestInitializer(
      Credentials credential, HttpRequestInitializer httpRequestInitializer) {
    if (credential == null) {
      return new ChainingHttpRequestInitializer(
          new NullCredentialInitializer(), httpRequestInitializer);
    } else {
      return new ChainingHttpRequestInitializer(
          new HttpCredentialsAdapter(credential), httpRequestInitializer);
    }
  }
}
