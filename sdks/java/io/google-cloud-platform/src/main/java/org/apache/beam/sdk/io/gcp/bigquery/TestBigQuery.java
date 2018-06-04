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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Test rule which creates a new table with specified schema,
 * with randomized name and exposes few APIs to work with it.
 *
 * <p>Uses a {@link TestPipeline} to load the data from BigQuery for assertions.
 *
 * <p>Deletes the table on test shutdown.
 */
public class TestBigQuery implements TestRule {
  private static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("YYYY_MM_dd_HH_mm_ss_SSS");

  private TestBigQueryOptions pipelineOptions;
  private Schema schema;
  private TestPipeline resultsPipelineRule;
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
        tableSchema,
        TestPipeline.create());
  }

  private TestBigQuery(
      TestBigQueryOptions pipelineOptions,
      Schema tableSchema,
      TestPipeline resultsPipelineRule) {
    this.pipelineOptions = pipelineOptions;
    this.schema = tableSchema;
    this.resultsPipelineRule = resultsPipelineRule;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return resultsPipelineRule.apply(testBigQuery(base, description), description);
  }

  private Statement testBigQuery(Statement base, Description description) {
    return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          if (TestBigQuery.this.datasetService != null) {
            throw new AssertionError(
                "BigQuery test was not shutdown previously. "
                + "Table is'" + table + "'. "
                + "Current test: " + description.getDisplayName());
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
            .setDescription("Table created for "
                            + description.getDisplayName() + " by TestBigQueryRule. "
                            + "Should be automatically cleaned up after test completion.");

    if (datasetService.getTable(tableReference) != null) {
      throw new IllegalStateException("Table '" + tableReference + "' already exists. "
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
        topicName
            .append(Class.forName(description.getClassName()).getSimpleName())
            .append("_");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    if (description.getMethodName() != null) {
      topicName.append(description.getMethodName()).append("_");
    }

    DATETIME_FORMAT.printTo(topicName, Instant.now());

    return topicName.toString() + "_"
           + String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong()));
  }

  public String tableSpec() {
    return String.format(
        "%s:%s.%s",
        table.getTableReference().getProjectId(),
        table.getTableReference().getDatasetId(),
        table.getTableReference().getTableId());
  }

  public void assertContainsInAnyOrder(TableRow ... tableRows) {
    PAssert
        .that(readAllRowsFromBQ(resultsPipelineRule, tableSpec()))
        .containsInAnyOrder(Arrays.asList(tableRows));
    resultsPipelineRule.run().waitUntilFinish(Duration.standardMinutes(5));
  }

  private PCollection<TableRow> readAllRowsFromBQ(Pipeline pipeline, String tableSpec) {
    return pipeline
        .apply(
            BigQueryIO
                .readTableRows()
                .from(tableSpec));
  }
}
