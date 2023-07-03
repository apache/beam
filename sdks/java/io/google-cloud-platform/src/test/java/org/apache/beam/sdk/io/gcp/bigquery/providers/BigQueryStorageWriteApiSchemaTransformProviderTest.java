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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransform;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransformConfiguration;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryStorageWriteApiSchemaTransformProviderTest {

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  private static final Schema SCHEMA =
      Schema.of(
          Field.of("name", FieldType.STRING),
          Field.of("number", FieldType.INT64),
          Field.of("dt", FieldType.logicalType(SqlTypes.DATETIME)));

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(SCHEMA)
              .withFieldValue("name", "a")
              .withFieldValue("number", 1L)
              .withFieldValue("dt", LocalDateTime.parse("2000-01-01T00:00:00"))
              .build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("name", "b")
              .withFieldValue("number", 2L)
              .withFieldValue("dt", LocalDateTime.parse("2000-01-02T00:00:00.123"))
              .build(),
          Row.withSchema(SCHEMA)
              .withFieldValue("name", "c")
              .withFieldValue("number", 3L)
              .withFieldValue("dt", LocalDateTime.parse("2000-01-03T00:00:00.123456"))
              .build());

  private static final Schema SCHEMA_WRONG =
      Schema.of(
          Field.of("name_wrong", FieldType.STRING),
          Field.of("number", FieldType.INT64),
          Field.of("dt", FieldType.logicalType(SqlTypes.DATETIME)));
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();

    fakeDatasetService.createDataset("project", "dataset", "", "", null);
  }

  @Test
  public void testInvalidConfig() {
    List<BigQueryStorageWriteApiSchemaTransformConfiguration.Builder> invalidConfigs =
        Arrays.asList(
            BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                .setTable("not_a_valid_table_spec"),
            BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                .setTable("project:dataset.table")
                .setCreateDisposition("INVALID_DISPOSITION"));

    for (BigQueryStorageWriteApiSchemaTransformConfiguration.Builder config : invalidConfigs) {
      assertThrows(
          Exception.class,
          () -> {
            config.build().validate();
          });
    }
  }

  public PCollectionRowTuple runWithConfig(
      BigQueryStorageWriteApiSchemaTransformConfiguration config) {
    return runWithConfig(config, ROWS);
  }

  public PCollectionRowTuple runWithConfig(
      BigQueryStorageWriteApiSchemaTransformConfiguration config, List<Row> inputRows) {
    BigQueryStorageWriteApiSchemaTransformProvider provider =
        new BigQueryStorageWriteApiSchemaTransformProvider();

    BigQueryStorageWriteApiSchemaTransform writeTransform =
        (BigQueryStorageWriteApiSchemaTransform) provider.from(config);

    writeTransform.setBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);

    PCollection<Row> rows = p.apply(Create.of(inputRows).withRowSchema(SCHEMA));

    PCollectionRowTuple input = PCollectionRowTuple.of(tag, rows);
    PCollectionRowTuple result = input.apply(writeTransform);

    return result;
  }

  public Boolean rowsEquals(List<Row> expectedRows, List<TableRow> actualRows) {
    if (expectedRows.size() != actualRows.size()) {
      return false;
    }
    for (int i = 0; i < expectedRows.size(); i++) {
      // Actual rows may come back out of order. For each TableRow, find its "number" column value
      // and match it to the index of the expected row.
      TableRow actualRow = actualRows.get(i);
      Row expectedRow = expectedRows.get(Integer.parseInt(actualRow.get("number").toString()) - 1);

      if (!expectedRow.getValue("name").equals(actualRow.get("name"))
          || !expectedRow
              .getValue("number")
              .equals(Long.parseLong(actualRow.get("number").toString()))) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testSimpleWrite() throws Exception {
    String tableSpec = "project:dataset.simple_write";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    runWithConfig(config, ROWS);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertTrue(
        rowsEquals(ROWS, fakeDatasetService.getAllRows("project", "dataset", "simple_write")));
  }

  @Test
  public void testSchemaValidationSuccess() throws Exception {
    String tableSpec = "project:dataset.schema_validation_success";
    Table table = new Table();
    TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
    table.setTableReference(tableReference);
    table.setSchema(BigQueryUtils.toTableSchema(SCHEMA));
    fakeDatasetService.createTable(table);
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setTable(tableSpec)
            .setCreateDisposition("CREATE_IF_NEEDED")
            .build();

    runWithConfig(config);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertEquals(
        3, fakeDatasetService.getAllRows("project", "dataset", "schema_validation_success").size());
  }

  @Test(expected = RuntimeException.class)
  public void testSchemaValidationFail() throws Exception {
    String tableSpec = "project:dataset.schema_validation_fail";
    Table table = new Table();
    TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
    table.setTableReference(tableReference);
    table.setSchema(BigQueryUtils.toTableSchema(SCHEMA_WRONG));
    fakeDatasetService.createTable(table);
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setTable(tableSpec)
            .setCreateDisposition("CREATE_IF_NEEDED")
            .build();
    BigQueryStorageWriteApiSchemaTransformProvider provider =
        new BigQueryStorageWriteApiSchemaTransformProvider();

    BigQueryStorageWriteApiSchemaTransform writeTransform =
        (BigQueryStorageWriteApiSchemaTransform) provider.from(config);
    writeTransform.setBigQueryServices(fakeBigQueryServices);
    List<Row> testRows =
        Arrays.asList(
            Row.withSchema(SCHEMA)
                .withFieldValue("name", "a")
                .withFieldValue("number", 1L)
                .withFieldValue("dt", LocalDateTime.parse("2000-01-01T00:00:00"))
                .build());
    String tag = provider.inputCollectionNames().get(0);
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Row> rows = pipeline.apply(Create.of(testRows).withRowSchema(SCHEMA));
    PCollectionRowTuple input = PCollectionRowTuple.of(tag, rows);
    writeTransform.expand(input);
  }

  @Test
  public void testInputElementCount() throws Exception {
    String tableSpec = "project:dataset.input_count";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    runWithConfig(config);
    PipelineResult result = p.run();

    MetricResults metrics = result.metrics();
    MetricQueryResults metricResults =
        metrics.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(
                    MetricNameFilter.named(
                        BigQueryStorageWriteApiSchemaTransform.class,
                        "BigQuery-write-element-counter"))
                .build());

    Iterable<MetricResult<Long>> counters = metricResults.getCounters();
    if (!counters.iterator().hasNext()) {
      throw new RuntimeException("no counters available for the input element count");
    }

    Long expectedCount = 3L;
    for (MetricResult<Long> count : counters) {
      assertEquals(expectedCount, count.getAttempted());
    }
  }

  @Test
  public void testFailedRows() throws Exception {
    String tableSpec = "project:dataset.write_with_fail";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    String failValue = "fail_me";

    List<Row> expectedSuccessfulRows = new ArrayList<>(ROWS);
    List<Row> expectedFailedRows = new ArrayList<>();
    for (long l = 1L; l <= 3L; l++) {
      expectedFailedRows.add(
          Row.withSchema(SCHEMA)
              .withFieldValue("name", failValue)
              .withFieldValue("number", l)
              .withFieldValue("dt", LocalDateTime.parse("2020-01-01T00:00:00.09"))
              .build());
    }

    List<Row> totalRows = new ArrayList<>(expectedSuccessfulRows);
    totalRows.addAll(expectedFailedRows);

    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable) tr -> tr.get("name").equals(failValue);
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    PCollectionRowTuple result = runWithConfig(config, totalRows);
    PCollection<Row> failedRows = result.get("FailedRows");

    PAssert.that(failedRows).containsInAnyOrder(expectedFailedRows);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertTrue(
        rowsEquals(
            expectedSuccessfulRows,
            fakeDatasetService.getAllRows("project", "dataset", "write_with_fail")));
  }

  @Test
  public void testErrorCount() throws Exception {
    String tableSpec = "project:dataset.error_count";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable) tr -> tr.get("name").equals("a");
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    runWithConfig(config);
    PipelineResult result = p.run();

    MetricResults metrics = result.metrics();
    MetricQueryResults metricResults =
        metrics.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(
                    MetricNameFilter.named(
                        BigQueryStorageWriteApiSchemaTransform.class,
                        "BigQuery-write-error-counter"))
                .build());

    Iterable<MetricResult<Long>> counters = metricResults.getCounters();
    if (!counters.iterator().hasNext()) {
      throw new RuntimeException("no counters available ");
    }

    Long expectedCount = 1L;
    for (MetricResult<Long> count : counters) {
      assertEquals(expectedCount, count.getAttempted());
    }
  }
}
