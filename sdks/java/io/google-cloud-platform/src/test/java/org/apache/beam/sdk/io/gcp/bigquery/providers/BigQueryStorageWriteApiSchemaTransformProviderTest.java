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

import com.google.api.services.bigquery.model.TableRow;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiPCollectionRowTupleTransform;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransformConfiguration;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
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
    BigQueryStorageWriteApiSchemaTransformProvider provider =
        new BigQueryStorageWriteApiSchemaTransformProvider();

    BigQueryStorageWriteApiPCollectionRowTupleTransform writeRowTupleTransform =
        (BigQueryStorageWriteApiPCollectionRowTupleTransform)
            provider.from(config).buildTransform();

    List<Row> testRows =
        Arrays.asList(
            Row.withSchema(SCHEMA)
                .withFieldValue("name", "a")
                .withFieldValue("number", 1L)
                .withFieldValue("dt", LocalDateTime.parse("2000-01-01T00:00:00"))
                .build(),
            Row.withSchema(SCHEMA)
                .withFieldValue("name", "b")
                .withFieldValue("number", 2L)
                .withFieldValue("dt", LocalDateTime.parse("2000-01-02T00:00:00"))
                .build(),
            Row.withSchema(SCHEMA)
                .withFieldValue("name", "c")
                .withFieldValue("number", 3L)
                .withFieldValue("dt", LocalDateTime.parse("2000-01-03T00:00:00"))
                .build());

    writeRowTupleTransform.setBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);

    PCollection<Row> rows = p.apply(Create.of(testRows).withRowSchema(SCHEMA));

    PCollectionRowTuple input = PCollectionRowTuple.of(tag, rows);
    PCollectionRowTuple result = input.apply(writeRowTupleTransform);

    return result;
  }

  @Test
  public void testSimpleWrite() throws Exception {
    String tableSpec = "project:dataset.simple_write";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    runWithConfig(config);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertEquals(3, fakeDatasetService.getAllRows("project", "dataset", "simple_write").size());
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
                        BigQueryStorageWriteApiPCollectionRowTupleTransform.class,
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

  public PCollectionRowTuple runWithError(
      BigQueryStorageWriteApiSchemaTransformConfiguration config) {
    BigQueryStorageWriteApiSchemaTransformProvider provider =
        new BigQueryStorageWriteApiSchemaTransformProvider();

    BigQueryStorageWriteApiPCollectionRowTupleTransform writeRowTupleTransform =
        (BigQueryStorageWriteApiPCollectionRowTupleTransform)
            provider.from(config).buildTransform();

    Function<TableRow, Boolean> shouldFailRow =
        (Function<TableRow, Boolean> & Serializable) tr -> tr.get("name").equals("a");
    fakeDatasetService.setShouldFailRow(shouldFailRow);

    TableRow row1 =
        new TableRow()
            .set("name", "a")
            .set("number", 1L)
            .set("dt", LocalDateTime.parse("2000-01-01T00:00:00"));
    TableRow row2 =
        new TableRow()
            .set("name", "b")
            .set("number", 2L)
            .set("dt", LocalDateTime.parse("2000-01-02T00:00:00"));
    TableRow row3 =
        new TableRow()
            .set("name", "c")
            .set("number", 3L)
            .set("dt", LocalDateTime.parse("2000-01-03T00:00:00"));

    writeRowTupleTransform.setBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);

    PCollection<Row> rows =
        p.apply(Create.of(row1, row2, row3))
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via((tableRow) -> BigQueryUtils.toBeamRow(SCHEMA, tableRow)))
            .setRowSchema(SCHEMA);

    PCollectionRowTuple input = PCollectionRowTuple.of(tag, rows);
    PCollectionRowTuple result = input.apply(writeRowTupleTransform);

    return result;
  }

  @Test
  public void testSimpleWriteWithFailure() throws Exception {
    String tableSpec = "project:dataset.simple_write_with_failure";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    runWithError(config);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertEquals(
        2, fakeDatasetService.getAllRows("project", "dataset", "simple_write_with_failure").size());
  }

  @Test
  public void testErrorCount() throws Exception {
    String tableSpec = "project:dataset.error_count";
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(tableSpec).build();

    runWithError(config);
    PipelineResult result = p.run();

    MetricResults metrics = result.metrics();
    MetricQueryResults metricResults =
        metrics.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(
                    MetricNameFilter.named(
                        BigQueryStorageWriteApiPCollectionRowTupleTransform.class,
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
