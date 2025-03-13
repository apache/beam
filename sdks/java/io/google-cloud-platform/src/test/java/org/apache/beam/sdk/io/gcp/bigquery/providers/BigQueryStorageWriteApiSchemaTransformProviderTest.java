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

import static org.apache.beam.sdk.io.gcp.bigquery.providers.PortableBigQueryDestinations.DESTINATION;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.PortableBigQueryDestinations.RECORD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableRow;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider.BigQueryStorageWriteApiSchemaTransform;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();

    fakeDatasetService.createDataset("project", "dataset", "", "", null);
  }

  @Test
  public void testInvalidConfig() {
    List<BigQueryWriteConfiguration.Builder> invalidConfigs =
        Arrays.asList(
            BigQueryWriteConfiguration.builder()
                .setTable("project:dataset.table")
                .setCreateDisposition("INVALID_DISPOSITION"));

    for (BigQueryWriteConfiguration.Builder config : invalidConfigs) {
      assertThrows(
          Exception.class,
          () -> {
            config.build().validate();
          });
    }
  }

  public PCollectionRowTuple runWithConfig(BigQueryWriteConfiguration config) {
    return runWithConfig(config, ROWS);
  }

  public PCollectionRowTuple runWithConfig(BigQueryWriteConfiguration config, List<Row> inputRows) {
    BigQueryStorageWriteApiSchemaTransformProvider provider =
        new BigQueryStorageWriteApiSchemaTransformProvider();

    BigQueryStorageWriteApiSchemaTransform writeTransform =
        (BigQueryStorageWriteApiSchemaTransform) provider.from(config);

    writeTransform.setBigQueryServices(fakeBigQueryServices);
    String tag = provider.inputCollectionNames().get(0);
    PCollection<Row> rows =
        p.apply(Create.of(inputRows).withRowSchema(inputRows.get(0).getSchema()));

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

      if (!rowEquals(expectedRow, actualRow)) {
        return false;
      }
    }
    return true;
  }

  public boolean rowEquals(Row expectedRow, TableRow actualRow) {
    return expectedRow.equals(BigQueryUtils.toBeamRow(expectedRow.getSchema(), actualRow));
  }

  @Test
  public void testSimpleWrite() throws Exception {
    String tableSpec = "project:dataset.simple_write";
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder().setTable(tableSpec).build();

    runWithConfig(config, ROWS);
    p.run().waitUntilFinish();

    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    assertTrue(
        rowsEquals(ROWS, fakeDatasetService.getAllRows("project", "dataset", "simple_write")));
  }

  @Test
  public void testWriteToDynamicDestinations() throws Exception {
    String dynamic = BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS;
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder().setTable(dynamic).build();

    String baseTableSpec = "project:dataset.dynamic_write_";

    Schema schemaWithDestinations =
        Schema.builder().addStringField(DESTINATION).addRowField(RECORD, SCHEMA).build();
    List<Row> rowsWithDestinations =
        ROWS.stream()
            .map(
                row ->
                    Row.withSchema(schemaWithDestinations)
                        .withFieldValue(DESTINATION, baseTableSpec + row.getInt64("number"))
                        .withFieldValue(RECORD, row)
                        .build())
            .collect(Collectors.toList());

    runWithConfig(config, rowsWithDestinations);
    p.run().waitUntilFinish();

    assertTrue(
        rowEquals(
            ROWS.get(0),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_1").get(0)));
    assertTrue(
        rowEquals(
            ROWS.get(1),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_2").get(0)));
    assertTrue(
        rowEquals(
            ROWS.get(2),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_3").get(0)));
  }

  @Test
  public void testWriteToPortableDynamicDestinations() throws Exception {
    String destinationTemplate = "project:dataset.dynamic_write_{name}_{number}";
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(destinationTemplate)
            .setKeep(Arrays.asList("number", "dt"))
            .build();

    runWithConfig(config);
    p.run().waitUntilFinish();

    RowFilter rowFilter = new RowFilter(SCHEMA).keep(Arrays.asList("number", "dt"));
    assertTrue(
        rowEquals(
            rowFilter.filter(ROWS.get(0)),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_a_1").get(0)));
    assertTrue(
        rowEquals(
            rowFilter.filter(ROWS.get(1)),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_b_2").get(0)));
    assertTrue(
        rowEquals(
            rowFilter.filter(ROWS.get(2)),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_c_3").get(0)));
  }

  List<Row> createCDCUpsertRows(List<Row> rows, boolean dynamicDestination, String tablePrefix) {

    Schema.Builder schemaBuilder =
        Schema.builder()
            .addRowField(RECORD, SCHEMA)
            .addRowField(
                BigQueryStorageWriteApiSchemaTransformProvider.ROW_PROPERTY_MUTATION_INFO,
                BigQueryStorageWriteApiSchemaTransformProvider.ROW_SCHEMA_MUTATION_INFO);

    if (dynamicDestination) {
      schemaBuilder = schemaBuilder.addStringField(DESTINATION);
    }

    Schema schemaWithCDC = schemaBuilder.build();
    return IntStream.range(0, rows.size())
        .mapToObj(
            idx -> {
              Row row = rows.get(idx);
              Row.FieldValueBuilder rowBuilder =
                  Row.withSchema(schemaWithCDC)
                      .withFieldValue(
                          BigQueryStorageWriteApiSchemaTransformProvider.ROW_PROPERTY_MUTATION_INFO,
                          Row.withSchema(
                                  BigQueryStorageWriteApiSchemaTransformProvider
                                      .ROW_SCHEMA_MUTATION_INFO)
                              .withFieldValue(
                                  BigQueryStorageWriteApiSchemaTransformProvider
                                      .ROW_PROPERTY_MUTATION_TYPE,
                                  "UPSERT")
                              .withFieldValue(
                                  BigQueryStorageWriteApiSchemaTransformProvider
                                      .ROW_PROPERTY_MUTATION_SQN,
                                  "AAA" + idx)
                              .build())
                      .withFieldValue(RECORD, row);
              if (dynamicDestination) {
                rowBuilder =
                    rowBuilder.withFieldValue(DESTINATION, tablePrefix + row.getInt64("number"));
              }
              return rowBuilder.build();
            })
        .collect(Collectors.toList());
  }

  @Test
  public void testCDCWrites() throws Exception {
    String tableSpec = "project:dataset.cdc_write";
    List<String> primaryKeyColumns = ImmutableList.of("name");

    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setUseAtLeastOnceSemantics(true)
            .setTable(tableSpec)
            .setUseCdcWrites(true)
            .setPrimaryKey(primaryKeyColumns)
            .build();

    List<Row> rowsDuplicated =
        Stream.concat(ROWS.stream(), ROWS.stream()).collect(Collectors.toList());

    runWithConfig(config, createCDCUpsertRows(rowsDuplicated, false, ""));
    p.run().waitUntilFinish();

    assertTrue(
        rowEquals(
            rowsDuplicated.get(3),
            fakeDatasetService.getAllRows("project", "dataset", "cdc_write").get(0)));
    assertTrue(
        rowEquals(
            rowsDuplicated.get(4),
            fakeDatasetService.getAllRows("project", "dataset", "cdc_write").get(1)));
    assertTrue(
        rowEquals(
            rowsDuplicated.get(5),
            fakeDatasetService.getAllRows("project", "dataset", "cdc_write").get(2)));
  }

  @Test
  public void testCDCWriteToDynamicDestinations() throws Exception {
    List<String> primaryKeyColumns = ImmutableList.of("name");
    String dynamic = BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS;
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setUseAtLeastOnceSemantics(true)
            .setTable(dynamic)
            .setUseCdcWrites(true)
            .setPrimaryKey(primaryKeyColumns)
            .build();

    String baseTableSpec = "project:dataset.cdc_dynamic_write_";

    List<Row> rowsDuplicated =
        Stream.concat(ROWS.stream(), ROWS.stream()).collect(Collectors.toList());

    runWithConfig(config, createCDCUpsertRows(rowsDuplicated, true, baseTableSpec));
    p.run().waitUntilFinish();

    assertTrue(
        rowEquals(
            rowsDuplicated.get(3),
            fakeDatasetService.getAllRows("project", "dataset", "cdc_dynamic_write_1").get(0)));
    assertTrue(
        rowEquals(
            rowsDuplicated.get(4),
            fakeDatasetService.getAllRows("project", "dataset", "cdc_dynamic_write_2").get(0)));
    assertTrue(
        rowEquals(
            rowsDuplicated.get(5),
            fakeDatasetService.getAllRows("project", "dataset", "cdc_dynamic_write_3").get(0)));
  }

  @Test
  public void testInputElementCount() throws Exception {
    String tableSpec = "project:dataset.input_count";
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder().setTable(tableSpec).build();

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
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(tableSpec)
            .setErrorHandling(
                BigQueryWriteConfiguration.ErrorHandling.builder().setOutput("FailedRows").build())
            .build();

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
    PCollection<Row> failedRows =
        result
            .get("FailedRows")
            .apply(
                "ExtractFailedRows",
                MapElements.into(TypeDescriptors.rows())
                    .via((rowAndError) -> rowAndError.<Row>getValue("failed_row")))
            .setRowSchema(SCHEMA);

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
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(tableSpec)
            .setErrorHandling(
                BigQueryWriteConfiguration.ErrorHandling.builder().setOutput("FailedRows").build())
            .build();

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

  @Test
  public void testManagedChoosesStorageApiForUnboundedWrites() {
    PCollection<Row> batchInput =
        p.apply(TestStream.create(SCHEMA).addElements(ROWS.get(0)).advanceWatermarkToInfinity());
    batchInput.apply(
        Managed.write(Managed.BIGQUERY)
            .withConfig(ImmutableMap.of("table", "project.dataset.table")));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> writeTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr ->
                    tr.getUniqueName()
                        .contains(BigQueryStorageWriteApiSchemaTransform.class.getSimpleName()))
            .collect(Collectors.toList());
    assertThat(writeTransformProto.size(), greaterThan(0));
    p.enableAbandonedNodeEnforcement(false);
  }
}
