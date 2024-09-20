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
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
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
    return expectedRow.getValue("name").equals(actualRow.get("name"))
        && expectedRow
            .getValue("number")
            .equals(Long.parseLong(actualRow.get("number").toString()));
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
  public void testWriteToDynamicDestinations() throws Exception {
    String dynamic = BigQueryStorageWriteApiSchemaTransformProvider.DYNAMIC_DESTINATIONS;
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder().setTable(dynamic).build();

    String baseTableSpec = "project:dataset.dynamic_write_";

    Schema schemaWithDestinations =
        Schema.builder().addStringField("destination").addRowField("record", SCHEMA).build();
    List<Row> rowsWithDestinations =
        ROWS.stream()
            .map(
                row ->
                    Row.withSchema(schemaWithDestinations)
                        .withFieldValue("destination", baseTableSpec + row.getInt64("number"))
                        .withFieldValue("record", row)
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
  public void testCDCWrites() throws Exception {
    String tableSpec = "project:dataset.cdc_write";

    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setTable(tableSpec)
            .setUseCDCWritesWithTablePrimaryKey(List.of("name"))
            .build();

    Schema cdcInfoSchema =
        Schema.builder()
            .addStringField("mutation_info")
            .addStringField("change_sequence_number")
            .build();
    Schema schemaWithCDC =
        Schema.builder()
            .addRowField("record", SCHEMA)
            .addRowField("cdc_info", cdcInfoSchema)
            .build();
    
    List<Row> rowsDuplicated =
        Stream.concat(ROWS.stream(), ROWS.stream()).collect(Collectors.toList());
    List<Row> rowsWithCDC =
        IntStream.range(0, rowsDuplicated.size())
            .mapToObj(
                idx -> {
                  Row row = rowsDuplicated.get(idx);
                  return Row.withSchema(schemaWithCDC)
                      .withFieldValue(
                          "cdc_info",
                          Row.withSchema(cdcInfoSchema)
                              .withFieldValue("mutation_info", "UPSERT")
                              .withFieldValue("change_sequence_number", "AAA/" + idx))
                      .withFieldValue("record", row)
                      .build();
                })
            .collect(Collectors.toList());

    runWithConfig(config, rowsWithCDC);
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
            fakeDatasetService.getAllRows("project", "dataset", "cdc_write").get(1)));
  }

  @Test
  public void testCDCWriteToDynamicDestinations() throws Exception {
    String dynamic = BigQueryStorageWriteApiSchemaTransformProvider.DYNAMIC_DESTINATIONS;
    BigQueryStorageWriteApiSchemaTransformConfiguration config =
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setTable(dynamic)
            .setUseCDCWritesWithTablePrimaryKey(List.of("name"))
            .build();

    String baseTableSpec = "project:dataset.dynamic_write_";

    Schema cdcInfoSchema =
        Schema.builder()
            .addStringField("mutation_info")
            .addStringField("change_sequence_number")
            .build();
    Schema schemaWithDestinationsAndCDC =
        Schema.builder()
            .addStringField("destination")
            .addRowField("record", SCHEMA)
            .addRowField("cdc_info", cdcInfoSchema)
            .build();
    
    List<Row> rowsDuplicated =
        Stream.concat(ROWS.stream(), ROWS.stream()).collect(Collectors.toList());
    List<Row> rowsWithDestinationsAndCDC =
        IntStream.range(0, rowsDuplicated.size())
            .mapToObj(
                idx -> {
                  Row row = rowsDuplicated.get(idx);
                  return Row.withSchema(schemaWithDestinationsAndCDC)
                      .withFieldValue("destination", baseTableSpec + row.getInt64("number"))
                      .withFieldValue(
                          "cdc_info",
                          Row.withSchema(cdcInfoSchema)
                              .withFieldValue("mutation_info", "UPSERT")
                              .withFieldValue("change_sequence_number", "AAA/" + idx))
                      .withFieldValue("record", row)
                      .build();
                })
            .collect(Collectors.toList());

    runWithConfig(config, rowsWithDestinationsAndCDC);
    p.run().waitUntilFinish();

    assertTrue(
        rowEquals(
            rowsDuplicated.get(3),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_1").get(0)));
    assertTrue(
        rowEquals(
            rowsDuplicated.get(4),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_2").get(0)));
    assertTrue(
        rowEquals(
            rowsDuplicated.get(5),
            fakeDatasetService.getAllRows("project", "dataset", "dynamic_write_3").get(0)));
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
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setTable(tableSpec)
            .setErrorHandling(
                BigQueryStorageWriteApiSchemaTransformConfiguration.ErrorHandling.builder()
                    .setOutput("FailedRows")
                    .build())
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
    ;

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
        BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
            .setTable(tableSpec)
            .setErrorHandling(
                BigQueryStorageWriteApiSchemaTransformConfiguration.ErrorHandling.builder()
                    .setOutput("FailedRows")
                    .build())
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
}
