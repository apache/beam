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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.security.SecureRandom;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for BigQuery TIMESTAMP with various precisions.
 *
 * <p>Tests write timestamps via Storage Write API and read back using different format (AVRO/ARROW)
 * and precision (PICOS/NANOS/MICROS) combinations.
 *
 * <p>Two tables are used:
 *
 * <ul>
 *   <li>FULL_RANGE: dates from 0001-01-01 to 9999-12-31 (for PICOS and MICROS reads)
 *   <li>NANOS_RANGE: dates within int64 nanos bounds ~1678 to ~2261 (for NANOS reads, which use
 *       Avro/Arrow int64 logical types that overflow outside this range)
 * </ul>
 */
@RunWith(JUnit4.class)
public class BigQueryTimestampPicosIT {

  private static String project;
  private static final String DATASET_ID =
      "bq_timestamp_picos_it_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  private static TestBigQueryOptions bqOptions;
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryTimestampPicosIT");

  private static String fullRangeTableSpec;
  private static String nanosRangeTableSpec;
  private static final String FULL_RANGE_TABLE = "timestamp_full_range";
  private static final String NANOS_RANGE_TABLE = "timestamp_nanos_range";

  //  TEST DATA
  //
  //  Tables have 4 timestamp columns written at different precisions:
  //    ts_picos  - written with 12 fractional digits (picoseconds)
  //    ts_nanos  - written with 9 fractional digits (nanoseconds)
  //    ts_micros - written with 6 fractional digits (microseconds)
  //    ts_millis - written with 3 fractional digits (milliseconds)

  /*
   * FULL_RANGE table - for PICOS and MICROS reads
   * Contains dates outside int64 nanos bounds (0001 and 9999)
   */
  private static final List<TableRow> FULL_RANGE_WRITE =
      ImmutableList.of(
          row(
              "2024-01-15T10:30:45.123456789012Z",
              "2024-01-15 10:30:45.123456789 UTC",
              "2024-01-15 10:30:45.123456 UTC",
              "2024-01-15 10:30:45.123 UTC"),
          row(
              "2024-06-20T15:45:30.987654321098Z",
              "2024-06-20 15:45:30.987654321 UTC",
              "2024-06-20 15:45:30.987654 UTC",
              "2024-06-20 15:45:30.987 UTC"),
          row(
              "0001-01-01T00:00:00.111222333444Z",
              "0001-01-01 00:00:00.111222333 UTC",
              "0001-01-01 00:00:00.111222 UTC",
              "0001-01-01 00:00:00.111 UTC"),
          row(
              "1970-01-01T00:00:00.555666777888Z",
              "1970-01-01 00:00:00.555666777 UTC",
              "1970-01-01 00:00:00.555666 UTC",
              "1970-01-01 00:00:00.555 UTC"),
          row(
              "9999-12-31T23:59:59.999888777666Z",
              "9999-12-31 23:59:59.999888777 UTC",
              "9999-12-31 23:59:59.999888 UTC",
              "9999-12-31 23:59:59.999 UTC"));

  /*
   * NANOS_RANGE table - for NANOS reads
   * Only dates within int64 nanos-since-epoch bounds (~1678 to ~2261)
   */
  private static final List<TableRow> NANOS_RANGE_WRITE =
      ImmutableList.of(
          row(
              "2024-01-15T10:30:45.123456789012Z",
              "2024-01-15 10:30:45.123456789 UTC",
              "2024-01-15 10:30:45.123456 UTC",
              "2024-01-15 10:30:45.123 UTC"),
          row(
              "2024-06-20T15:45:30.987654321098Z",
              "2024-06-20 15:45:30.987654321 UTC",
              "2024-06-20 15:45:30.987654 UTC",
              "2024-06-20 15:45:30.987 UTC"),
          row(
              "1678-09-21T00:12:43.145224192555Z",
              "1678-09-21 00:12:43.145224192 UTC",
              "1678-09-21 00:12:43.145224 UTC",
              "1678-09-21 00:12:43.145 UTC"),
          row(
              "1970-01-01T00:00:00.555666777888Z",
              "1970-01-01 00:00:00.555666777 UTC",
              "1970-01-01 00:00:00.555666 UTC",
              "1970-01-01 00:00:00.555 UTC"),
          row(
              "2261-04-11T23:47:16.854775807333Z",
              "2261-04-11 23:47:16.854775807 UTC",
              "2261-04-11 23:47:16.854775 UTC",
              "2261-04-11 23:47:16.854 UTC"));

  private static final List<TableRow> FULL_RANGE_READ_PICOS =
      ImmutableList.of(
          row(
              "2024-01-15T10:30:45.123456789012Z",
              "2024-01-15T10:30:45.123456789000Z",
              "2024-01-15T10:30:45.123456000000Z",
              "2024-01-15T10:30:45.123000000000Z"),
          row(
              "2024-06-20T15:45:30.987654321098Z",
              "2024-06-20T15:45:30.987654321000Z",
              "2024-06-20T15:45:30.987654000000Z",
              "2024-06-20T15:45:30.987000000000Z"),
          row(
              "0001-01-01T00:00:00.111222333444Z",
              "0001-01-01T00:00:00.111222333000Z",
              "0001-01-01T00:00:00.111222000000Z",
              "0001-01-01T00:00:00.111000000000Z"),
          row(
              "1970-01-01T00:00:00.555666777888Z",
              "1970-01-01T00:00:00.555666777000Z",
              "1970-01-01T00:00:00.555666000000Z",
              "1970-01-01T00:00:00.555000000000Z"),
          row(
              "9999-12-31T23:59:59.999888777666Z",
              "9999-12-31T23:59:59.999888777000Z",
              "9999-12-31T23:59:59.999888000000Z",
              "9999-12-31T23:59:59.999000000000Z"));

  private static final List<TableRow> NANOS_RANGE_READ_NANOS =
      ImmutableList.of(
          row(
              "2024-01-15 10:30:45.123456789 UTC",
              "2024-01-15 10:30:45.123456789 UTC",
              "2024-01-15 10:30:45.123456 UTC",
              "2024-01-15 10:30:45.123 UTC"),
          row(
              "2024-06-20 15:45:30.987654321 UTC",
              "2024-06-20 15:45:30.987654321 UTC",
              "2024-06-20 15:45:30.987654 UTC",
              "2024-06-20 15:45:30.987 UTC"),
          row(
              "1678-09-21 00:12:43.145224192 UTC",
              "1678-09-21 00:12:43.145224192 UTC",
              "1678-09-21 00:12:43.145224 UTC",
              "1678-09-21 00:12:43.145 UTC"),
          row(
              "1970-01-01 00:00:00.555666777 UTC",
              "1970-01-01 00:00:00.555666777 UTC",
              "1970-01-01 00:00:00.555666 UTC",
              "1970-01-01 00:00:00.555 UTC"),
          row(
              "2261-04-11 23:47:16.854775807 UTC",
              "2261-04-11 23:47:16.854775807 UTC",
              "2261-04-11 23:47:16.854775 UTC",
              "2261-04-11 23:47:16.854 UTC"));

  private static final List<TableRow> FULL_RANGE_READ_MICROS =
      ImmutableList.of(
          row(
              "2024-01-15 10:30:45.123456 UTC",
              "2024-01-15 10:30:45.123456 UTC",
              "2024-01-15 10:30:45.123456 UTC",
              "2024-01-15 10:30:45.123 UTC"),
          row(
              "2024-06-20 15:45:30.987654 UTC",
              "2024-06-20 15:45:30.987654 UTC",
              "2024-06-20 15:45:30.987654 UTC",
              "2024-06-20 15:45:30.987 UTC"),
          row(
              "0001-01-01 00:00:00.111222 UTC",
              "0001-01-01 00:00:00.111222 UTC",
              "0001-01-01 00:00:00.111222 UTC",
              "0001-01-01 00:00:00.111 UTC"),
          row(
              "1970-01-01 00:00:00.555666 UTC",
              "1970-01-01 00:00:00.555666 UTC",
              "1970-01-01 00:00:00.555666 UTC",
              "1970-01-01 00:00:00.555 UTC"),
          row(
              "9999-12-31 23:59:59.999888 UTC",
              "9999-12-31 23:59:59.999888 UTC",
              "9999-12-31 23:59:59.999888 UTC",
              "9999-12-31 23:59:59.999 UTC"));

  private static final List<TableRow> FULL_RANGE_READ_MICROS_ARROW =
      ImmutableList.of(
          row(
              "2024-01-15 10:30:45.123 UTC",
              "2024-01-15 10:30:45.123 UTC",
              "2024-01-15 10:30:45.123 UTC",
              "2024-01-15 10:30:45.123 UTC"),
          row(
              "2024-06-20 15:45:30.987 UTC",
              "2024-06-20 15:45:30.987 UTC",
              "2024-06-20 15:45:30.987 UTC",
              "2024-06-20 15:45:30.987 UTC"),
          row(
              "0001-01-01 00:00:00.111 UTC",
              "0001-01-01 00:00:00.111 UTC",
              "0001-01-01 00:00:00.111 UTC",
              "0001-01-01 00:00:00.111 UTC"),
          row(
              "1970-01-01 00:00:00.555 UTC",
              "1970-01-01 00:00:00.555 UTC",
              "1970-01-01 00:00:00.555 UTC",
              "1970-01-01 00:00:00.555 UTC"),
          row(
              "9999-12-31 23:59:59.999 UTC",
              "9999-12-31 23:59:59.999 UTC",
              "9999-12-31 23:59:59.999 UTC",
              "9999-12-31 23:59:59.999 UTC"));

  private static TableRow row(String picos, String nanos, String micros, String millis) {
    return new TableRow()
        .set("ts_picos", picos)
        .set("ts_nanos", nanos)
        .set("ts_micros", micros)
        .set("ts_millis", millis);
  }

  @BeforeClass
  public static void setup() throws Exception {
    bqOptions = TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class);
    project = bqOptions.as(GcpOptions.class).getProject();

    BQ_CLIENT.createNewDataset(project, DATASET_ID, null, "us-central1");

    fullRangeTableSpec = String.format("%s:%s.%s", project, DATASET_ID, FULL_RANGE_TABLE);
    nanosRangeTableSpec = String.format("%s:%s.%s", project, DATASET_ID, NANOS_RANGE_TABLE);

    // Write full range table
    Pipeline writePipeline1 = Pipeline.create(bqOptions);
    writePipeline1
        .apply("CreateFullRange", Create.of(FULL_RANGE_WRITE))
        .apply(
            "WriteFullRange",
            BigQueryIO.writeTableRows()
                .to(fullRangeTableSpec)
                .withSchema(createSchema())
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    writePipeline1.run().waitUntilFinish();

    // Write nanos range table
    Pipeline writePipeline2 = Pipeline.create(bqOptions);
    writePipeline2
        .apply("CreateNanosRange", Create.of(NANOS_RANGE_WRITE))
        .apply(
            "WriteNanosRange",
            BigQueryIO.writeTableRows()
                .to(nanosRangeTableSpec)
                .withSchema(createSchema())
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    writePipeline2.run().waitUntilFinish();
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(project, DATASET_ID);
  }

  private static TableSchema createSchema() {
    return new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setName("ts_picos")
                    .setType("TIMESTAMP")
                    .setTimestampPrecision(12L),
                new TableFieldSchema()
                    .setName("ts_nanos")
                    .setType("TIMESTAMP")
                    .setTimestampPrecision(12L),
                new TableFieldSchema()
                    .setName("ts_micros")
                    .setType("TIMESTAMP")
                    .setTimestampPrecision(12L),
                new TableFieldSchema()
                    .setName("ts_millis")
                    .setType("TIMESTAMP")
                    .setTimestampPrecision(12L)));
  }

  private void runReadTest(
      TimestampPrecision precision,
      DataFormat format,
      String tableSpec,
      String tableName,
      List<TableRow> expectedRows) {

    Pipeline readPipeline = Pipeline.create(bqOptions);

    PCollection<TableRow> fromTable =
        readPipeline.apply(
            "ReadFromTable",
            BigQueryIO.readTableRows()
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                .withFormat(format)
                .withDirectReadPicosTimestampPrecision(precision)
                .from(tableSpec));
    PCollection<TableRow> fromTableWithSchema =
        readPipeline.apply(
            "ReadFromTableWithSchema",
            BigQueryIO.readTableRowsWithSchema()
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                .withFormat(format)
                .withDirectReadPicosTimestampPrecision(precision)
                .from(tableSpec));

    PCollection<TableRow> fromQuery =
        readPipeline.apply(
            "ReadFromQuery",
            BigQueryIO.readTableRows()
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                .fromQuery(String.format("SELECT * FROM %s.%s.%s", project, DATASET_ID, tableName))
                .usingStandardSql()
                .withFormat(format)
                .withDirectReadPicosTimestampPrecision(precision));

    PAssert.that(fromTable).containsInAnyOrder(expectedRows);
    PAssert.that(fromQuery).containsInAnyOrder(expectedRows);
    PAssert.that(fromTableWithSchema).containsInAnyOrder(expectedRows);

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testRead_Picos_Avro() {
    runReadTest(
        TimestampPrecision.PICOS,
        DataFormat.AVRO,
        fullRangeTableSpec,
        FULL_RANGE_TABLE,
        FULL_RANGE_READ_PICOS);
  }

  @Test
  public void testRead_Picos_Arrow() {
    runReadTest(
        TimestampPrecision.PICOS,
        DataFormat.ARROW,
        fullRangeTableSpec,
        FULL_RANGE_TABLE,
        FULL_RANGE_READ_PICOS);
  }

  @Test
  public void testRead_Nanos_Avro() {
    runReadTest(
        TimestampPrecision.NANOS,
        DataFormat.AVRO,
        nanosRangeTableSpec,
        NANOS_RANGE_TABLE,
        NANOS_RANGE_READ_NANOS);
  }

  @Test
  public void testRead_Nanos_Arrow() {
    runReadTest(
        TimestampPrecision.NANOS,
        DataFormat.ARROW,
        nanosRangeTableSpec,
        NANOS_RANGE_TABLE,
        NANOS_RANGE_READ_NANOS);
  }

  @Test
  public void testRead_Micros_Avro() {
    runReadTest(
        TimestampPrecision.MICROS,
        DataFormat.AVRO,
        fullRangeTableSpec,
        FULL_RANGE_TABLE,
        FULL_RANGE_READ_MICROS);
  }

  @Test
  public void testRead_Micros_Arrow() {
    // Known issue: Arrow MICROS truncates to milliseconds
    runReadTest(
        TimestampPrecision.MICROS,
        DataFormat.ARROW,
        fullRangeTableSpec,
        FULL_RANGE_TABLE,
        FULL_RANGE_READ_MICROS_ARROW);
  }
}
