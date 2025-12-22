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
 * Integration tests for BigQuery TIMESTAMP with picosecond precision.
 *
 * <p>Tests write data via Storage Write API and read back using different precision settings. Each
 * test clearly shows: WRITE DATA → READ SETTINGS → EXPECTED OUTPUT.
 */
@RunWith(JUnit4.class)
public class BigQueryTimestampPicosIT {

  private static final long PICOS_PRECISION = 12L;

  private static String project;
  private static final String DATASET_ID =
      "bq_ts_picos_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryTimestampPicosIT");
  private static TestBigQueryOptions bqOptions;
  private static String tableSpec;

  // ============================================================================
  // UNIFIED SCHEMA - Contains all timestamp column types
  // ============================================================================
  //
  // Schema structure:
  //   - ts_simple:       TIMESTAMP(12)
  //   - ts_array:        ARRAY<TIMESTAMP(12)>
  //   - event:           STRUCT<
  //                        name: STRING,
  //                        ts: TIMESTAMP(12)
  //                      >
  //   - events:          ARRAY<STRUCT<
  //                        name: STRING,
  //                        ts: TIMESTAMP(12)
  //                      >>
  //   - ts_map:          ARRAY<STRUCT<
  //                        key: TIMESTAMP(12),
  //                        value: TIMESTAMP(12)
  //                      >>

  private static final TableSchema SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  // Simple timestamp column
                  new TableFieldSchema()
                      .setName("ts_simple")
                      .setType("TIMESTAMP")
                      .setTimestampPrecision(PICOS_PRECISION),
                  // Array of timestamps
                  new TableFieldSchema()
                      .setName("ts_array")
                      .setType("TIMESTAMP")
                      .setTimestampPrecision(PICOS_PRECISION)
                      .setMode("REPEATED"),
                  // Nested struct with timestamp
                  new TableFieldSchema()
                      .setName("event")
                      .setType("STRUCT")
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema().setName("name").setType("STRING"),
                              new TableFieldSchema()
                                  .setName("ts")
                                  .setType("TIMESTAMP")
                                  .setTimestampPrecision(PICOS_PRECISION))),
                  // Repeated struct with timestamp
                  new TableFieldSchema()
                      .setName("events")
                      .setType("STRUCT")
                      .setMode("REPEATED")
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema().setName("name").setType("STRING"),
                              new TableFieldSchema()
                                  .setName("ts")
                                  .setType("TIMESTAMP")
                                  .setTimestampPrecision(PICOS_PRECISION))),
                  // Map-like: repeated struct with timestamp key and value
                  new TableFieldSchema()
                      .setName("ts_map")
                      .setType("STRUCT")
                      .setMode("REPEATED")
                      .setFields(
                          ImmutableList.of(
                              new TableFieldSchema()
                                  .setName("key")
                                  .setType("TIMESTAMP")
                                  .setTimestampPrecision(PICOS_PRECISION),
                              new TableFieldSchema()
                                  .setName("value")
                                  .setType("TIMESTAMP")
                                  .setTimestampPrecision(PICOS_PRECISION)))));

  // ============================================================================
  // TEST DATA - Written once, read with different precision settings
  // ============================================================================

  private static final List<TableRow> WRITE_DATA =
      ImmutableList.of(
          new TableRow()
              .set("ts_simple", "2024-01-15T10:30:45.123456789012Z")
              .set(
                  "ts_array",
                  ImmutableList.of(
                      "2024-01-15T10:30:45.111111111111Z", "2024-06-20T15:45:30.222222222222Z"))
              .set(
                  "event",
                  new TableRow()
                      .set("name", "login")
                      .set("ts", "2024-01-15T10:30:45.333333333333Z"))
              .set(
                  "events",
                  ImmutableList.of(
                      new TableRow()
                          .set("name", "click")
                          .set("ts", "2024-01-15T10:30:45.444444444444Z"),
                      new TableRow()
                          .set("name", "scroll")
                          .set("ts", "2024-01-15T10:30:45.555555555555Z")))
              .set(
                  "ts_map",
                  ImmutableList.of(
                      new TableRow()
                          .set("key", "2024-01-15T10:30:45.666666666666Z")
                          .set("value", "2024-01-15T10:30:45.777777777777Z"))),
          new TableRow()
              .set("ts_simple", "1970-01-01T00:00:00.000000000001Z")
              .set("ts_array", ImmutableList.of("1970-01-01T00:00:00.000000000002Z"))
              .set(
                  "event",
                  new TableRow()
                      .set("name", "epoch")
                      .set("ts", "1970-01-01T00:00:00.000000000003Z"))
              .set(
                  "events",
                  ImmutableList.of(
                      new TableRow()
                          .set("name", "start")
                          .set("ts", "1970-01-01T00:00:00.000000000004Z")))
              .set(
                  "ts_map",
                  ImmutableList.of(
                      new TableRow()
                          .set("key", "1970-01-01T00:00:00.000000000005Z")
                          .set("value", "1970-01-01T00:00:00.000000000006Z"))));

  @BeforeClass
  public static void setup() throws Exception {
    bqOptions = TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class);
    project = bqOptions.as(GcpOptions.class).getProject();
    BQ_CLIENT.createNewDataset(project, DATASET_ID, null, "us-central1");
    tableSpec = String.format("%s:%s.%s", project, DATASET_ID, "timestamp_picos_test");

    // Write test data
    Pipeline writePipeline = Pipeline.create(bqOptions);
    writePipeline
        .apply("CreateData", Create.of(WRITE_DATA))
        .apply(
            "WriteData",
            BigQueryIO.writeTableRows()
                .to(tableSpec)
                .withSchema(SCHEMA)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    writePipeline.run().waitUntilFinish();
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(project, DATASET_ID);
  }

  @Test
  public void testReadWithPicosPrecision_Avro() {
    // WRITE DATA (written in @BeforeClass):
    //   ts_simple: "2024-01-15T10:30:45.123456789012Z"  (12 digits)
    //   ts_array:  ["...111111111111Z", "...222222222222Z"]
    //   event:     {name: "login", ts: "...333333333333Z"}
    //   events:    [{name: "click", ts: "...444444444444Z"}, ...]
    //   ts_map:    [{key: "...666666666666Z", value: "...777777777777Z"}]
    //
    // READ SETTINGS:
    //   precision: PICOS (12 digits)
    //   format:    AVRO
    //
    // EXPECTED: All 12 digits preserved in ISO format

    List<TableRow> expectedOutput =
        ImmutableList.of(
            new TableRow()
                .set("ts_simple", "2024-01-15T10:30:45.123456789012Z")
                .set(
                    "ts_array",
                    ImmutableList.of(
                        "2024-01-15T10:30:45.111111111111Z", "2024-06-20T15:45:30.222222222222Z"))
                .set(
                    "event",
                    new TableRow()
                        .set("name", "login")
                        .set("ts", "2024-01-15T10:30:45.333333333333Z"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow()
                            .set("name", "click")
                            .set("ts", "2024-01-15T10:30:45.444444444444Z"),
                        new TableRow()
                            .set("name", "scroll")
                            .set("ts", "2024-01-15T10:30:45.555555555555Z")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "2024-01-15T10:30:45.666666666666Z")
                            .set("value", "2024-01-15T10:30:45.777777777777Z"))),
            new TableRow()
                .set("ts_simple", "1970-01-01T00:00:00.000000000001Z")
                .set("ts_array", ImmutableList.of("1970-01-01T00:00:00.000000000002Z"))
                .set(
                    "event",
                    new TableRow()
                        .set("name", "epoch")
                        .set("ts", "1970-01-01T00:00:00.000000000003Z"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow()
                            .set("name", "start")
                            .set("ts", "1970-01-01T00:00:00.000000000004Z")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "1970-01-01T00:00:00.000000000005Z")
                            .set("value", "1970-01-01T00:00:00.000000000006Z"))));

    runReadTest(TimestampPrecision.PICOS, DataFormat.AVRO, expectedOutput);
  }

  @Test
  public void testReadWithPicosPrecision_Arrow() {
    // WRITE DATA: Same as above (12-digit picosecond timestamps)
    //
    // READ SETTINGS:
    //   precision: PICOS (12 digits)
    //   format:    ARROW
    //
    // EXPECTED: All 12 digits preserved in ISO format

    List<TableRow> expectedOutput =
        ImmutableList.of(
            new TableRow()
                .set("ts_simple", "2024-01-15T10:30:45.123456789012Z")
                .set(
                    "ts_array",
                    ImmutableList.of(
                        "2024-01-15T10:30:45.111111111111Z", "2024-06-20T15:45:30.222222222222Z"))
                .set(
                    "event",
                    new TableRow()
                        .set("name", "login")
                        .set("ts", "2024-01-15T10:30:45.333333333333Z"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow()
                            .set("name", "click")
                            .set("ts", "2024-01-15T10:30:45.444444444444Z"),
                        new TableRow()
                            .set("name", "scroll")
                            .set("ts", "2024-01-15T10:30:45.555555555555Z")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "2024-01-15T10:30:45.666666666666Z")
                            .set("value", "2024-01-15T10:30:45.777777777777Z"))),
            new TableRow()
                .set("ts_simple", "1970-01-01T00:00:00.000000000001Z")
                .set("ts_array", ImmutableList.of("1970-01-01T00:00:00.000000000002Z"))
                .set(
                    "event",
                    new TableRow()
                        .set("name", "epoch")
                        .set("ts", "1970-01-01T00:00:00.000000000003Z"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow()
                            .set("name", "start")
                            .set("ts", "1970-01-01T00:00:00.000000000004Z")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "1970-01-01T00:00:00.000000000005Z")
                            .set("value", "1970-01-01T00:00:00.000000000006Z"))));

    runReadTest(TimestampPrecision.PICOS, DataFormat.ARROW, expectedOutput);
  }

  @Test
  public void testReadWithNanosPrecision_Avro() {
    // WRITE DATA: 12-digit picosecond timestamps
    //
    // READ SETTINGS:
    //   precision: NANOS (9 digits)
    //   format:    AVRO
    //
    // EXPECTED: Truncated to 9 digits, UTC format
    //   "2024-01-15T10:30:45.123456789012Z" → "2024-01-15 10:30:45.123456789 UTC"

    List<TableRow> expectedOutput =
        ImmutableList.of(
            new TableRow()
                .set("ts_simple", "2024-01-15 10:30:45.123456789 UTC")
                .set(
                    "ts_array",
                    ImmutableList.of(
                        "2024-01-15 10:30:45.111111111 UTC", "2024-06-20 15:45:30.222222222 UTC"))
                .set(
                    "event",
                    new TableRow()
                        .set("name", "login")
                        .set("ts", "2024-01-15 10:30:45.333333333 UTC"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow()
                            .set("name", "click")
                            .set("ts", "2024-01-15 10:30:45.444444444 UTC"),
                        new TableRow()
                            .set("name", "scroll")
                            .set("ts", "2024-01-15 10:30:45.555555555 UTC")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "2024-01-15 10:30:45.666666666 UTC")
                            .set("value", "2024-01-15 10:30:45.777777777 UTC"))),
            new TableRow()
                .set("ts_simple", "1970-01-01 00:00:00 UTC") // .000000000 truncated
                .set("ts_array", ImmutableList.of("1970-01-01 00:00:00 UTC"))
                .set(
                    "event",
                    new TableRow().set("name", "epoch").set("ts", "1970-01-01 00:00:00 UTC"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow().set("name", "start").set("ts", "1970-01-01 00:00:00 UTC")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "1970-01-01 00:00:00 UTC")
                            .set("value", "1970-01-01 00:00:00 UTC"))));

    runReadTest(TimestampPrecision.NANOS, DataFormat.AVRO, expectedOutput);
  }

  @Test
  public void testReadWithMicrosPrecision_Avro() {
    // WRITE DATA: 12-digit picosecond timestamps
    //
    // READ SETTINGS:
    //   precision: MICROS (6 digits)
    //   format:    AVRO
    //
    // EXPECTED: Truncated to 6 digits, UTC format
    //   "2024-01-15T10:30:45.123456789012Z" → "2024-01-15 10:30:45.123456 UTC"

    List<TableRow> expectedOutput =
        ImmutableList.of(
            new TableRow()
                .set("ts_simple", "2024-01-15 10:30:45.123456 UTC")
                .set(
                    "ts_array",
                    ImmutableList.of(
                        "2024-01-15 10:30:45.111111 UTC", "2024-06-20 15:45:30.222222 UTC"))
                .set(
                    "event",
                    new TableRow().set("name", "login").set("ts", "2024-01-15 10:30:45.333333 UTC"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow()
                            .set("name", "click")
                            .set("ts", "2024-01-15 10:30:45.444444 UTC"),
                        new TableRow()
                            .set("name", "scroll")
                            .set("ts", "2024-01-15 10:30:45.555555 UTC")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "2024-01-15 10:30:45.666666 UTC")
                            .set("value", "2024-01-15 10:30:45.777777 UTC"))),
            new TableRow()
                .set("ts_simple", "1970-01-01 00:00:00 UTC")
                .set("ts_array", ImmutableList.of("1970-01-01 00:00:00 UTC"))
                .set(
                    "event",
                    new TableRow().set("name", "epoch").set("ts", "1970-01-01 00:00:00 UTC"))
                .set(
                    "events",
                    ImmutableList.of(
                        new TableRow().set("name", "start").set("ts", "1970-01-01 00:00:00 UTC")))
                .set(
                    "ts_map",
                    ImmutableList.of(
                        new TableRow()
                            .set("key", "1970-01-01 00:00:00 UTC")
                            .set("value", "1970-01-01 00:00:00 UTC"))));

    runReadTest(TimestampPrecision.MICROS, DataFormat.AVRO, expectedOutput);
  }
  private void runReadTest(
      TimestampPrecision precision, DataFormat format, List<TableRow> expectedOutput) {
    Pipeline readPipeline = Pipeline.create(bqOptions);

    PCollection<TableRow> result =
        readPipeline.apply(
            String.format("Read_%s_%s", precision, format),
            BigQueryIO.readTableRows()
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                .withFormat(format)
                .withDirectReadPicosTimestampPrecision(precision)
                .from(tableSpec));

    PAssert.that(result).containsInAnyOrder(expectedOutput);
    readPipeline.run().waitUntilFinish();
  }
}
