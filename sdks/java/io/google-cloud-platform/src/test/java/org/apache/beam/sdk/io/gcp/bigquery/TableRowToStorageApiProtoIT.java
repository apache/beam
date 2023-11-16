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
import static org.junit.Assert.assertNull;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/** Unit tests for {@link TableRowToStorageApiProto}. */
public class TableRowToStorageApiProtoIT {

  private static final Logger LOG = LoggerFactory.getLogger(TableRowToStorageApiProtoIT.class);
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("TableRowToStorageApiProtoIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "table_row_to_storage_api_proto_" + System.nanoTime();

  private static final TableSchema BASE_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(new TableFieldSchema().setType("STRING").setName("stringValue"))
                  .add(new TableFieldSchema().setType("BYTES").setName("bytesValue"))
                  .add(new TableFieldSchema().setType("INT64").setName("int64Value"))
                  .add(new TableFieldSchema().setType("INTEGER").setName("intValue"))
                  .add(new TableFieldSchema().setType("FLOAT64").setName("float64Value"))
                  .add(new TableFieldSchema().setType("FLOAT").setName("floatValue"))
                  .add(new TableFieldSchema().setType("BOOL").setName("boolValue"))
                  .add(new TableFieldSchema().setType("BOOLEAN").setName("booleanValue"))
                  .add(new TableFieldSchema().setType("TIMESTAMP").setName("timestampValue"))
                  .add(new TableFieldSchema().setType("TIME").setName("timeValue"))
                  .add(new TableFieldSchema().setType("DATETIME").setName("datetimeValue"))
                  .add(new TableFieldSchema().setType("DATE").setName("dateValue"))
                  .add(new TableFieldSchema().setType("NUMERIC").setName("numericValue"))
                  .add(new TableFieldSchema().setType("BIGNUMERIC").setName("bigNumericValue"))
                  .add(
                      new TableFieldSchema()
                          .setType("BYTES")
                          .setMode("REPEATED")
                          .setName("arrayValue"))
                  .build());

  private static final List<Object> REPEATED_BYTES =
      ImmutableList.of(
          BaseEncoding.base64().encode("hello".getBytes(StandardCharsets.UTF_8)),
          "goodbye".getBytes(StandardCharsets.UTF_8),
          "solong".getBytes(StandardCharsets.UTF_8));

  private static final TableRow BASE_TABLE_ROW =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", "2.8168")
          .set("floatValue", "2.817")
          .set("boolValue", "true")
          .set("booleanValue", "true")
          .set("timestampValue", "1970-01-01T00:00:00.000043Z")
          .set("timeValue", "00:52:07.123456")
          .set("datetimeValue", "2019-08-16T00:52:07.123456")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES);

  private static final TableRow BASE_TABLE_ROW_JODA_TIME =
      new TableRow()
          .set("stringValue", "string")
          .set("bytesValue", "string".getBytes(StandardCharsets.UTF_8))
          .set("int64Value", 42)
          .set("intValue", 43)
          .set("float64Value", 2.8168f)
          .set("floatValue", 2.817f)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", org.joda.time.Instant.parse("1970-01-01T00:00:00.0043Z"))
          .set("timeValue", org.joda.time.LocalTime.parse("00:52:07.123456"))
          .set("datetimeValue", org.joda.time.LocalDateTime.parse("2019-08-16T00:52:07.123456"))
          .set("dateValue", org.joda.time.LocalDate.parse("2019-08-16"))
          .set("numericValue", new BigDecimal("23.4"))
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES);

  private static final TableRow BASE_TABLE_ROW_JAVA_TIME =
      new TableRow()
          .set("stringValue", "string")
          .set("bytesValue", "string".getBytes(StandardCharsets.UTF_8))
          .set("int64Value", 42)
          .set("intValue", 43)
          .set("float64Value", 2.8168f)
          .set("floatValue", 2.817f)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", Instant.parse("1970-01-01T00:00:00.000043Z"))
          .set("timeValue", LocalTime.parse("00:52:07.123456"))
          .set("datetimeValue", LocalDateTime.parse("2019-08-16T00:52:07.123456"))
          .set("dateValue", LocalDate.parse("2019-08-16"))
          .set("numericValue", new BigDecimal("23.4"))
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES);

  private static final TableRow BASE_TABLE_ROW_NUM_TIME =
      new TableRow()
          .set("stringValue", "string")
          .set("bytesValue", "string".getBytes(StandardCharsets.UTF_8))
          .set("int64Value", 42)
          .set("intValue", 43)
          .set("float64Value", 2.8168f)
          .set("floatValue", 2.817f)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", 43)
          .set("timeValue", 3497124416L)
          .set("datetimeValue", 142111881387172416L)
          .set("dateValue", 18124)
          .set("numericValue", new BigDecimal("23.4"))
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES);

  @SuppressWarnings({
    "FloatingPointLiteralPrecision" // https://github.com/apache/beam/issues/23666
  })
  private static final TableRow BASE_TABLE_ROW_FLOATS =
      new TableRow()
          .set("stringValue", "string")
          .set("bytesValue", "string".getBytes(StandardCharsets.UTF_8))
          .set("int64Value", 42)
          .set("intValue", 43)
          .set("float64Value", 2.8168f)
          .set("floatValue", 2.817f)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", 43)
          .set("timeValue", 3497124416L)
          .set("datetimeValue", 142111881387172416D)
          .set("dateValue", 18124)
          .set("numericValue", 23.4)
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES);

  private static final TableRow BASE_TABLE_ROW_NULL =
      new TableRow()
          //          .set("stringValue", null)  // do not set stringValue, this should work
          .set("bytesValue", null)
          .set("int64Value", null)
          .set("intValue", null)
          .set("float64Value", null)
          .set("floatValue", null)
          .set("boolValue", null)
          .set("booleanValue", null)
          .set("timestampValue", null)
          .set("timeValue", null)
          .set("datetimeValue", null)
          .set("dateValue", null)
          .set("numericValue", null)
          .set("arrayValue", null);

  private static final List<Object> REPEATED_BYTES_EXPECTED =
      ImmutableList.of(
          BaseEncoding.base64().encode("hello".getBytes(StandardCharsets.UTF_8)),
          BaseEncoding.base64().encode("goodbye".getBytes(StandardCharsets.UTF_8)),
          BaseEncoding.base64().encode("solong".getBytes(StandardCharsets.UTF_8)));

  private static final TableRow BASE_TABLE_ROW_EXPECTED =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", 2.8168)
          .set("floatValue", 2.817)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", "4.3E-5")
          .set("timeValue", "00:52:07.123456")
          .set("datetimeValue", "2019-08-16T00:52:07.123456")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES_EXPECTED);

  // joda is up to millisecond precision, expect truncation
  private static final TableRow BASE_TABLE_ROW_JODA_EXPECTED =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", 2.8168)
          .set("floatValue", 2.817)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", "0.004")
          .set("timeValue", "00:52:07.123000")
          .set("datetimeValue", "2019-08-16T00:52:07.123000")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES_EXPECTED);

  private static final TableRow BASE_TABLE_ROW_NUM_EXPECTED =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", 2.8168)
          .set("floatValue", 2.817)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", "4.3E-5")
          .set("timeValue", "00:52:07.123456")
          .set("datetimeValue", "2019-08-16T00:52:07.123456")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES_EXPECTED);

  private static final TableRow BASE_TABLE_ROW_FLOATS_EXPECTED =
      new TableRow()
          .set("stringValue", "string")
          .set(
              "bytesValue", BaseEncoding.base64().encode("string".getBytes(StandardCharsets.UTF_8)))
          .set("int64Value", "42")
          .set("intValue", "43")
          .set("float64Value", 2.8168)
          .set("floatValue", 2.817)
          .set("boolValue", true)
          .set("booleanValue", true)
          .set("timestampValue", "4.3E-5")
          .set("timeValue", "00:52:07.123456")
          .set("datetimeValue", "2019-08-16T00:52:07.123456")
          .set("dateValue", "2019-08-16")
          .set("numericValue", "23.4")
          .set("bigNumericValue", "23334.4")
          .set("arrayValue", REPEATED_BYTES_EXPECTED);

  // only nonnull values are returned, null in arrayValue should be converted to empty list
  private static final TableRow BASE_TABLE_ROW_NULL_EXPECTED =
      new TableRow().set("arrayValue", ImmutableList.of());

  private static final TableSchema NESTED_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              ImmutableList.<TableFieldSchema>builder()
                  .add(
                      new TableFieldSchema()
                          .setType("STRUCT")
                          .setName("nestedValue1")
                          .setMode("REQUIRED")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .add(
                      new TableFieldSchema()
                          .setType("RECORD")
                          .setName("nestedValue2")
                          .setMode("REPEATED")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .add(
                      new TableFieldSchema()
                          .setType("RECORD")
                          .setName("nestedValue3")
                          .setMode("NULLABLE")
                          .setFields(BASE_TABLE_SCHEMA.getFields()))
                  .build());

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    LOG.info("Start to clean up tables and datasets.");
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @Test
  public void testBaseTableRow() throws IOException, InterruptedException {
    String tableSpec = createTable(BASE_TABLE_SCHEMA);

    runPipeline(tableSpec, Collections.singleton(BASE_TABLE_ROW));

    List<TableRow> actualTableRows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM %s", tableSpec), PROJECT, true, true);

    assertEquals(1, actualTableRows.size());
    assertEquals(BASE_TABLE_ROW_EXPECTED, actualTableRows.get(0));
  }

  @Test
  public void testNestedRichTypesAndNull() throws IOException, InterruptedException {
    String tableSpec = createTable(NESTED_TABLE_SCHEMA);
    TableRow tableRow =
        new TableRow()
            .set("nestedValue1", BASE_TABLE_ROW)
            .set(
                "nestedValue2",
                Arrays.asList(
                    BASE_TABLE_ROW_JAVA_TIME,
                    BASE_TABLE_ROW_JODA_TIME,
                    BASE_TABLE_ROW_NUM_TIME,
                    BASE_TABLE_ROW_FLOATS,
                    BASE_TABLE_ROW_NULL))
            .set("nestedValue3", null);

    runPipeline(tableSpec, Collections.singleton(tableRow));

    List<TableRow> actualTableRows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM %s", tableSpec), PROJECT, true, true);

    assertEquals(1, actualTableRows.size());
    assertEquals(BASE_TABLE_ROW_EXPECTED, actualTableRows.get(0).get("nestedValue1"));
    assertEquals(
        ImmutableList.of(
            BASE_TABLE_ROW_EXPECTED,
            BASE_TABLE_ROW_JODA_EXPECTED,
            BASE_TABLE_ROW_NUM_EXPECTED,
            BASE_TABLE_ROW_FLOATS_EXPECTED,
            BASE_TABLE_ROW_NULL_EXPECTED),
        actualTableRows.get(0).get("nestedValue2"));
    assertNull(actualTableRows.get(0).get("nestedValue3"));
  }

  private static String createTable(TableSchema tableSchema)
      throws IOException, InterruptedException {
    String table = "table" + System.nanoTime();
    BQ_CLIENT.deleteTable(PROJECT, BIG_QUERY_DATASET_ID, table);
    BQ_CLIENT.createNewTable(
        PROJECT,
        BIG_QUERY_DATASET_ID,
        new Table()
            .setSchema(tableSchema)
            .setTableReference(
                new TableReference()
                    .setTableId(table)
                    .setDatasetId(BIG_QUERY_DATASET_ID)
                    .setProjectId(PROJECT)));
    return PROJECT + "." + BIG_QUERY_DATASET_ID + "." + table;
  }

  private static void runPipeline(String tableSpec, Iterable<TableRow> tableRows) {
    Pipeline p = Pipeline.create();
    p.apply("Create test cases", Create.of(tableRows))
        .apply(
            "Write using Storage Write API",
            BigQueryIO.<TableRow>write()
                .to(tableSpec)
                .withFormatFunction(SerializableFunctions.identity())
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));
    p.run().waitUntilFinish();
  }
}
