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
package org.apache.beam.examples.snippets.transforms.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyData;
import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyStruct;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for BigQuery samples.
 *
 * <p>To run locally:
 *
 * <pre>{@code
 * ./gradlew integrationTest -p examples/java/ --info \
 *   --tests org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQuerySamplesIT \
 *   -DintegrationTestPipelineOptions='["--tempLocation=gs://YOUR-BUCKET/temp"]' \
 *   -DintegrationTestRunner=direct
 * }</pre>
 */
@RunWith(JUnit4.class)
public class BigQuerySamplesIT {
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final BigQuery BIGQUERY = BigQueryOptions.getDefaultInstance().getService();
  private static final String DATASET =
      "beam_bigquery_samples_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);

  @Rule public final transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readTablePipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readQueryPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readBQStorageAPIPipeline = TestPipeline.create();
  @Rule public final TestName testName = new TestName();

  @BeforeClass
  public static void beforeAll() throws Exception {
    BIGQUERY.create(DatasetInfo.newBuilder(PROJECT, DATASET).build());
  }

  @AfterClass
  public static void afterAll() {
    BIGQUERY.delete(DatasetId.of(PROJECT, DATASET), DatasetDeleteOption.deleteContents());
  }

  @Test
  public void testTableIO() throws Exception {
    String table = testName.getMethodName();

    // ===--- Test 1: createTableRow + writeToTable ---===\\
    // The rest of the tests depend on this since this is the one that writes
    // the contents into the BigQuery table, which the other tests then read.
    TableSchema schema = BigQuerySchemaCreate.createSchema();
    PCollection<TableRow> rows =
        writePipeline.apply(Create.of(Arrays.asList(BigQueryTableRowCreate.createTableRow())));
    BigQueryWriteToTable.writeToTable(PROJECT, DATASET, table, schema, rows);
    writePipeline.run().waitUntilFinish();

    // Check that the BigQuery table has the data using the BigQuery Client Library.
    String query = String.format("SELECT * FROM `%s.%s.%s`", PROJECT, DATASET, table);
    List<String> queryResults =
        StreamSupport.stream(
                BIGQUERY.query(QueryJobConfiguration.of(query)).iterateAll().spliterator(), false)
            .flatMap(values -> fieldValueListToStrings(values).stream())
            .collect(Collectors.toList());
    assertEquals(expected, queryResults);

    // ===--- Test 2: readFromTable ---=== \\
    readAndCheck(BigQueryReadFromTable.readFromTable(PROJECT, DATASET, table, readTablePipeline));
    readTablePipeline.run().waitUntilFinish();

    // ===--- Test 3: readFromQuery ---=== \\
    readAndCheck(BigQueryReadFromQuery.readFromQuery(PROJECT, DATASET, table, readQueryPipeline));
    readQueryPipeline.run().waitUntilFinish();

    // ===--- Test 4: readFromTableWithBigQueryStorageAPI ---=== \\
    readAndCheck(
        BigQueryReadFromTableWithBigQueryStorageAPI.readFromTableWithBigQueryStorageAPI(
            PROJECT, DATASET, table, readBQStorageAPIPipeline));
    readBQStorageAPIPipeline.run().waitUntilFinish();
  }

  // -- Helper methods -- \\
  private static void readAndCheck(PCollection<MyData> rows) {
    PCollection<String> contents =
        rows.apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via(BigQuerySamplesIT::myDataToStrings));
    PAssert.that(contents).containsInAnyOrder(expected);
  }

  private static List<String> expected =
      Arrays.asList(
          "string: UTF-8 strings are supported! 🌱🌳🌍",
          "int64: 432",
          "float64: 3.14159265",
          "numeric: 1234.56",
          "bool: true",
          "bytes: VVRGLTggYnl0ZSBzdHJpbmcg8J+MsfCfjLPwn4yN",
          "date: 2020-03-19",
          "datetime: 2020-03-19T20:41:25.123",
          "time: 20:41:25.123",
          "timestamp: 2020-03-20T03:41:42.123Z",
          "geography: POINT(30 10)",
          "array: [1, 2, 3, 4]",
          "struct: {string: Text 🌱🌳🌍, int64: 42}");

  private static List<String> myDataToStrings(MyData data) {
    return Arrays.asList(
        String.format("string: %s", data.myString),
        String.format("int64: %d", data.myInt64),
        String.format("float64: %.8f", data.myFloat64),
        String.format("numeric: %.2f", data.myNumeric.doubleValue()),
        String.format("bool: %s", data.myBoolean),
        String.format("bytes: %s", Base64.getEncoder().encodeToString(data.myBytes)),
        String.format("date: %s", data.myDate),
        String.format("datetime: %s", data.myDateTime),
        String.format("time: %s", data.myTime),
        String.format("timestamp: %s", data.myTimestamp),
        String.format("geography: %s", data.myGeography),
        String.format("array: %s", data.myArray),
        String.format(
            "struct: {string: %s, int64: %s}",
            data.myStruct.stringValue, data.myStruct.int64Value));
  }

  private static List<String> fieldValueListToStrings(FieldValueList row) {
    MyData data = new MyData();

    data.myString = row.get("string_field").getStringValue();
    data.myInt64 = row.get("int64_field").getLongValue();
    data.myFloat64 = row.get("float64_field").getDoubleValue();
    data.myNumeric = new BigDecimal(row.get("numeric_field").getDoubleValue());
    data.myBoolean = row.get("bool_field").getBooleanValue();
    data.myBytes = Base64.getDecoder().decode(row.get("bytes_field").getStringValue());

    data.myDate = LocalDate.parse(row.get("date_field").getStringValue()).toString();
    data.myDateTime = LocalDateTime.parse(row.get("datetime_field").getStringValue()).toString();
    data.myTime = LocalTime.parse(row.get("time_field").getStringValue()).toString();
    data.myTimestamp =
        Instant.ofEpochMilli(
                (long) (Double.parseDouble(row.get("timestamp_field").getStringValue()) * 1000.0))
            .toString();

    data.myGeography = row.get("geography_field").getStringValue();

    data.myArray =
        row.get("array_field").getRepeatedValue().stream()
            .map(FieldValue::getLongValue)
            .collect(Collectors.toList());

    FieldValueList structValues = row.get("struct_field").getRecordValue();
    data.myStruct = new MyStruct();
    data.myStruct.stringValue = structValues.get(0).getStringValue();
    data.myStruct.int64Value = structValues.get(1).getLongValue();

    return myDataToStrings(data);
  }
}
