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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static junit.framework.TestCase.assertNull;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTable.METHOD_PROPERTY;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTable.WRITE_DISPOSITION_PROPERTY;
import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests form writing to BigQuery with Beam SQL. */
@RunWith(JUnit4.class)
public class BigQueryReadWriteIT implements Serializable {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("id", INT64)
          .addNullableField("name", STRING)
          .addNullableField("arr", FieldType.array(STRING))
          .build();

  private static final Schema SOURCE_SCHEMA_TWO =
      Schema.builder()
          .addNullableField("c_bigint", INT64)
          .addNullableField("c_tinyint", BYTE)
          .addNullableField("c_smallint", INT16)
          .addNullableField("c_integer", INT32)
          .addNullableField("c_float", FLOAT)
          .addNullableField("c_double", DOUBLE)
          .addNullableField("c_boolean", BOOLEAN)
          .addNullableField("c_timestamp", CalciteUtils.TIMESTAMP)
          .addNullableField("c_varchar", STRING)
          .addNullableField("c_char", STRING)
          .addNullableField("c_arr", FieldType.array(STRING))
          .build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(SOURCE_SCHEMA);
  @Rule public transient TestBigQuery bigQueryTestingTypes = TestBigQuery.create(SOURCE_SCHEMA_TWO);

  @Test
  public void testSQLWrite() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    bigQueryTestingTypes
        .assertThatAllRows(SOURCE_SCHEMA_TWO)
        .now(
            containsInAnyOrder(
                row(
                    SOURCE_SCHEMA_TWO,
                    9223372036854775807L,
                    (byte) 127,
                    (short) 32767,
                    2147483647,
                    (float) 1.0,
                    1.0,
                    true,
                    parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                    "varchar",
                    "char",
                    Arrays.asList("123", "456"))));
  }

  @Test
  public void testSQLRead_withExport() throws IOException {
    bigQueryTestingTypes.insertRows(
        SOURCE_SCHEMA_TWO,
        row(
            SOURCE_SCHEMA_TWO,
            9223372036854775807L,
            (byte) 127,
            (short) 32767,
            2147483647,
            (float) 1.0,
            1.0,
            true,
            parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
            "varchar",
            "char",
            Arrays.asList("123", "456")));

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "'"
            + "TBLPROPERTIES "
            + "'{ "
            + METHOD_PROPERTY
            + ": \""
            + Method.EXPORT.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    PAssert.that(output)
        .containsInAnyOrder(
            row(
                SOURCE_SCHEMA_TWO,
                9223372036854775807L,
                (byte) 127,
                (short) 32767,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                Arrays.asList("123", "456")));
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testSQLWriteAndRead() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    PAssert.that(output)
        .containsInAnyOrder(
            row(
                SOURCE_SCHEMA_TWO,
                9223372036854775807L,
                (byte) 127,
                (short) 32767,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                Arrays.asList("123", "456")));
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testSQLWriteAndRead_WithWriteDispositionEmpty() throws IOException {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "'"
            + "TBLPROPERTIES "
            + "'{ "
            + WRITE_DISPOSITION_PROPERTY
            + ": \""
            + WriteDisposition.WRITE_EMPTY.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    bigQueryTestingTypes
        .assertThatAllRows(SOURCE_SCHEMA_TWO)
        .now(
            containsInAnyOrder(
                row(
                    SOURCE_SCHEMA_TWO,
                    9223372036854775807L,
                    (byte) 127,
                    (short) 32767,
                    2147483647,
                    (float) 1.0,
                    1.0,
                    true,
                    parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                    "varchar",
                    "char",
                    Arrays.asList("123", "456"))));
  }

  @Test
  public void testSQLWriteAndRead_WithWriteDispositionTruncate() throws IOException {
    bigQueryTestingTypes.insertRows(
        SOURCE_SCHEMA_TWO,
        row(
            SOURCE_SCHEMA_TWO,
            8223372036854775807L,
            (byte) 256,
            (short) 26892,
            1462973245,
            (float) 2.0,
            2.0,
            true,
            parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
            "varchar",
            "char",
            Arrays.asList("123", "456")));

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "'"
            + "TBLPROPERTIES "
            + "'{ "
            + WRITE_DISPOSITION_PROPERTY
            + ": \""
            + WriteDisposition.WRITE_TRUNCATE.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    bigQueryTestingTypes
        .assertThatAllRows(SOURCE_SCHEMA_TWO)
        .now(
            containsInAnyOrder(
                row(
                    SOURCE_SCHEMA_TWO,
                    9223372036854775807L,
                    (byte) 127,
                    (short) 32767,
                    2147483647,
                    (float) 1.0,
                    1.0,
                    true,
                    parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                    "varchar",
                    "char",
                    Arrays.asList("123", "456"))));
  }

  @Test
  public void testSQLWriteAndRead_WithWriteDispositionAppend() throws IOException {
    bigQueryTestingTypes.insertRows(
        SOURCE_SCHEMA_TWO,
        row(
            SOURCE_SCHEMA_TWO,
            8223372036854775807L,
            (byte) 256,
            (short) 26892,
            1462973245,
            (float) 2.0,
            2.0,
            true,
            parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
            "varchar",
            "char",
            Arrays.asList("123", "456")));

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "' \n"
            + "TBLPROPERTIES "
            + "'{ "
            + WRITE_DISPOSITION_PROPERTY
            + ": \""
            + WriteDisposition.WRITE_APPEND.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    bigQueryTestingTypes
        .assertThatAllRows(SOURCE_SCHEMA_TWO)
        .now(
            containsInAnyOrder(
                row(
                    SOURCE_SCHEMA_TWO,
                    9223372036854775807L,
                    (byte) 127,
                    (short) 32767,
                    2147483647,
                    (float) 1.0,
                    1.0,
                    true,
                    parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                    "varchar",
                    "char",
                    Arrays.asList("123", "456")),
                row(
                    SOURCE_SCHEMA_TWO,
                    8223372036854775807L,
                    (byte) 256,
                    (short) 26892,
                    1462973245,
                    (float) 2.0,
                    2.0,
                    true,
                    parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                    "varchar",
                    "char",
                    Arrays.asList("123", "456"))));
  }

  @Test
  public void testSQLWriteAndRead_withExport() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "' \n"
            + "TBLPROPERTIES "
            + "'{ "
            + METHOD_PROPERTY
            + ": \""
            + Method.EXPORT.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    PAssert.that(output)
        .containsInAnyOrder(
            row(
                SOURCE_SCHEMA_TWO,
                9223372036854775807L,
                (byte) 127,
                (short) 32767,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                Arrays.asList("123", "456")));
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testSQLWriteAndRead_withDirectRead() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "' \n"
            + "TBLPROPERTIES "
            + "'{ "
            + METHOD_PROPERTY
            + ": \""
            + Method.DIRECT_READ.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    String selectTableStatement = "SELECT * FROM TEST";
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(selectTableStatement));

    PAssert.that(output)
        .containsInAnyOrder(
            row(
                SOURCE_SCHEMA_TWO,
                9223372036854775807L,
                (byte) 127,
                (short) 32767,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                Arrays.asList("123", "456")));
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testSQLRead_withDirectRead_withProjectPushDown() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "' \n"
            + "TBLPROPERTIES "
            + "'{ "
            + METHOD_PROPERTY
            + ": \""
            + Method.DIRECT_READ.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    String selectTableStatement = "SELECT c_integer, c_varchar, c_tinyint FROM TEST";
    BeamRelNode relNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    // Calc is not dropped because BigQuery does not support field reordering yet.
    assertThat(relNode, instanceOf(BeamCalcRel.class));
    assertThat(relNode.getInput(0), instanceOf(BeamPushDownIOSourceRel.class));
    // IO projects fields in the same order they are defined in the schema.
    assertThat(
        relNode.getInput(0).getRowType().getFieldNames(),
        containsInAnyOrder("c_tinyint", "c_integer", "c_varchar"));
    // Field reordering is done in a Calc
    assertThat(
        output.getSchema(),
        equalTo(
            Schema.builder()
                .addNullableField("c_integer", INT32)
                .addNullableField("c_varchar", STRING)
                .addNullableField("c_tinyint", BYTE)
                .build()));

    PAssert.that(output)
        .containsInAnyOrder(row(output.getSchema(), 2147483647, "varchar", (byte) 127));
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testSQLRead_withDirectRead_withProjectAndFilterPushDown() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "' \n"
            + "TBLPROPERTIES "
            + "'{ "
            + METHOD_PROPERTY
            + ": \""
            + Method.DIRECT_READ.toString()
            + "\" }'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";

    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    String selectTableStatement = "SELECT c_varchar, c_integer FROM TEST where c_tinyint=127";
    BeamRelNode relNode = sqlEnv.parseQuery(selectTableStatement);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    assertThat(relNode, instanceOf(BeamCalcRel.class));
    // Predicate should be pushed-down to IO level
    assertNull(((BeamCalcRel) relNode).getProgram().getCondition());

    assertThat(relNode.getInput(0), instanceOf(BeamPushDownIOSourceRel.class));
    // Unused fields should not be projected by an IO
    assertThat(
        relNode.getInput(0).getRowType().getFieldNames(),
        containsInAnyOrder("c_varchar", "c_integer"));

    assertThat(
        output.getSchema(),
        equalTo(
            Schema.builder()
                .addNullableField("c_varchar", STRING)
                .addNullableField("c_integer", INT32)
                .build()));

    PAssert.that(output).containsInAnyOrder(row(output.getSchema(), "varchar", 2147483647));
    PipelineResult.State state = readPipeline.run().waitUntilFinish(Duration.standardMinutes(5));
    assertThat(state, equalTo(State.DONE));
  }

  @Test
  public void testSQLTypes() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   c_bigint BIGINT, \n"
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_timestamp TIMESTAMP, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_char CHAR, \n"
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQueryTestingTypes.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "TIMESTAMP '2018-05-28 20:17:40.123', "
            + "'varchar', "
            + "'char', "
            + "ARRAY['123', '456']"
            + ")";
    sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    assertThat(
        bigQueryTestingTypes.getFlatJsonRows(SOURCE_SCHEMA_TWO),
        containsInAnyOrder(
            row(
                SOURCE_SCHEMA_TWO,
                9223372036854775807L,
                (byte) 127,
                (short) 32767,
                2147483647,
                (float) 1.0,
                1.0,
                true,
                parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.123"),
                "varchar",
                "char",
                Arrays.asList("123", "456"))));
  }

  @Test
  public void testInsertSelect() throws Exception {
    BeamSqlEnv sqlEnv =
        BeamSqlEnv.inMemory(
            readOnlyTableProvider(
                pipeline,
                "ORDERS_IN_MEMORY",
                row(SOURCE_SCHEMA, 1L, "foo", Arrays.asList("111", "aaa")),
                row(SOURCE_SCHEMA, 2L, "bar", Arrays.asList("222", "bbb")),
                row(SOURCE_SCHEMA, 3L, "baz", Arrays.asList("333", "ccc"))),
            new BigQueryTableProvider());

    String createTableStatement =
        "CREATE EXTERNAL TABLE ORDERS_BQ( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR, \n "
            + "   arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO ORDERS_BQ \n"
            + " SELECT \n"
            + "    id as `id`, \n"
            + "    name as `name`, \n"
            + "    arr as `arr` \n"
            + " FROM ORDERS_IN_MEMORY";
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    List<Row> allJsonRows = bigQuery.getFlatJsonRows(SOURCE_SCHEMA);

    assertThat(
        allJsonRows,
        containsInAnyOrder(
            row(SOURCE_SCHEMA, 1L, "foo", Arrays.asList("111", "aaa")),
            row(SOURCE_SCHEMA, 2L, "bar", Arrays.asList("222", "bbb")),
            row(SOURCE_SCHEMA, 3L, "baz", Arrays.asList("333", "ccc"))));
  }

  private TableProvider readOnlyTableProvider(Pipeline pipeline, String tableName, Row... rows) {

    return new ReadOnlyTableProvider(
        "PCOLLECTION",
        ImmutableMap.of(tableName, new BeamPCollectionTable(createPCollection(pipeline, rows))));
  }

  private PCollection<Row> createPCollection(Pipeline pipeline, Row... rows) {
    return pipeline.apply(Create.of(Arrays.asList(rows)).withRowSchema(SOURCE_SCHEMA));
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
