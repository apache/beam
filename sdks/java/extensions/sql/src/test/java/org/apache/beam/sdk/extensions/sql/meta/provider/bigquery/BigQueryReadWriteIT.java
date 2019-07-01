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

import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
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
  public void testSQLRead() {
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
    assertEquals(state, State.DONE);
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
    return pipeline.apply(
        Create.of(Arrays.asList(rows))
            .withSchema(
                SOURCE_SCHEMA, SerializableFunctions.identity(), SerializableFunctions.identity()));
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
