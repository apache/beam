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
package org.apache.beam.sdk.extensions.sql.zetasql;

import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimeStampWithoutTimeZone;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ZetaSQL windowing functions (TUMBLE, HOP, and SESSION). */
@RunWith(JUnit4.class)
public class StreamingSqlTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testZetaSQLBasicSlidingWindowing() {
    String sql =
        "SELECT "
            + "COUNT(*) as field_count, "
            + "HOP_START(\"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\") as window_start, "
            + "HOP_END(\"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\") as window_end "
            + "FROM window_test_table "
            + "GROUP BY HOP(ts, \"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_star")
            .addLogicalTypeField("field1", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("field2", SqlTypes.TIMESTAMP)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:09Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:05Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:06Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:08Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:08Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:10Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:09Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:11Z"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicSessionWindowing() {
    String sql =
        "SELECT "
            + "COUNT(*) as field_count, "
            + "SESSION_START(\"INTERVAL 3 SECOND\") as window_start, "
            + "SESSION_END(\"INTERVAL 3 SECOND\") as window_end "
            + "FROM window_test_table_two "
            + "GROUP BY SESSION(ts, \"INTERVAL 3 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_star")
            .addLogicalTypeField("field1", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("field2", SqlTypes.TIMESTAMP)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:12Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:12Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:06Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:06Z"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLNestedQueryFour() {
    String sql =
        "SELECT t1.Value, TUMBLE_START('INTERVAL 1 SECOND') AS period_start, MIN(t2.Value) as"
            + " min_v FROM KeyValue AS t1 INNER JOIN BigTable AS t2 on t1.Key = t2.RowKey GROUP BY"
            + " t1.Value, TUMBLE(t2.ts, 'INTERVAL 1 SECOND')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addStringField("value")
                        .addLogicalTypeField("min_v", SqlTypes.TIMESTAMP)
                        .addStringField("period_start")
                        .build())
                .addValues(
                    "KeyValue235",
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"),
                    "BigTable235")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQuerySeven() {
    String sql =
        "WITH T1 AS (SELECT * FROM KeyValue) SELECT "
            + "COUNT(*) as field_count, "
            + "TUMBLE_START(\"INTERVAL 1 SECOND\") as window_start, "
            + "TUMBLE_END(\"INTERVAL 1 SECOND\") as window_end "
            + "FROM T1 "
            + "GROUP BY TUMBLE(ts, \"INTERVAL 1 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_start")
            .addLogicalTypeField("field1", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("field2", SqlTypes.TIMESTAMP)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:08Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:06Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTVFTumbleAggregation() {
    String sql =
        "SELECT COUNT(*) as field_count, "
            + "window_start "
            + "FROM TUMBLE((select * from KeyValue), descriptor(ts), 'INTERVAL 1 SECOND') "
            + "GROUP BY window_start";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field_count")
            .addLogicalTypeField("window_start", SqlTypes.TIMESTAMP)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(1L, parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(1L, parseTimeStampWithoutTimeZone("2018-07-01T21:26:06Z"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testHopAsTVFAggregation() {
    String sql =
        "SELECT COUNT(*) as field_count, window_start, window_end "
            + "FROM HOP((select * from window_test_table), "
            + "descriptor(ts), \"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\") "
            + "GROUP BY window_start, window_end;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field_count")
            .addLogicalTypeField("field1", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("field2", SqlTypes.TIMESTAMP)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:09Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:05Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:07Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:06Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:08Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:08Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:10Z"))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:09Z"),
                    parseTimeStampWithoutTimeZone("2018-07-01T21:26:11Z"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void runTumbleWindow() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS getFieldCount,"
            + " window_start, "
            + " window_end "
            + " FROM TUMBLE((select * from streaming_sql_test_table_a), descriptor(f_timestamp), \"INTERVAL 1 HOUR\") "
            + " GROUP BY window_start, window_end, f_long";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addLogicalTypeField("window_start", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("window_end", SqlTypes.TIMESTAMP)
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimeStampWithoutTimeZone("2017-01-01T01:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T02:00:00Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-01T02:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T03:00:00Z"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runTumbleWindowFor31Days() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS getFieldCount,"
            + " window_start, "
            + " window_end "
            + " FROM TUMBLE((select * from streaming_sql_test_table_b), descriptor(f_timestamp), \"INTERVAL 31 DAY\") "
            + " GROUP BY f_long, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addLogicalTypeField("window_start", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("window_end", SqlTypes.TIMESTAMP)
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2016-12-08T00:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-08T00:00:00Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    2000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-08T00:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-02-08T00:00:00Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    3000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2017-02-08T00:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-03-11T00:00:00Z"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runHopWindow() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS `getFieldCount`,"
            + "  `window_start`, "
            + "  `window_end` "
            + " FROM HOP((select * from streaming_sql_test_table_a), descriptor(f_timestamp), "
            + " \"INTERVAL 30 MINUTE\", \"INTERVAL 1 HOUR\")"
            + " GROUP BY f_long, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addLogicalTypeField("window_start", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("window_end", SqlTypes.TIMESTAMP)
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimeStampWithoutTimeZone("2017-01-01T00:30:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T01:30:00Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimeStampWithoutTimeZone("2017-01-01T01:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T02:00:00Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-01T01:30:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T02:30:00Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-01T02:00:00Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T03:00:00Z"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runSessionWindow() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS `getFieldCount`,"
            + " `window_start`, "
            + " `window_end` "
            + " FROM SESSION((select * from streaming_sql_test_table_a), descriptor(f_timestamp), "
            + " descriptor(f_long), \"INTERVAL 5 MINUTE\")"
            + " GROUP BY f_long, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addLogicalTypeField("window_start", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("window_end", SqlTypes.TIMESTAMP)
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimeStampWithoutTimeZone("2017-01-01T01:01:03Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T01:11:03Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-01T02:04:03Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T02:09:03Z"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runSessionWindow2() throws Exception {
    String sql =
        "SELECT f_long, f_string, COUNT(*) AS `getFieldCount`,"
            + " `window_start`, `window_end` "
            + " FROM SESSION((select * from streaming_sql_test_table_a), descriptor(f_timestamp), "
            + " descriptor(f_long, f_string), \"INTERVAL 5 MINUTE\")"
            + " GROUP BY f_long, f_string, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addStringField("f_string")
            .addInt64Field("size")
            .addLogicalTypeField("window_start", SqlTypes.TIMESTAMP)
            .addLogicalTypeField("window_end", SqlTypes.TIMESTAMP)
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    "string_row1",
                    2L,
                    parseTimeStampWithoutTimeZone("2017-01-01T01:01:03Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T01:07:03Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    "string_row3",
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-01T01:06:03Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T01:11:03Z"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    "第四行",
                    1L,
                    parseTimeStampWithoutTimeZone("2017-01-01T02:04:03Z"),
                    parseTimeStampWithoutTimeZone("2017-01-01T02:09:03Z"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  @Ignore("[BEAM-9191] CAST operator does not work fully due to bugs in unparsing")
  public void testZetaSQLStructFieldAccessInTumble() {
    String sql =
        "SELECT TUMBLE_START('INTERVAL 1 MINUTE') FROM table_with_struct_ts_string AS A GROUP BY "
            + "TUMBLE(CAST(A.struct_col.struct_col_str AS TIMESTAMP), 'INTERVAL 1 MINUTE')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addLogicalTypeField("field", SqlTypes.TIMESTAMP).build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValue(parseTimeStampWithoutTimeZone("2019-01-15T13:21:00Z"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
